#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin/base'
require 'fluent/log'
require 'fluent/plugin_id'
require 'fluent/plugin_helper'
require 'fluent/timezone'
require 'fluent/unique_id'

require 'time'
require 'monitor'

module Fluent
  module Plugin
    class Output < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin
      include UniqueId::Mixin

      helpers :thread, :retry_state

      CHUNK_KEY_PATTERN = /^[-_.@a-zA-Z0-9]+$/
      CHUNK_KEY_PLACEHOLDER_PATTERN = /\$\{[-_.@a-zA-Z0-9]+\}/

      CHUNKING_FIELD_WARN_NUM = 4

      config_param :time_as_integer, :bool, default: false

      # `<buffer>` and `<secondary>` sections are available only when '#format' and '#write' are implemented
      config_section :buffer, param_name: :buffer_config, init: true, required: false, multi: false, final: true do
        config_argument :chunk_keys, :array, value_type: :string, default: []
        config_param :@type, :string, default: 'memory', alias: :type

        config_param :timekey, :time, default: nil # range size to be used: `time.to_i / @timekey`
        config_param :timekey_wait, :time, default: 600
        # These are for #extract_placeholders
        config_param :timekey_use_utc, :bool, default: false # default is localtime
        config_param :timekey_zone, :string, default: Time.now.strftime('%z') # e.g., "-0700" or "Asia/Tokyo"

        desc 'If true, plugin will try to flush buffer just before shutdown.'
        config_param :flush_at_shutdown, :bool, default: nil # change default by buffer_plugin.persistent?

        desc 'How to enqueue chunks to be flushed. "interval" flushes per flush_interval, "immediate" flushes just after event arrival.'
        config_param :flush_mode, :enum, list: [:default, :lazy, :interval, :immediate], default: :default
        config_param :flush_interval, :time, default: 60, desc: 'The interval between buffer chunk flushes.'

        config_param :flush_thread_count, :integer, default: 1, desc: 'The number of threads to flush the buffer.'

        config_param :flush_thread_interval, :float, default: 1.0, desc: 'Seconds to sleep between checks for buffer flushes in flush threads.'
        config_param :flush_thread_burst_interval, :float, default: 1.0, desc: 'Seconds to sleep between flushes when many buffer chunks are queued.'

        config_param :delayed_commit_timeout, :time, default: 60, desc: 'Seconds of timeout for buffer chunks to be committed by plugins later.'

        config_param :overflow_action, :enum, list: [:throw_exception, :block, :drop_oldest_chunk], default: :throw_exception, desc: 'The action when the size of buffer exceeds the limit.'

        config_param :retry_forever, :bool, default: false, desc: 'If true, plugin will ignore retry_timeout and retry_max_times options and retry flushing forever.'
        config_param :retry_timeout, :time, default: 72 * 60 * 60, desc: 'The maximum seconds to retry to flush while failing, until plugin discards buffer chunks.'
        # 72hours == 17 times with exponential backoff (not to change default behavior)
        config_param :retry_max_times, :integer, default: nil, desc: 'The maximum number of times to retry to flush while failing.'

        config_param :retry_secondary_threshold, :float, default: 0.8, desc: 'ratio of retry_timeout to switch to use secondary while failing.'
        # expornential backoff sequence will be initialized at the time of this threshold

        desc 'How to wait next retry to flush buffer.'
        config_param :retry_type, :enum, list: [:exponential_backoff, :periodic], default: :exponential_backoff
        ### Periodic -> fixed :retry_wait
        ### Exponencial backoff: k is number of retry times
        # c: constant factor, @retry_wait
        # b: base factor, @retry_exponential_backoff_base
        # k: times
        # total retry time: c + c * b^1 + (...) + c*b^k = c*b^(k+1) - 1
        config_param :retry_wait, :time, default: 1, desc: 'Seconds to wait before next retry to flush, or constant factor of exponential backoff.'
        config_param :retry_exponential_backoff_base, :float, default: 2, desc: 'The base number of exponencial backoff for retries.'
        config_param :retry_max_interval, :time, default: nil, desc: 'The maximum interval seconds for exponencial backoff between retries while failing.'

        config_param :retry_randomize, :bool, default: true, desc: 'If true, output plugin will retry after randomized interval not to do burst retries.'
      end

      config_section :secondary, param_name: :secondary_config, required: false, multi: false, final: true do
        config_param :@type, :string, default: nil, alias: :type
        config_section :buffer, required: false, multi: false do
          # dummy to detect invalid specification for here
        end
        config_section :secondary, required: false, multi: false do
          # dummy to detect invalid specification for here
        end
      end

      def process(tag, es)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def write(chunk)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def try_write(chunk)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def format(tag, time, record)
        # standard msgpack_event_stream chunk will be used if this method is not implemented in plugin subclass
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def prefer_buffered_processing
        # override this method to return false only when all of these are true:
        #  * plugin has both implementation for buffered and non-buffered methods
        #  * plugin is expected to work as non-buffered plugin if no `<buffer>` sections specified
        true
      end

      def prefer_delayed_commit
        # override this method to decide which is used of `write` or `try_write` if both are implemented
        true
      end

      # Internal states
      FlushThreadState = Struct.new(:thread, :next_time)
      DequeuedChunkInfo = Struct.new(:chunk_id, :time, :timeout) do
        def expired?
          time + timeout < Time.now
        end
      end

      attr_reader :as_secondary, :delayed_commit, :delayed_commit_timeout
      attr_reader :num_errors, :emit_count, :emit_records, :write_count, :rollback_count

      # for tests
      attr_reader :buffer, :retry, :secondary, :chunk_keys, :chunk_key_time, :chunk_key_tag
      attr_accessor :output_enqueue_thread_waiting, :in_tests

      # output_enqueue_thread_waiting: for test of output.rb itself
      # in_tests: for tests of plugins with test drivers

      def initialize
        super
        @counters_monitor = Monitor.new
        @buffering = false
        @delayed_commit = false
        @as_secondary = false
        @in_tests = false
        @primary_instance = nil

        # TODO: well organized counters
        @num_errors = 0
        @emit_count = 0
        @emit_records = 0
        @write_count = 0
        @rollback_count = 0

        # How to process events is decided here at once, but it will be decided in delayed way on #configure & #start
        if implement?(:synchronous)
          if implement?(:buffered) || implement?(:delayed_commit)
            @buffering = nil # do #configure or #start to determine this for full-featured plugins
          else
            @buffering = false
          end
        else
          @buffering = true
        end
        @custom_format = implement?(:custom_format)

        @buffer = nil
        @secondary = nil
        @retry = nil
        @dequeued_chunks = nil
        @output_flush_threads = nil

        @simple_chunking = nil
        @chunk_keys = @chunk_key_time = @chunk_key_tag = nil
        @flush_mode = nil
      end

      def acts_as_secondary(primary)
        @as_secondary = true
        @primary_instance = primary
        (class << self; self; end).module_eval do
          define_method(:extract_placeholders){ |str, metadata| @primary_instance.extract_placeholders(str, metadata) }
          define_method(:commit_write){ |chunk_id| @primary_instance.commit_write(chunk_id, delayed: delayed_commit, secondary: true) }
          define_method(:rollback_write){ |chunk_id| @primary_instance.rollback_write(chunk_id) }
        end
      end

      def configure(conf)
        unless implement?(:synchronous) || implement?(:buffered) || implement?(:delayed_commit)
          raise "BUG: output plugin must implement some methods. see developer documents."
        end

        has_buffer_section = (conf.elements(name: 'buffer').size > 0)

        super

        if has_buffer_section
          unless implement?(:buffered) || implement?(:delayed_commit)
            raise Fluent::ConfigError, "<buffer> section is configured, but plugin '#{self.class}' doesn't support buffering"
          end
          @buffering = true
        else # no buffer sections
          if implement?(:synchronous)
            if !implement?(:buffered) && !implement?(:delayed_commit)
              if @as_secondary
                raise Fluent::ConfigError, "secondary plugin '#{self.class}' must support buffering, but doesn't."
              end
              @buffering = false
            else
              if @as_secondary
                # secondary plugin always works as buffered plugin without buffer instance
                @buffering = true
              else
                # @buffering.nil? shows that enabling buffering or not will be decided in lazy way in #start
                @buffering = nil
              end
            end
          else # buffered or delayed_commit is supported by `unless` of first line in this method
            @buffering = true
          end
        end

        if @as_secondary
          if !@buffering && !@buffering.nil?
            raise Fluent::ConfigError, "secondary plugin '#{self.class}' must support buffering, but doesn't"
          end
        end

        if (@buffering || @buffering.nil?) && !@as_secondary
          # When @buffering.nil?, @buffer_config was initialized with default value for all parameters.
          # If so, this configuration MUST success.
          @chunk_keys = @buffer_config.chunk_keys.dup
          @chunk_key_time = !!@chunk_keys.delete('time')
          @chunk_key_tag = !!@chunk_keys.delete('tag')
          if @chunk_keys.any?{ |key| key !~ CHUNK_KEY_PATTERN }
            raise Fluent::ConfigError, "chunk_keys specification includes invalid char"
          end

          if @chunk_key_time
            raise Fluent::ConfigError, "<buffer ...> argument includes 'time', but timekey is not configured" unless @buffer_config.timekey
            Fluent::Timezone.validate!(@buffer_config.timekey_zone)
            @buffer_config.timekey_zone = '+0000' if @buffer_config.timekey_use_utc
            @output_time_formatter_cache = {}
          end

          if (@chunk_key_tag ? 1 : 0) + @chunk_keys.size >= CHUNKING_FIELD_WARN_NUM
            log.warn "many chunk keys specified, and it may cause too many chunks on your system."
          end

          # no chunk keys or only tags (chunking can be done without iterating event stream)
          @simple_chunking = !@chunk_key_time && @chunk_keys.empty?

          @flush_mode = @buffer_config.flush_mode
          if @flush_mode == :default
            @flush_mode = (@chunk_key_time ? :lazy : :interval)
          end

          buffer_type = @buffer_config[:@type]
          buffer_conf = conf.elements(name: 'buffer').first || Fluent::Config::Element.new('buffer', '', {}, [])
          @buffer = Plugin.new_buffer(buffer_type, parent: self)
          @buffer.configure(buffer_conf)

          @flush_at_shutdown = @buffer_config.flush_at_shutdown
          if @flush_at_shutdown.nil?
            @flush_at_shutdown = if @buffer.persistent?
                                   false
                                 else
                                   true # flush_at_shutdown is true in default for on-memory buffer
                                 end
          elsif !@flush_at_shutdown && !@buffer.persistent?
            buf_type = Plugin.lookup_type_from_class(@buffer.class)
            log.warn "'flush_at_shutdown' is false, and buffer plugin '#{buf_type}' is not persistent buffer."
            log.warn "your configuration will lose buffered data at shutdown. please confirm your configuration again."
          end
        end

        if @secondary_config
          raise Fluent::ConfigError, "Invalid <secondary> section for non-buffered plugin" unless @buffering
          raise Fluent::ConfigError, "<secondary> section cannot have <buffer> section" if @secondary_config.buffer
          raise Fluent::ConfigError, "<secondary> section cannot have <secondary> section" if @secondary_config.secondary
          raise Fluent::ConfigError, "<secondary> section and 'retry_forever' are exclusive" if @buffer_config.retry_forever

          secondary_type = @secondary_config[:@type]
          unless secondary_type
            secondary_type = conf['@type'] # primary plugin type
          end
          secondary_conf = conf.elements(name: 'secondary').first
          @secondary = Plugin.new_output(secondary_type)
          @secondary.acts_as_secondary(self)
          @secondary.configure(secondary_conf)
          @secondary.router = router if @secondary.has_router?
          if self.class != @secondary.class
            log.warn "secondary type should be same with primary one", primary: self.class.to_s, secondary: @secondary.class.to_s
          end
        else
          @secondary = nil
        end

        self
      end

      def start
        super

        if @buffering.nil?
          @buffering = prefer_buffered_processing
          if !@buffering && @buffer
            @buffer.terminate # it's not started, so terminate will be enough
          end
        end

        if @buffering
          m = method(:emit_buffered)
          (class << self; self; end).module_eval do
            define_method(:emit_events, m)
          end

          @custom_format = implement?(:custom_format)
          @delayed_commit = if implement?(:buffered) && implement?(:delayed_commit)
                              prefer_delayed_commit
                            else
                              implement?(:delayed_commit)
                            end
          @delayed_commit_timeout = @buffer_config.delayed_commit_timeout
        else # !@buffering
          m = method(:emit_sync)
          (class << self; self; end).module_eval do
            define_method(:emit_events, m)
          end
        end

        if @buffering && !@as_secondary
          @retry = nil
          @retry_mutex = Mutex.new

          @buffer.start

          @output_flush_threads = []
          @output_flush_threads_mutex = Mutex.new
          @output_flush_threads_running = true

          # mainly for test: detect enqueue works as code below:
          #   @output.interrupt_flushes
          #   # emits
          #   @output.enqueue_thread_wait
          @output_flush_interrupted = false
          @output_enqueue_thread_mutex = Mutex.new
          @output_enqueue_thread_waiting = false

          @dequeued_chunks = []
          @dequeued_chunks_mutex = Mutex.new

          @buffer_config.flush_thread_count.times do |i|
            thread_title = "flush_thread_#{i}".to_sym
            thread_state = FlushThreadState.new(nil, nil)
            thread = thread_create(thread_title) do
              flush_thread_run(thread_state)
            end
            thread_state.thread = thread
            @output_flush_threads_mutex.synchronize do
              @output_flush_threads << thread_state
            end
          end
          @output_flush_thread_current_position = 0

          unless @in_tests
            if @flush_mode == :interval || @chunk_key_time
              thread_create(:enqueue_thread, &method(:enqueue_thread_run))
            end
          end
        end
        @secondary.start if @secondary
      end

      def stop
        @secondary.stop if @secondary
        @buffer.stop if @buffering && @buffer

        super
      end

      def before_shutdown
        @secondary.before_shutdown if @secondary

        if @buffering && @buffer
          if @flush_at_shutdown
            force_flush
          end
          @buffer.before_shutdown
        end

        super
      end

      def shutdown
        @secondary.shutdown if @secondary
        @buffer.shutdown if @buffering && @buffer

        super
      end

      def after_shutdown
        try_rollback_all if @buffering && !@as_secondary # rollback regardless with @delayed_commit, because secondary may do it
        @secondary.after_shutdown if @secondary

        if @buffering && @buffer
          @buffer.after_shutdown

          @output_flush_threads_running = false
          if @output_flush_threads && !@output_flush_threads.empty?
            @output_flush_threads.each do |state|
              state.thread.run if state.thread.alive? # to wakeup thread and make it to stop by itself
            end
            @output_flush_threads.each do |state|
              state.thread.join
            end
          end
        end

        super
      end

      def close
        @buffer.close if @buffering && @buffer
        @secondary.close if @secondary

        super
      end

      def terminate
        @buffer.terminate if @buffering && @buffer
        @secondary.terminate if @secondary

        super
      end

      def support_in_v12_style?(feature)
        # for plugins written in v0.12 styles
        case feature
        when :synchronous    then false
        when :buffered       then false
        when :delayed_commit then false
        when :custom_format  then false
        else
          raise ArgumentError, "unknown feature: #{feature}"
        end
      end

      def implement?(feature)
        methods_of_plugin = self.class.instance_methods(false)
        case feature
        when :synchronous    then methods_of_plugin.include?(:process) || support_in_v12_style?(:synchronous)
        when :buffered       then methods_of_plugin.include?(:write) || support_in_v12_style?(:buffered)
        when :delayed_commit then methods_of_plugin.include?(:try_write)
        when :custom_format  then methods_of_plugin.include?(:format) || support_in_v12_style?(:custom_format)
        else
          raise ArgumentError, "Unknown feature for output plugin: #{feature}"
        end
      end

      # TODO: optimize this code
      def extract_placeholders(str, metadata)
        if metadata.timekey.nil? && metadata.tag.nil? && metadata.variables.nil?
          str
        else
          rvalue = str
          # strftime formatting
          if @chunk_key_time # this section MUST be earlier than rest to use raw 'str'
            @output_time_formatter_cache[str] ||= Fluent::Timezone.formatter(@buffer_config.timekey_zone, str)
            rvalue = @output_time_formatter_cache[str].call(metadata.timekey)
          end
          # ${tag}, ${tag[0]}, ${tag[1]}, ...
          if @chunk_key_tag
            if str =~ /\$\{tag\[\d+\]\}/
              hash = {'${tag}' => metadata.tag}
              metadata.tag.split('.').each_with_index do |part, i|
                hash["${tag[#{i}]}"] = part
              end
              rvalue = rvalue.gsub(/\$\{tag(\[\d+\])?\}/, hash)
            elsif str.include?('${tag}')
              rvalue = rvalue.gsub('${tag}', metadata.tag)
            end
          end
          # ${a_chunk_key}, ...
          if !@chunk_keys.empty? && metadata.variables
            hash = {'${tag}' => '${tag}'} # not to erase this wrongly
            @chunk_keys.each do |key|
              hash["${#{key}}"] = metadata.variables[key.to_sym]
            end
            rvalue = rvalue.gsub(CHUNK_KEY_PLACEHOLDER_PATTERN, hash)
          end
          rvalue
        end
      end

      def emit_events(tag, es)
        # actually this method will be overwritten by #configure
        if @buffering
          emit_buffered(tag, es)
        else
          emit_sync(tag, es)
        end
      end

      def emit_sync(tag, es)
        @counters_monitor.synchronize{ @emit_count += 1 }
        begin
          process(tag, es)
          @counters_monitor.synchronize{ @emit_records += es.size }
        rescue
          @counters_monitor.synchronize{ @num_errors += 1 }
          raise
        end
      end

      def emit_buffered(tag, es)
        @counters_monitor.synchronize{ @emit_count += 1 }
        begin
          execute_chunking(tag, es, enqueue: (@flush_mode == :immediate))
          if !@retry && @buffer.queued?
            submit_flush_once
          end
        rescue
          # TODO: separate number of errors into emit errors and write/flush errors
          @counters_monitor.synchronize{ @num_errors += 1 }
          raise
        end
      end

      # TODO: optimize this code
      def metadata(tag, time, record)
        # this arguments are ordered in output plugin's rule
        # Metadata 's argument order is different from this one (timekey, tag, variables)

        raise ArgumentError, "tag must be a String: #{tag.class}" unless tag.nil? || tag.is_a?(String)
        raise ArgumentError, "time must be a Fluent::EventTime (or Integer): #{time.class}" unless time.nil? || time.is_a?(Fluent::EventTime) || time.is_a?(Integer)
        raise ArgumentError, "record must be a Hash: #{record.class}" unless record.nil? || record.is_a?(Hash)

        if @chunk_keys.nil? && @chunk_key_time.nil? && @chunk_key_tag.nil?
          # for tests
          return Struct.new(:timekey, :tag, :variables).new
        end

        # timekey is int from epoch, and `timekey - timekey % 60` is assumed to mach with 0s of each minutes.
        # it's wrong if timezone is configured as one which supports leap second, but it's very rare and
        # we can ignore it (especially in production systems).
        if @chunk_keys.empty?
          if !@chunk_key_time && !@chunk_key_tag
            @buffer.metadata()
          elsif @chunk_key_time && @chunk_key_tag
            time_int = time.to_i
            timekey = (time_int - (time_int % @buffer_config.timekey)).to_i
            @buffer.metadata(timekey: timekey, tag: tag)
          elsif @chunk_key_time
            time_int = time.to_i
            timekey = (time_int - (time_int % @buffer_config.timekey)).to_i
            @buffer.metadata(timekey: timekey)
          else
            @buffer.metadata(tag: tag)
          end
        else
          timekey = if @chunk_key_time
                      time_int = time.to_i
                      (time_int - (time_int % @buffer_config.timekey)).to_i
                    else
                      nil
                    end
          pairs = Hash[@chunk_keys.map{|k| [k.to_sym, record[k]]}]
          @buffer.metadata(timekey: timekey, tag: (@chunk_key_tag ? tag : nil), variables: pairs)
        end
      end

      def execute_chunking(tag, es, enqueue: false)
        if @simple_chunking
          handle_stream_simple(tag, es, enqueue: enqueue)
        elsif @custom_format
          handle_stream_with_custom_format(tag, es, enqueue: enqueue)
        else
          handle_stream_with_standard_format(tag, es, enqueue: enqueue)
        end
      end

      def write_guard(&block)
        begin
          block.call
        rescue Fluent::Plugin::Buffer::BufferOverflowError
          log.warn "failed to write data into buffer by buffer overflow"
          case @buffer_config.overflow_action
          when :throw_exception
            raise
          when :block
            log.debug "buffer.write is now blocking"
            until @buffer.storable?
              sleep 1
            end
            log.debug "retrying buffer.write after blocked operation"
            retry
          when :drop_oldest_chunk
            begin
              oldest = @buffer.dequeue_chunk
              if oldest
                log.warn "dropping oldest chunk to make space after buffer overflow", chunk_id: oldest.unique_id
                @buffer.purge_chunk(oldest.unique_id)
              else
                log.error "no queued chunks to be dropped for drop_oldest_chunk"
              end
            rescue
              # ignore any errors
            end
            raise unless @buffer.storable?
            retry
          else
            raise "BUG: unknown overflow_action '#{@buffer_config.overflow_action}'"
          end
        end
      end

      def handle_stream_with_custom_format(tag, es, enqueue: false)
        meta_and_data = {}
        records = 0
        es.each do |time, record|
          meta = metadata(tag, time, record)
          meta_and_data[meta] ||= []
          meta_and_data[meta] << format(tag, time, record)
          records += 1
        end
        write_guard do
          @buffer.write(meta_and_data, bulk: false, enqueue: enqueue)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        true
      end

      def handle_stream_with_standard_format(tag, es, enqueue: false)
        meta_and_data = {}
        records = 0
        es.each do |time, record|
          meta = metadata(tag, time, record)
          meta_and_data[meta] ||= MultiEventStream.new
          meta_and_data[meta].add(time, record)
          records += 1
        end
        meta_and_data_bulk = {}
        meta_and_data.each_pair do |meta, m_es|
          meta_and_data_bulk[meta] = [m_es.to_msgpack_stream(time_int: @time_as_integer), m_es.size]
        end
        write_guard do
          @buffer.write(meta_and_data_bulk, bulk: true, enqueue: enqueue)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        true
      end

      def handle_stream_simple(tag, es, enqueue: false)
        meta = metadata((@chunk_key_tag ? tag : nil), nil, nil)
        records = es.size
        if @custom_format
          records = 0
          es_size = 0
          es_bulk = ''
          es.each do |time,record|
            es_bulk << format(tag, time, record)
            es_size += 1
            records += 1
          end
        else
          es_size = es.size
          es_bulk = es.to_msgpack_stream(time_int: @time_as_integer)
        end
        write_guard do
          @buffer.write({meta => [es_bulk, es_size]}, bulk: true, enqueue: enqueue)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        true
      end

      def commit_write(chunk_id, delayed: @delayed_commit, secondary: false)
        if delayed
          @dequeued_chunks_mutex.synchronize do
            @dequeued_chunks.delete_if{ |info| info.chunk_id == chunk_id }
          end
        end
        @buffer.purge_chunk(chunk_id)

        @retry_mutex.synchronize do
          if @retry # success to flush chunks in retries
            if secondary
              log.warn "retry succeeded by secondary.", plugin_id: plugin_id, chunk_id: dump_unique_id_hex(chunk_id)
            else
              log.warn "retry succeeded.", plugin_id: plugin_id, chunk_id: dump_unique_id_hex(chunk_id)
            end
            @retry = nil
          end
        end
      end

      def rollback_write(chunk_id)
        # This API is to rollback chunks explicitly from plugins.
        # 3rd party plugins can depend it on automatic rollback of #try_rollback_write
        @dequeued_chunks_mutex.synchronize do
          @dequeued_chunks.delete_if{ |info| info.chunk_id == chunk_id }
        end
        # returns true if chunk was rollbacked as expected
        #         false if chunk was already flushed and couldn't be rollbacked unexpectedly
        # in many cases, false can be just ignored
        if @buffer.takeback_chunk(chunk_id)
          @counters_monitor.synchronize{ @rollback_count += 1 }
          true
        else
          false
        end
      end

      def try_rollback_write
        @dequeued_chunks_mutex.synchronize do
          while @dequeued_chunks.first && @dequeued_chunks.first.expired?
            info = @dequeued_chunks.shift
            if @buffer.takeback_chunk(info.chunk_id)
              @counters_monitor.synchronize{ @rollback_count += 1 }
              log.warn "failed to flush the buffer chunk, timeout to commit.", plugin_id: plugin_id, chunk_id: dump_unique_id_hex(info.chunk_id), flushed_at: info.time
            end
          end
        end
      end

      def try_rollback_all
        return unless @dequeued_chunks
        @dequeued_chunks_mutex.synchronize do
          until @dequeued_chunks.empty?
            info = @dequeued_chunks.shift
            if @buffer.takeback_chunk(info.chunk_id)
              @counters_monitor.synchronize{ @rollback_count += 1 }
              log.info "delayed commit for buffer chunks was cancelled in shutdown", plugin_id: plugin_id, chunk_id: dump_unique_id_hex(info.chunk_id)
            end
          end
        end
      end

      def next_flush_time
        if @buffer.queued?
          @retry_mutex.synchronize do
            @retry ? @retry.next_time : Time.now + @buffer_config.flush_thread_burst_interval
          end
        else
          Time.now + @buffer_config.flush_thread_interval
        end
      end

      def try_flush
        chunk = @buffer.dequeue_chunk
        return unless chunk

        output = self
        using_secondary = false
        if @retry_mutex.synchronize{ @retry && @retry.secondary? }
          output = @secondary
          using_secondary = true
        end

        unless @custom_format
          chunk.extend ChunkMessagePackEventStreamer
        end

        begin
          if output.delayed_commit
            @counters_monitor.synchronize{ @write_count += 1 }
            output.try_write(chunk)
            @dequeued_chunks_mutex.synchronize do
              # delayed_commit_timeout for secondary is configured in <buffer> of primary (<secondary> don't get <buffer>)
              @dequeued_chunks << DequeuedChunkInfo.new(chunk.unique_id, Time.now, self.delayed_commit_timeout)
            end
          else # output plugin without delayed purge
            chunk_id = chunk.unique_id
            @counters_monitor.synchronize{ @write_count += 1 }
            output.write(chunk)
            commit_write(chunk_id, secondary: using_secondary)
          end
        rescue => e
          log.debug "taking back chunk for errors.", plugin_id: plugin_id, chunk: dump_unique_id_hex(chunk.unique_id)
          @buffer.takeback_chunk(chunk.unique_id)

          @retry_mutex.synchronize do
            if @retry
              @counters_monitor.synchronize{ @num_errors += 1 }
              if @retry.limit?
                records = @buffer.queued_records
                log.error "failed to flush the buffer, and hit limit for retries. dropping all chunks in the buffer queue.", plugin_id: plugin_id, retry_times: @retry.steps, records: records, error: e
                log.error_backtrace e.backtrace
                @buffer.clear_queue!
                log.debug "buffer queue cleared", plugin_id: plugin_id
                @retry = nil
              else
                @retry.step
                msg = if using_secondary
                        "failed to flush the buffer with secondary output."
                      else
                        "failed to flush the buffer."
                      end
                log.warn msg, plugin_id: plugin_id, retry_time: @retry.steps, next_retry: @retry.next_time, chunk: dump_unique_id_hex(chunk.unique_id), error: e
                log.warn_backtrace e.backtrace
              end
            else
              @retry = retry_state(@buffer_config.retry_randomize)
              @counters_monitor.synchronize{ @num_errors += 1 }
              log.warn "failed to flush the buffer.", plugin_id: plugin_id, retry_time: @retry.steps, next_retry: @retry.next_time, chunk: dump_unique_id_hex(chunk.unique_id), error: e
              log.warn_backtrace e.backtrace
            end
          end
        end
      end

      def retry_state(randomize)
        if @secondary
          retry_state_create(
            :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
            forever: @buffer_config.retry_forever, max_steps: @buffer_config.retry_max_times, backoff_base: @buffer_config.retry_exponential_backoff_base,
            max_interval: @buffer_config.retry_max_interval,
            secondary: true, secondary_threshold: @buffer_config.retry_secondary_threshold,
            randomize: randomize
          )
        else
          retry_state_create(
            :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
            forever: @buffer_config.retry_forever, max_steps: @buffer_config.retry_max_times, backoff_base: @buffer_config.retry_exponential_backoff_base,
            max_interval: @buffer_config.retry_max_interval,
            randomize: randomize
          )
        end
      end

      def submit_flush_once
        # Without locks: it is rough but enough to select "next" writer selection
        @output_flush_thread_current_position = (@output_flush_thread_current_position + 1) % @buffer_config.flush_thread_count
        state = @output_flush_threads[@output_flush_thread_current_position]
        state.next_time = 0
        state.thread.run
      end

      def force_flush
        if @buffering
          @buffer.enqueue_all
          submit_flush_all
        end
      end

      def submit_flush_all
        while !@retry && @buffer.queued?
          submit_flush_once
          sleep @buffer_config.flush_thread_burst_interval
        end
      end

      # only for tests of output plugin
      def interrupt_flushes
        @output_flush_interrupted = true
      end

      # only for tests of output plugin
      def enqueue_thread_wait
        @output_enqueue_thread_mutex.synchronize do
          @output_flush_interrupted = false
          @output_enqueue_thread_waiting = true
        end
        require 'timeout'
        Timeout.timeout(10) do
          Thread.pass while @output_enqueue_thread_waiting
        end
      end

      # only for tests of output plugin
      def flush_thread_wakeup
        @output_flush_threads.each do |state|
          state.next_time = 0
          state.thread.run
        end
      end

      def enqueue_thread_run
        value_for_interval = nil
        if @flush_mode == :interval
          value_for_interval = @buffer_config.flush_interval
        end
        if @chunk_key_time
          if !value_for_interval || @buffer_config.timekey < value_for_interval
            value_for_interval = @buffer_config.timekey
          end
        end
        unless value_for_interval
          raise "BUG: both of flush_interval and timekey are disabled"
        end
        interval = value_for_interval / 11.0
        if interval < @buffer_config.flush_thread_interval
          interval = @buffer_config.flush_thread_interval
        end

        begin
          while @output_flush_threads_running
            now_int = Time.now.to_i
            if @output_flush_interrupted
              sleep interval
              next
            end

            @output_enqueue_thread_mutex.lock
            begin
              if @flush_mode == :interval
                flush_interval = @buffer_config.flush_interval.to_i
                # This block should be done by integer values.
                # If both of flush_interval & flush_thread_interval are 1s, expected actual flush timing is 1.5s.
                # If we use integered values for this comparison, expected actual flush timing is 1.0s.
                @buffer.enqueue_all{ |metadata, chunk| chunk.created_at.to_i + flush_interval <= now_int }
              end

              if @chunk_key_time
                timekey_unit = @buffer_config.timekey
                timekey_wait = @buffer_config.timekey_wait
                current_timekey = now_int - now_int % timekey_unit
                @buffer.enqueue_all{ |metadata, chunk| metadata.timekey < current_timekey && metadata.timekey + timekey_unit + timekey_wait <= now_int }
              end
            rescue => e
              log.error "unexpected error while checking flushed chunks. ignored.", plugin_id: plugin_id, error_class: e.class, error: e
              log.error_backtrace
            end
            @output_enqueue_thread_waiting = false
            @output_enqueue_thread_mutex.unlock
            sleep interval
          end
        rescue => e
          # normal errors are rescued by inner begin-rescue clause.
          log.error "error on enqueue thread", plugin_id: plugin_id, error_class: e.class, error: e
          log.error_backtrace
          raise
        end
      end

      def flush_thread_run(state)
        flush_thread_interval = @buffer_config.flush_thread_interval

        # If the given clock_id is not supported, Errno::EINVAL is raised.
        clock_id = Process::CLOCK_MONOTONIC rescue Process::CLOCK_MONOTONIC_RAW
        state.next_time = Process.clock_gettime(clock_id) + flush_thread_interval

        begin
          # This thread don't use `thread_current_running?` because this thread should run in `before_shutdown` phase
          while @output_flush_threads_running
            time = Process.clock_gettime(clock_id)
            interval = state.next_time - time

            if state.next_time <= time
              try_flush
              # next_flush_interval uses flush_thread_interval or flush_thread_burst_interval (or retrying)
              interval = next_flush_time.to_f - Time.now.to_f
              # TODO: if secondary && delayed-commit, next_flush_time will be much longer than expected (because @retry still exists)
              #   @retry should be cleard if delayed commit is enabled? Or any other solution?
              state.next_time = Process.clock_gettime(clock_id) + interval
            end

            if @dequeued_chunks_mutex.synchronize{ !@dequeued_chunks.empty? && @dequeued_chunks.first.expired? }
              unless @output_flush_interrupted
                try_rollback_write
              end
            end

            sleep interval if interval > 0
          end
        rescue => e
          # normal errors are rescued by output plugins in #try_flush
          # so this rescue section is for critical & unrecoverable errors
          log.error "error on output thread", plugin_id: plugin_id, error_class: e.class, error: e
          log.error_backtrace
          raise
        end
      end
    end
  end
end
