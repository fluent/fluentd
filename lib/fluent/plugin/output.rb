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

require 'fluent/error'
require 'fluent/plugin/base'
require 'fluent/plugin_helper/record_accessor'
require 'fluent/log'
require 'fluent/plugin_id'
require 'fluent/plugin_helper'
require 'fluent/timezone'
require 'fluent/unique_id'
require 'fluent/clock'

require 'time'
require 'monitor'

module Fluent
  module Plugin
    class Output < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin
      include UniqueId::Mixin

      helpers_internal :thread, :retry_state

      CHUNK_KEY_PATTERN = /^[-_.@a-zA-Z0-9]+$/
      CHUNK_KEY_PLACEHOLDER_PATTERN = /\$\{[-_.@$a-zA-Z0-9]+\}/
      CHUNK_TAG_PLACEHOLDER_PATTERN = /\$\{(tag(?:\[-?\d+\])?)\}/
      CHUNK_ID_PLACEHOLDER_PATTERN = /\$\{chunk_id\}/

      CHUNKING_FIELD_WARN_NUM = 4

      config_param :time_as_integer, :bool, default: false
      desc 'The threshold to show slow flush logs'
      config_param :slow_flush_log_threshold, :float, default: 20.0

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
        # exponential backoff sequence will be initialized at the time of this threshold

        desc 'How to wait next retry to flush buffer.'
        config_param :retry_type, :enum, list: [:exponential_backoff, :periodic], default: :exponential_backoff
        ### Periodic -> fixed :retry_wait
        ### Exponential backoff: k is number of retry times
        # c: constant factor, @retry_wait
        # b: base factor, @retry_exponential_backoff_base
        # k: times
        # total retry time: c + c * b^1 + (...) + c*b^k = c*b^(k+1) - 1
        config_param :retry_wait, :time, default: 1, desc: 'Seconds to wait before next retry to flush, or constant factor of exponential backoff.'
        config_param :retry_exponential_backoff_base, :float, default: 2, desc: 'The base number of exponential backoff for retries.'
        config_param :retry_max_interval, :time, default: nil, desc: 'The maximum interval seconds for exponential backoff between retries while failing.'

        config_param :retry_randomize, :bool, default: true, desc: 'If true, output plugin will retry after randomized interval not to do burst retries.'
        config_param :disable_chunk_backup, :bool, default: false, desc: 'If true, chunks are thrown away when unrecoverable error happens'
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

      def formatted_to_msgpack_binary?
        # To indicate custom format method (#format) returns msgpack binary or not.
        # If #format returns msgpack binary, override this method to return true.
        false
      end

      # Compatibility for existing plugins
      def formatted_to_msgpack_binary
        formatted_to_msgpack_binary?
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

      def multi_workers_ready?
        false
      end

      # Internal states
      FlushThreadState = Struct.new(:thread, :next_clock, :mutex, :cond_var)
      DequeuedChunkInfo = Struct.new(:chunk_id, :time, :timeout) do
        def expired?
          time + timeout < Time.now
        end
      end

      attr_reader :as_secondary, :delayed_commit, :delayed_commit_timeout, :timekey_zone
      attr_reader :num_errors, :emit_count, :emit_records, :write_count, :rollback_count

      # for tests
      attr_reader :buffer, :retry, :secondary, :chunk_keys, :chunk_key_accessors, :chunk_key_time, :chunk_key_tag
      attr_accessor :output_enqueue_thread_waiting, :dequeued_chunks, :dequeued_chunks_mutex
      # output_enqueue_thread_waiting: for test of output.rb itself
      attr_accessor :retry_for_error_chunk # if true, error flush will be retried even if under_plugin_development is true

      def initialize
        super
        @counters_monitor = Monitor.new
        @buffering = false
        @delayed_commit = false
        @as_secondary = false
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
        @enable_msgpack_streamer = false # decided later

        @buffer = nil
        @secondary = nil
        @retry = nil
        @dequeued_chunks = nil
        @dequeued_chunks_mutex = nil
        @output_enqueue_thread = nil
        @output_flush_threads = nil

        @simple_chunking = nil
        @chunk_keys = @chunk_key_accessors = @chunk_key_time = @chunk_key_tag = nil
        @flush_mode = nil
        @timekey_zone = nil

        @retry_for_error_chunk = false
      end

      def acts_as_secondary(primary)
        @as_secondary = true
        @primary_instance = primary
        @chunk_keys = @primary_instance.chunk_keys || []
        @chunk_key_tag = @primary_instance.chunk_key_tag || false
        if @primary_instance.chunk_key_time
          @chunk_key_time = @primary_instance.chunk_key_time
          @timekey_zone = @primary_instance.timekey_zone
          @output_time_formatter_cache = {}
        end
        self.context_router = primary.context_router

        singleton_class.module_eval do
          define_method(:commit_write){ |chunk_id| @primary_instance.commit_write(chunk_id, delayed: delayed_commit, secondary: true) }
          define_method(:rollback_write){ |chunk_id, update_retry: true| @primary_instance.rollback_write(chunk_id, update_retry) }
        end
      end

      def configure(conf)
        unless implement?(:synchronous) || implement?(:buffered) || implement?(:delayed_commit)
          raise "BUG: output plugin must implement some methods. see developer documents."
        end

        has_buffer_section = (conf.elements(name: 'buffer').size > 0)
        has_flush_interval = conf.has_key?('flush_interval')

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
          if @chunk_keys.any? { |key|
              begin
                k = Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter(key)
                if k.is_a?(String)
                  k !~ CHUNK_KEY_PATTERN
                else
                  if key.start_with?('$[')
                    raise Fluent::ConfigError, "in chunk_keys: bracket notation is not allowed"
                  else
                    false
                  end
                end
              rescue => e
                raise Fluent::ConfigError, "in chunk_keys: #{e.message}"
              end
            }
            raise Fluent::ConfigError, "chunk_keys specification includes invalid char"
          else
            @chunk_key_accessors = Hash[@chunk_keys.map { |key| [key.to_sym, Fluent::PluginHelper::RecordAccessor::Accessor.new(key)] }]
          end

          if @chunk_key_time
            raise Fluent::ConfigError, "<buffer ...> argument includes 'time', but timekey is not configured" unless @buffer_config.timekey
            Fluent::Timezone.validate!(@buffer_config.timekey_zone)
            @timekey_zone = @buffer_config.timekey_use_utc ? '+0000' : @buffer_config.timekey_zone
            @timekey = @buffer_config.timekey
            @timekey_use_utc = @buffer_config.timekey_use_utc
            @offset = Fluent::Timezone.utc_offset(@timekey_zone)
            @calculate_offset = @offset.respond_to?(:call) ? @offset : nil
            @output_time_formatter_cache = {}
          end

          if (@chunk_key_tag ? 1 : 0) + @chunk_keys.size >= CHUNKING_FIELD_WARN_NUM
            log.warn "many chunk keys specified, and it may cause too many chunks on your system."
          end

          # no chunk keys or only tags (chunking can be done without iterating event stream)
          @simple_chunking = !@chunk_key_time && @chunk_keys.empty?

          @flush_mode = @buffer_config.flush_mode
          if @flush_mode == :default
            if has_flush_interval
              log.info "'flush_interval' is configured at out side of <buffer>. 'flush_mode' is set to 'interval' to keep existing behaviour"
              @flush_mode = :interval
            else
              @flush_mode = (@chunk_key_time ? :lazy : :interval)
            end
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

          if (@flush_mode != :interval) && buffer_conf.has_key?('flush_interval')
            if buffer_conf.has_key?('flush_mode')
              raise Fluent::ConfigError, "'flush_interval' can't be specified when 'flush_mode' is not 'interval' explicitly: '#{@flush_mode}'"
            else
              log.warn "'flush_interval' is ignored because default 'flush_mode' is not 'interval': '#{@flush_mode}'"
            end
          end

          if @buffer.queued_chunks_limit_size.nil?
            @buffer.queued_chunks_limit_size = @buffer_config.flush_thread_count
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
          unless @secondary.respond_to?(:acts_as_secondary)
            raise Fluent::ConfigError, "Failed to setup secondary plugin in '#{conf['@type']}'. '#{secondary_type}' plugin in not allowed due to non buffered output"
          end
          @secondary.acts_as_secondary(self)
          @secondary.configure(secondary_conf)
          if (self.class != @secondary.class) && (@custom_format || @secondary.implement?(:custom_format))
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
            # At here, this plugin works as non-buffered plugin.
            # Un-assign @buffer not to show buffering metrics (e.g., in_monitor_agent)
            @buffer = nil
          end
        end

        if @buffering
          m = method(:emit_buffered)
          singleton_class.module_eval do
            define_method(:emit_events, m)
          end

          @custom_format = implement?(:custom_format)
          @enable_msgpack_streamer = @custom_format ? formatted_to_msgpack_binary : true
          @delayed_commit = if implement?(:buffered) && implement?(:delayed_commit)
                              prefer_delayed_commit
                            else
                              implement?(:delayed_commit)
                            end
          @delayed_commit_timeout = @buffer_config.delayed_commit_timeout
        else # !@buffering
          m = method(:emit_sync)
          singleton_class.module_eval do
            define_method(:emit_events, m)
          end
        end

        if @buffering && !@as_secondary
          @retry = nil
          @retry_mutex = Mutex.new

          @buffer.start

          @output_enqueue_thread = nil
          @output_enqueue_thread_running = true

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
            thread_state = FlushThreadState.new(nil, nil, Mutex.new, ConditionVariable.new)
            thread = thread_create(thread_title) do
              flush_thread_run(thread_state)
            end
            thread_state.thread = thread
            @output_flush_threads_mutex.synchronize do
              @output_flush_threads << thread_state
            end
          end
          @output_flush_thread_current_position = 0

          if !@under_plugin_development && (@flush_mode == :interval || @chunk_key_time)
            @output_enqueue_thread = thread_create(:enqueue_thread, &method(:enqueue_thread_run))
          end
        end
        @secondary.start if @secondary
      end

      def after_start
        super
        @secondary.after_start if @secondary
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
          # Need to ensure to stop enqueueing ... after #shutdown, we cannot write any data
          @output_enqueue_thread_running = false
          if @output_enqueue_thread && @output_enqueue_thread.alive?
            @output_enqueue_thread.wakeup
            @output_enqueue_thread.join
          end
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
              # to wakeup thread and make it to stop by itself
              state.mutex.synchronize {
                if state.thread && state.thread.status
                  state.next_clock = 0
                  state.cond_var.signal
                end
              }
              Thread.pass
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

      def placeholder_validate!(name, str)
        placeholder_validators(name, str).each do |v|
          v.validate!
        end
      end

      def placeholder_validators(name, str, time_key = (@chunk_key_time && @buffer_config.timekey), tag_key = @chunk_key_tag, chunk_keys = @chunk_keys)
        validators = []

        sec, title, example = get_placeholders_time(str)
        if sec || time_key
          validators << PlaceholderValidator.new(name, str, :time, {sec: sec, title: title, example: example, timekey: time_key})
        end

        parts = get_placeholders_tag(str)
        if tag_key || !parts.empty?
          validators << PlaceholderValidator.new(name, str, :tag, {parts: parts, tagkey: tag_key})
        end

        keys = get_placeholders_keys(str)
        if chunk_keys && !chunk_keys.empty? || !keys.empty?
          validators << PlaceholderValidator.new(name, str, :keys, {keys: keys, chunkkeys: chunk_keys})
        end

        validators
      end

      class PlaceholderValidator
        attr_reader :name, :string, :type, :argument

        def initialize(name, str, type, arg)
          @name = name
          @string = str
          @type = type
          raise ArgumentError, "invalid type:#{type}" if @type != :time && @type != :tag && @type != :keys
          @argument = arg
        end

        def time?
          @type == :time
        end

        def tag?
          @type == :tag
        end

        def keys?
          @type == :keys
        end

        def validate!
          case @type
          when :time then validate_time!
          when :tag  then validate_tag!
          when :keys then validate_keys!
          end
        end

        def validate_time!
          sec = @argument[:sec]
          title = @argument[:title]
          example = @argument[:example]
          timekey = @argument[:timekey]
          if !sec && timekey
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' doesn't have timestamp placeholders for timekey #{timekey.to_i}"
          end
          if sec && !timekey
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' has timestamp placeholders, but chunk key 'time' is not configured"
          end
          if sec && timekey && timekey < sec
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' doesn't have timestamp placeholder for #{title}('#{example}') for timekey #{timekey.to_i}"
          end
        end

        def validate_tag!
          parts = @argument[:parts]
          tagkey = @argument[:tagkey]
          if tagkey && parts.empty?
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' doesn't have tag placeholder"
          end
          if !tagkey && !parts.empty?
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' has tag placeholders, but chunk key 'tag' is not configured"
          end
        end

        def validate_keys!
          keys = @argument[:keys]
          chunk_keys = @argument[:chunkkeys]
          if (chunk_keys - keys).size > 0
            not_specified = (chunk_keys - keys).sort
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' doesn't have enough placeholders for keys #{not_specified.join(',')}"
          end
          if (keys - chunk_keys).size > 0
            not_satisfied = (keys - chunk_keys).sort
            raise Fluent::ConfigError, "Parameter '#{name}: #{string}' has placeholders, but chunk keys doesn't have keys #{not_satisfied.join(',')}"
          end
        end
      end

      TIME_KEY_PLACEHOLDER_THRESHOLDS = [
        [1, :second, '%S'],
        [60, :minute, '%M'],
        [3600, :hour, '%H'],
        [86400, :day, '%d'],
      ]
      TIMESTAMP_CHECK_BASE_TIME = Time.parse("2016-01-01 00:00:00 UTC")
      # it's not validated to use timekey larger than 1 day
      def get_placeholders_time(str)
        base_str = TIMESTAMP_CHECK_BASE_TIME.strftime(str)
        TIME_KEY_PLACEHOLDER_THRESHOLDS.each do |triple|
          sec = triple.first
          return triple if (TIMESTAMP_CHECK_BASE_TIME + sec).strftime(str) != base_str
        end
        nil
      end

      # -1 means whole tag
      def get_placeholders_tag(str)
        # [["tag"],["tag[0]"]]
        parts = []
        str.scan(CHUNK_TAG_PLACEHOLDER_PATTERN).map(&:first).each do |ph|
          if ph == "tag"
            parts << -1
          elsif ph =~ /^tag\[(-?\d+)\]$/
            parts << $1.to_i
          end
        end
        parts.sort
      end

      def get_placeholders_keys(str)
        str.scan(CHUNK_KEY_PLACEHOLDER_PATTERN).map{|ph| ph[2..-2]}.reject{|s| (s == "tag") || (s == 'chunk_id') }.sort
      end

      # TODO: optimize this code
      def extract_placeholders(str, chunk)
        metadata = if chunk.is_a?(Fluent::Plugin::Buffer::Chunk)
                     chunk_passed = true
                     chunk.metadata
                   else
                     chunk_passed = false
                     # For existing plugins. Old plugin passes Chunk.metadata instead of Chunk
                     chunk
                   end
        if metadata.empty?
          str.sub(CHUNK_ID_PLACEHOLDER_PATTERN) {
            if chunk_passed
              dump_unique_id_hex(chunk.unique_id)
            else
              log.warn "${chunk_id} is not allowed in this plugin. Pass Chunk instead of metadata in extract_placeholders's 2nd argument"
            end
          }
        else
          rvalue = str.dup
          # strftime formatting
          if @chunk_key_time # this section MUST be earlier than rest to use raw 'str'
            @output_time_formatter_cache[str] ||= Fluent::Timezone.formatter(@timekey_zone, str)
            rvalue = @output_time_formatter_cache[str].call(metadata.timekey)
          end
          # ${tag}, ${tag[0]}, ${tag[1]}, ... , ${tag[-2]}, ${tag[-1]}
          if @chunk_key_tag
            if str.include?('${tag}')
              rvalue = rvalue.gsub('${tag}', metadata.tag)
            end
            if str =~ CHUNK_TAG_PLACEHOLDER_PATTERN
              hash = {}
              tag_parts = metadata.tag.split('.')
              tag_parts.each_with_index do |part, i|
                hash["${tag[#{i}]}"] = part
                hash["${tag[#{i-tag_parts.size}]}"] = part
              end
              rvalue = rvalue.gsub(CHUNK_TAG_PLACEHOLDER_PATTERN, hash)
            end
            if rvalue =~ CHUNK_TAG_PLACEHOLDER_PATTERN
              log.warn "tag placeholder '#{$1}' not replaced. tag:#{metadata.tag}, template:#{str}"
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
          if rvalue =~ CHUNK_KEY_PLACEHOLDER_PATTERN
            log.warn "chunk key placeholder '#{$1}' not replaced. template:#{str}"
          end
          rvalue.sub(CHUNK_ID_PLACEHOLDER_PATTERN) {
            if chunk_passed
              dump_unique_id_hex(chunk.unique_id)
            else
              log.warn "${chunk_id} is not allowed in this plugin. Pass Chunk instead of metadata in extract_placeholders's 2nd argument"
            end
          }
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
            timekey = calculate_timekey(time)
            @buffer.metadata(timekey: timekey, tag: tag)
          elsif @chunk_key_time
            timekey = calculate_timekey(time)
            @buffer.metadata(timekey: timekey)
          else
            @buffer.metadata(tag: tag)
          end
        else
          timekey = if @chunk_key_time
                      calculate_timekey(time)
                    else
                      nil
                    end
          pairs = Hash[@chunk_key_accessors.map { |k, a| [k, a.call(record)] }]
          @buffer.metadata(timekey: timekey, tag: (@chunk_key_tag ? tag : nil), variables: pairs)
        end
      end

      def calculate_timekey(time)
        time_int = time.to_i
        if @timekey_use_utc
          (time_int - (time_int % @timekey)).to_i
        else
          offset = @calculate_offset ? @calculate_offset.call(time) : @offset
          (time_int - ((time_int + offset)% @timekey)).to_i
        end
      end

      def chunk_for_test(tag, time, record)
        require 'fluent/plugin/buffer/memory_chunk'

        m = metadata_for_test(tag, time, record)
        Fluent::Plugin::Buffer::MemoryChunk.new(m)
      end

      def metadata_for_test(tag, time, record)
        raise "BUG: #metadata_for_test is available only when no actual metadata exists" unless @buffer.metadata_list.empty?
        m = metadata(tag, time, record)
        @buffer.metadata_list_clear!
        m
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
          log.warn "failed to write data into buffer by buffer overflow", action: @buffer_config.overflow_action
          case @buffer_config.overflow_action
          when :throw_exception
            raise
          when :block
            log.debug "buffer.write is now blocking"
            until @buffer.storable?
              if self.stopped?
                log.error "breaking block behavior to shutdown Fluentd"
                # to break infinite loop to exit Fluentd process
                raise
              end
              log.trace "sleeping until buffer can store more data"
              sleep 1
            end
            log.debug "retrying buffer.write after blocked operation"
            retry
          when :drop_oldest_chunk
            begin
              oldest = @buffer.dequeue_chunk
              if oldest
                log.warn "dropping oldest chunk to make space after buffer overflow", chunk_id: dump_unique_id_hex(oldest.unique_id)
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

      FORMAT_MSGPACK_STREAM = ->(e){ e.to_msgpack_stream }
      FORMAT_COMPRESSED_MSGPACK_STREAM = ->(e){ e.to_compressed_msgpack_stream }
      FORMAT_MSGPACK_STREAM_TIME_INT = ->(e){ e.to_msgpack_stream(time_int: true) }
      FORMAT_COMPRESSED_MSGPACK_STREAM_TIME_INT = ->(e){ e.to_compressed_msgpack_stream(time_int: true) }

      def generate_format_proc
        if @buffer && @buffer.compress == :gzip
          @time_as_integer ? FORMAT_COMPRESSED_MSGPACK_STREAM_TIME_INT : FORMAT_COMPRESSED_MSGPACK_STREAM
        else
          @time_as_integer ? FORMAT_MSGPACK_STREAM_TIME_INT : FORMAT_MSGPACK_STREAM
        end
      end

      # metadata_and_data is a Hash of:
      #  (standard format) metadata => event stream
      #  (custom format)   metadata => array of formatted event
      # For standard format, formatting should be done for whole event stream, but
      #   "whole event stream" may be a split of "es" here when it's bigger than chunk_limit_size.
      #   `@buffer.write` will do this splitting.
      # For custom format, formatting will be done here. Custom formatting always requires
      #   iteration of event stream, and it should be done just once even if total event stream size
      #   is bigger than chunk_limit_size because of performance.
      def handle_stream_with_custom_format(tag, es, enqueue: false)
        meta_and_data = {}
        records = 0
        es.each do |time, record|
          meta = metadata(tag, time, record)
          meta_and_data[meta] ||= []
          res = format(tag, time, record)
          if res
            meta_and_data[meta] << res
            records += 1
          end
        end
        write_guard do
          @buffer.write(meta_and_data, enqueue: enqueue)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        true
      end

      def handle_stream_with_standard_format(tag, es, enqueue: false)
        format_proc = generate_format_proc
        meta_and_data = {}
        records = 0
        es.each do |time, record|
          meta = metadata(tag, time, record)
          meta_and_data[meta] ||= MultiEventStream.new
          meta_and_data[meta].add(time, record)
          records += 1
        end
        write_guard do
          @buffer.write(meta_and_data, format: format_proc, enqueue: enqueue)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        true
      end

      def handle_stream_simple(tag, es, enqueue: false)
        format_proc = nil
        meta = metadata((@chunk_key_tag ? tag : nil), nil, nil)
        records = es.size
        if @custom_format
          records = 0
          data = []
          es.each do |time, record|
            res = format(tag, time, record)
            if res
              data << res
              records += 1
            end
          end
        else
          format_proc = generate_format_proc
          data = es
        end
        write_guard do
          @buffer.write({meta => data}, format: format_proc, enqueue: enqueue)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        true
      end

      def commit_write(chunk_id, delayed: @delayed_commit, secondary: false)
        log.on_trace { log.trace "committing write operation to a chunk", chunk: dump_unique_id_hex(chunk_id), delayed: delayed }

        if delayed
          @dequeued_chunks_mutex.synchronize do
            @dequeued_chunks.delete_if{ |info| info.chunk_id == chunk_id }
          end
        end
        @buffer.purge_chunk(chunk_id)

        @retry_mutex.synchronize do
          if @retry # success to flush chunks in retries
            if secondary
              log.warn "retry succeeded by secondary.", chunk_id: dump_unique_id_hex(chunk_id)
            else
              log.warn "retry succeeded.", chunk_id: dump_unique_id_hex(chunk_id)
            end
            @retry = nil
          end
        end
      end

      # update_retry parameter is for preventing busy loop by async write
      # We will remove this parameter by re-design retry_state management between threads.
      def rollback_write(chunk_id, update_retry: true)
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
          if update_retry
            primary = @as_secondary ? @primary_instance : self
            primary.update_retry_state(chunk_id, @as_secondary)
          end
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
              log.warn "failed to flush the buffer chunk, timeout to commit.", chunk_id: dump_unique_id_hex(info.chunk_id), flushed_at: info.time
              primary = @as_secondary ? @primary_instance : self
              primary.update_retry_state(info.chunk_id, @as_secondary)
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
              log.info "delayed commit for buffer chunks was cancelled in shutdown", chunk_id: dump_unique_id_hex(info.chunk_id)
              primary = @as_secondary ? @primary_instance : self
              primary.update_retry_state(info.chunk_id, @as_secondary)
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

      UNRECOVERABLE_ERRORS = [Fluent::UnrecoverableError, TypeError, ArgumentError, NoMethodError]

      def try_flush
        chunk = @buffer.dequeue_chunk
        return unless chunk

        log.on_trace { log.trace "trying flush for a chunk", chunk: dump_unique_id_hex(chunk.unique_id) }

        output = self
        using_secondary = false
        if @retry_mutex.synchronize{ @retry && @retry.secondary? }
          output = @secondary
          using_secondary = true
        end

        if @enable_msgpack_streamer
          chunk.extend ChunkMessagePackEventStreamer
        end

        begin
          chunk_write_start = Fluent::Clock.now

          if output.delayed_commit
            log.trace "executing delayed write and commit", chunk: dump_unique_id_hex(chunk.unique_id)
            @counters_monitor.synchronize{ @write_count += 1 }
            @dequeued_chunks_mutex.synchronize do
              # delayed_commit_timeout for secondary is configured in <buffer> of primary (<secondary> don't get <buffer>)
              @dequeued_chunks << DequeuedChunkInfo.new(chunk.unique_id, Time.now, self.delayed_commit_timeout)
            end

            output.try_write(chunk)
            check_slow_flush(chunk_write_start)
          else # output plugin without delayed purge
            chunk_id = chunk.unique_id
            dump_chunk_id = dump_unique_id_hex(chunk_id)
            log.trace "adding write count", instance: self.object_id
            @counters_monitor.synchronize{ @write_count += 1 }
            log.trace "executing sync write", chunk: dump_chunk_id

            output.write(chunk)
            check_slow_flush(chunk_write_start)

            log.trace "write operation done, committing", chunk: dump_chunk_id
            commit_write(chunk_id, delayed: false, secondary: using_secondary)
            log.trace "done to commit a chunk", chunk: dump_chunk_id
          end
        rescue *UNRECOVERABLE_ERRORS => e
          if @secondary
            if using_secondary
              log.warn "got unrecoverable error in secondary.", error: e
              log.warn_backtrace
              backup_chunk(chunk, using_secondary, output.delayed_commit)
            else
              if (self.class == @secondary.class)
                log.warn "got unrecoverable error in primary and secondary type is same as primary. Skip secondary", error: e
                log.warn_backtrace
                backup_chunk(chunk, using_secondary, output.delayed_commit)
              else
                # Call secondary output directly without retry update.
                # In this case, delayed commit causes inconsistent state in dequeued chunks so async output in secondary is not allowed for now.
                if @secondary.delayed_commit
                  log.warn "got unrecoverable error in primary and secondary is async output. Skip secondary for backup", error: e
                  log.warn_backtrace
                  backup_chunk(chunk, using_secondary, output.delayed_commit)
                else
                  log.warn "got unrecoverable error in primary. Skip retry and flush chunk to secondary", error: e
                  log.warn_backtrace
                  begin
                    @secondary.write(chunk)
                    commit_write(chunk_id, delayed: output.delayed_commit, secondary: true)
                  rescue => e
                    log.warn "got an error in secondary for unrecoverable error", error: e
                    log.warn_backtrace
                    backup_chunk(chunk, using_secondary, output.delayed_commit)
                  end
                end
              end
            end
          else
            log.warn "got unrecoverable error in primary and no secondary", error: e
            log.warn_backtrace
            backup_chunk(chunk, using_secondary, output.delayed_commit)
          end
        rescue => e
          log.debug "taking back chunk for errors.", chunk: dump_unique_id_hex(chunk.unique_id)
          if output.delayed_commit
            @dequeued_chunks_mutex.synchronize do
              @dequeued_chunks.delete_if{|d| d.chunk_id == chunk.unique_id }
            end
          end
          @buffer.takeback_chunk(chunk.unique_id)

          update_retry_state(chunk.unique_id, using_secondary, e)

          raise if @under_plugin_development && !@retry_for_error_chunk
        end
      end

      def backup_chunk(chunk, using_secondary, delayed_commit)
        if @buffer_config.disable_chunk_backup
          log.warn "disable_chunk_backup is true. #{dump_unique_id_hex(chunk.unique_id)} chunk is thrown away"
        else
          unique_id = dump_unique_id_hex(chunk.unique_id)
          safe_plugin_id = plugin_id.gsub(/[ "\/\\:;|*<>?]/, '_')
          backup_base_dir = system_config.root_dir || DEFAULT_BACKUP_DIR
          backup_file = File.join(backup_base_dir, 'backup', "worker#{fluentd_worker_id}", safe_plugin_id, "#{unique_id}.log")
          backup_dir = File.dirname(backup_file)

          log.warn "bad chunk is moved to #{backup_file}"
          FileUtils.mkdir_p(backup_dir) unless Dir.exist?(backup_dir)
          File.open(backup_file, 'ab', system_config.file_permission || 0644) { |f|
            chunk.write_to(f)
          }
        end
        commit_write(chunk.unique_id, secondary: using_secondary, delayed: delayed_commit)
      end

      def check_slow_flush(start)
        elapsed_time = Fluent::Clock.now - start
        if elapsed_time > @slow_flush_log_threshold
          log.warn "buffer flush took longer time than slow_flush_log_threshold:",
                   elapsed_time: elapsed_time, slow_flush_log_threshold: @slow_flush_log_threshold, plugin_id: self.plugin_id
        end
      end

      def update_retry_state(chunk_id, using_secondary, error = nil)
        @retry_mutex.synchronize do
          @counters_monitor.synchronize{ @num_errors += 1 }
          chunk_id_hex = dump_unique_id_hex(chunk_id)

          unless @retry
            @retry = retry_state(@buffer_config.retry_randomize)
            if error
              log.warn "failed to flush the buffer.", retry_time: @retry.steps, next_retry_seconds: @retry.next_time, chunk: chunk_id_hex, error: error
              log.warn_backtrace error.backtrace
            end
            return
          end

          # @retry exists

          if @retry.limit?
            if error
              records = @buffer.queued_records
              msg = "failed to flush the buffer, and hit limit for retries. dropping all chunks in the buffer queue."
              log.error msg, retry_times: @retry.steps, records: records, error: error
              log.error_backtrace error.backtrace
            end
            @buffer.clear_queue!
            log.debug "buffer queue cleared"
            @retry = nil
          else
            @retry.step
            if error
              if using_secondary
                msg = "failed to flush the buffer with secondary output."
                log.warn msg, retry_time: @retry.steps, next_retry_seconds: @retry.next_time, chunk: chunk_id_hex, error: error
                log.warn_backtrace error.backtrace
              else
                msg = "failed to flush the buffer."
                log.warn msg, retry_time: @retry.steps, next_retry_seconds: @retry.next_time, chunk: chunk_id_hex, error: error
                log.warn_backtrace error.backtrace
              end
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
        state.mutex.synchronize {
          if state.thread && state.thread.status # "run"/"sleep"/"aborting" or false(successfully stop) or nil(killed by exception)
            state.next_clock = 0
            state.cond_var.signal
          else
            log.warn "thread is already dead"
          end
        }
        Thread.pass
      end

      def force_flush
        if @buffering
          @buffer.enqueue_all(true)
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
          state.mutex.synchronize {
            if state.thread && state.thread.status
              state.next_clock = 0
              state.cond_var.signal
            end
          }
          Thread.pass
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

        while !self.after_started? && !self.stopped?
          sleep 0.5
        end
        log.debug "enqueue_thread actually running"

        begin
          while @output_enqueue_thread_running
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
              raise if @under_plugin_development
              log.error "unexpected error while checking flushed chunks. ignored.", error: e
              log.error_backtrace
            ensure
              @output_enqueue_thread_waiting = false
              @output_enqueue_thread_mutex.unlock
            end
            sleep interval
          end
        rescue => e
          # normal errors are rescued by inner begin-rescue clause.
          log.error "error on enqueue thread", error: e
          log.error_backtrace
          raise
        end
      end

      def flush_thread_run(state)
        flush_thread_interval = @buffer_config.flush_thread_interval

        state.next_clock = Fluent::Clock.now + flush_thread_interval

        while !self.after_started? && !self.stopped?
          sleep 0.5
        end
        log.debug "flush_thread actually running"

        state.mutex.lock
        begin
          # This thread don't use `thread_current_running?` because this thread should run in `before_shutdown` phase
          while @output_flush_threads_running
            current_clock = Fluent::Clock.now
            next_retry_time = nil

            @retry_mutex.synchronize do
              next_retry_time = @retry ? @retry.next_time : nil
            end

            if state.next_clock > current_clock
              interval = state.next_clock - current_clock
            elsif next_retry_time && next_retry_time > Time.now
              interval = next_retry_time.to_f - Time.now.to_f
            else
              state.mutex.unlock
              begin
                try_flush
                # next_flush_time uses flush_thread_interval or flush_thread_burst_interval (or retrying)
                interval = next_flush_time.to_f - Time.now.to_f
                # TODO: if secondary && delayed-commit, next_flush_time will be much longer than expected
                #       because @retry still exists (#commit_write is not called yet in #try_flush)
                #       @retry should be cleared if delayed commit is enabled? Or any other solution?
                state.next_clock = Fluent::Clock.now + interval
              ensure
                state.mutex.lock
              end
            end

            if @dequeued_chunks_mutex.synchronize{ !@dequeued_chunks.empty? && @dequeued_chunks.first.expired? }
              unless @output_flush_interrupted
                state.mutex.unlock
                begin
                  try_rollback_write
                ensure
                  state.mutex.lock
                end
              end
            end

            state.cond_var.wait(state.mutex, interval) if interval > 0
          end
        rescue => e
          # normal errors are rescued by output plugins in #try_flush
          # so this rescue section is for critical & unrecoverable errors
          log.error "error on output thread", error: e
          log.error_backtrace
          raise
        ensure
          state.mutex.unlock
        end
      end
    end
  end
end
