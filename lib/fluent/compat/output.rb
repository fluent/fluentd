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

require 'fluent/plugin'
require 'fluent/plugin/output'
require 'fluent/compat/output_chain'
require 'fluent/timezone'

require 'time'

module Fluent
  module Compat
    NULL_OUTPUT_CHAIN = NullOutputChain.instance

    module CompatOutputUtils
      def self.buffer_section(conf)
        conf.elements.select{|e| e.name == 'buffer'}.first
      end

      def self.secondary_section(conf)
        conf.elements.select{|e| e.name == 'secondary'}.first
      end

    end

    module BufferedEventStreamMixin
      include Enumerable

      def repeatable?
        true
      end

      def each(&block)
        msgpack_each(&block)
      end

      def to_msgpack_stream
        read
      end

      def key
        metadata.tag
      end
    end

    module AddTimeSliceKeyToChunkMixin
      def time_slice_format=(format)
        @_time_slice_format = format
      end

      def timekey_range=(range)
        @_timekey_range = range
      end

      def timezone=(tz)
        @_timezone = tz
      end

      def assume_timekey!
        @_formatter = Fluent::Timezone.formatter(@_timezone, @_time_slice_format)

        return if self.metadata.timekey
        if self.respond_to?(:path) && self.path =~ /\.(\d+)\.(?:b|q)(?:[a-z0-9]+)/
          begin
            self.metadata.timekey = Time.parse($1, @_time_slice_format).to_i
          rescue ArgumentError
            # unknown format / value as timekey
          end
        end
        unless self.metadata.timekey
          # file creation time is assumed in the time range of that time slice
          # because the first record should be in that range.
          time_int = self.created_at.to_i
          self.metadata.timekey = time_int - (time_int % @_timekey_range)
        end
      end

      def key
        @_formatter.call(self.metadata.timekey)
      end
    end

    module ChunkSizeCompatMixin
      def size
        self.bytesize
      end
    end

    module BufferedChunkMixin
      # prepend this module to BufferedOutput (including ObjectBufferedOutput) plugin singleton class
      def write(chunk)
        chunk.extend(ChunkSizeCompatMixin)
        super
      end
    end

    module TimeSliceChunkMixin
      # prepend this module to TimeSlicedOutput plugin singleton class
      def write(chunk)
        chunk.extend(ChunkSizeCompatMixin)
        chunk.extend(AddTimeSliceKeyToChunkMixin)
        chunk.time_slice_format = @time_slice_format
        chunk.timekey_range = @_timekey_range
        chunk.timezone = @timezone
        chunk.assume_timekey!
        super
      end
    end

    class Output < Fluent::Plugin::Output
      # TODO: warn when deprecated

      helpers :event_emitter

      def support_in_v12_style?(feature)
        case feature
        when :synchronous    then true
        when :buffered       then false
        when :delayed_commit then false
        when :custom_format  then false
        end
      end

      ## emit must be implemented in plugin
      # def emit(tag, es, chain)
      # end

      def process(tag, es)
        emit(tag, es, NULL_OUTPUT_CHAIN)
      end
    end

    class MultiOutput < Output
    end

    class BufferedOutput < Fluent::Plugin::Output
      # TODO: warn when deprecated

      helpers :event_emitter

      def support_in_v12_style?(feature)
        case feature
        when :synchronous    then false
        when :buffered       then true
        when :delayed_commit then false
        when :custom_format  then true
        end
      end

      desc 'The buffer type (memory, file)'
      config_param :buffer_type, :string, default: 'memory'
      desc 'The interval between data flushes.'
      config_param :flush_interval, :time, default: 60
      config_param :try_flush_interval, :float, default: 1
      desc 'If true, the value of `retry_value` is ignored and there is no limit'
      config_param :disable_retry_limit, :bool, default: false
      desc 'The limit on the number of retries before buffered data is discarded'
      config_param :retry_limit, :integer, default: 17
      desc 'The initial intervals between write retries.'
      config_param :retry_wait, :time, default: 1.0
      desc 'The maximum intervals between write retries.'
      config_param :max_retry_wait, :time, default: nil
      desc 'The number of threads to flush the buffer.'
      config_param :num_threads, :integer, default: 1
      desc 'The interval between data flushes for queued chunk.'
      config_param :queued_chunk_flush_interval, :time, default: 1

      desc 'The size of each buffer chunk.'
      config_param :buffer_chunk_limit, :size, default: 8*1024*1024
      desc 'The length limit of the chunk queue.'
      config_param :buffer_queue_limit, :integer, default: 256
      desc 'The action when the size of buffer queue exceeds the buffer_queue_limit.'
      config_param :buffer_queue_full_action, :enum, list: [:exception, :block], default: :exception

      config_param :flush_at_shutdown, :bool, default: true

      PARAMS_MAP = [
        ["buffer_type", "@type"],
        ["num_threads",    "flush_threads"],
        ["flush_interval", "flush_interval"],
        ["try_flush_interval",          "flush_thread_interval"],
        ["queued_chunk_flush_interval", "flush_burst_interval"],
        ["disable_retry_limit",         "retry_forever"],
        ["retry_limit",                 "retry_max_times"],
        ["max_retry_wait",              "retry_max_interval"],
        ["buffer_chunk_limit", "chunk_bytes_limit"],
        ["buffer_queue_limit", "queue_length_limit"],
        ["buffer_queue_full_action", nil], # TODO: implement this on fluent/plugin/buffer
        ["flush_at_shutdown",  "flush_at_shutdown"],
      ]

      def configure(conf)
        bufconf = CompatOutputUtils.buffer_section(conf)
        config_style = (bufconf ? :v1 : :v0)
        if config_style == :v0
          buf_params = {
            "flush_mode" => "fast",
            "retry_type" => "expbackoff",
          }
          PARAMS_MAP.each do |older, newer|
            buf_params[newer] = conf[older] if conf.has_key?(older)
          end

          bufconf = Fluent::Config::Element.new('buffer', '', buf_params, [])

          conf.elements << bufconf

          secconf = CompatOutputUtils.secondary_section(conf)
          if secconf
            if secconf['type'] && !secconf['@type']
              secconf['@type'] = secconf['type']
              log.warn "'type' is deprecated, and will be ignored in v1: use '@type' instead."
            end
          end
        end

        methods_of_plugin = self.class.instance_methods(false)
        @overrides_format_stream = methods_of_plugin.include?(:format_stream)

        super

        if config_style == :v1
          unless @buffer_config.chunk_keys.empty?
            raise Fluent::ConfigError, "this plugin '#{self.class}' cannot handle arguments for <buffer ...> section"
          end
        end

        (class << self; self; end).module_eval do
          prepend BufferedChunkMixin
        end
      end

      # #format MUST be implemented in plugin
      # #write is also

      # This method overrides Fluent::Plugin::Output#handle_stream_simple
      # because v0.12 BufferedOutput may overrides #format_stream, but original #handle_stream_simple method doesn't consider about it
      def handle_stream_simple(tag, es)
        if @overrides_format_stream
          meta = metadata(nil, nil, nil)
          bulk = format_stream(tag, es)
          @buffer.emit_bulk(meta, bulk, es.size)
          return [meta]
        end

        meta = metadata(nil, nil, nil)
        es_size = 0
        es_bulk = es.map{|time,record| es_size += 1; format(tag, time, record) }.join
        @buffer.emit_bulk(meta, es_bulk, es_size)
        @counters_monitor.synchronize{ @emit_records += es_size }
        [meta]
      end

      def extract_placeholders(str, metadata)
        raise "BUG: compat plugin does not support extract_placeholders: use newer plugin API"
      end
    end

    class ObjectBufferedOutput < Fluent::Plugin::Output
      # TODO: warn when deprecated

      helpers :event_emitter

      # This plugin cannot inherit BufferedOutput because #configure sets chunk_key 'tag'
      # to flush chunks per tags, but BufferedOutput#configure doesn't allow setting chunk_key
      # in v1 style configuration

      def support_in_v12_style?(feature)
        case feature
        when :synchronous    then false
        when :buffered       then true
        when :delayed_commit then false
        when :custom_format  then false
        end
      end

      desc 'The buffer type (memory, file)'
      config_param :buffer_type, :string, default: 'memory'
      desc 'The interval between data flushes.'
      config_param :flush_interval, :time, default: 60
      config_param :try_flush_interval, :float, default: 1
      desc 'If true, the value of `retry_value` is ignored and there is no limit'
      config_param :disable_retry_limit, :bool, default: false
      desc 'The limit on the number of retries before buffered data is discarded'
      config_param :retry_limit, :integer, default: 17
      desc 'The initial intervals between write retries.'
      config_param :retry_wait, :time, default: 1.0
      desc 'The maximum intervals between write retries.'
      config_param :max_retry_wait, :time, default: nil
      desc 'The number of threads to flush the buffer.'
      config_param :num_threads, :integer, default: 1
      desc 'The interval between data flushes for queued chunk.'
      config_param :queued_chunk_flush_interval, :time, default: 1

      desc 'The size of each buffer chunk.'
      config_param :buffer_chunk_limit, :size, default: 8*1024*1024
      desc 'The length limit of the chunk queue.'
      config_param :buffer_queue_limit, :integer, default: 256
      desc 'The action when the size of buffer queue exceeds the buffer_queue_limit.'
      config_param :buffer_queue_full_action, :enum, list: [:exception, :block], default: :exception

      config_param :flush_at_shutdown, :bool, default: true

      config_set_default :time_as_integer, true

      PARAMS_MAP = [
        ["buffer_type", "@type"],
        ["num_threads",    "flush_threads"],
        ["flush_interval", "flush_interval"],
        ["try_flush_interval",          "flush_thread_interval"],
        ["queued_chunk_flush_interval", "flush_burst_interval"],
        ["disable_retry_limit",         "retry_forever"],
        ["retry_limit",                 "retry_max_times"],
        ["max_retry_wait",              "retry_max_interval"],
        ["buffer_chunk_limit", "chunk_bytes_limit"],
        ["buffer_queue_limit", "queue_length_limit"],
        ["buffer_queue_full_action", nil], # TODO: implement this on fluent/plugin/buffer
        ["flush_at_shutdown",  "flush_at_shutdown"],
      ]

      def configure(conf)
        bufconf = CompatOutputUtils.buffer_section(conf)
        config_style = (bufconf ? :v1 : :v0)
        if config_style == :v0
          buf_params = {
            "flush_mode" => "fast",
            "retry_type" => "expbackoff",
          }
          PARAMS_MAP.each do |older, newer|
            buf_params[newer] = conf[older] if conf.has_key?(older)
          end

          bufconf = Fluent::Config::Element.new('buffer', 'tag', buf_params, [])

          conf.elements << bufconf

          secconf = CompatOutputUtils.secondary_section(conf)
          if secconf
            if secconf['type'] && !secconf['@type']
              secconf['@type'] = secconf['type']
              log.warn "'type' is deprecated, and will be ignored in v1: use '@type' instead."
            end
          end
        end

        super

        if config_style == :v1
          if @buffer_config.chunk_keys == ['tag']
            raise Fluent::ConfigError, "this plugin '#{self.class}' allows <buffer tag> only"
          end
        end

        (class << self; self; end).module_eval do
          prepend BufferedChunkMixin
        end
      end

      def format_stream(tag, es) # for BufferedOutputTestDriver
        es.to_msgpack_stream(time_int: @time_as_integer)
      end

      def write(chunk)
        write_objects(chunk.metadata.tag, chunk)
      end

      def extract_placeholders(str, metadata)
        raise "BUG: compat plugin does not support extract_placeholders: use newer plugin API"
      end
    end

    class TimeSlicedOutput < Fluent::Plugin::Output
      # TODO: warn when deprecated

      helpers :event_emitter

      def support_in_v12_style?(feature)
        case feature
        when :synchronous    then false
        when :buffered       then true
        when :delayed_commit then false
        when :custom_format  then true
        end
      end

      desc 'The buffer type (memory, file)'
      config_param :buffer_type, :string, default: 'file'
      desc 'The interval between data flushes.'
      config_param :flush_interval, :time, default: nil
      config_param :try_flush_interval, :float, default: 1
      desc 'If true, the value of `retry_value` is ignored and there is no limit'
      config_param :disable_retry_limit, :bool, default: false
      desc 'The limit on the number of retries before buffered data is discarded'
      config_param :retry_limit, :integer, default: 17
      desc 'The initial intervals between write retries.'
      config_param :retry_wait, :time, default: 1.0
      desc 'The maximum intervals between write retries.'
      config_param :max_retry_wait, :time, default: nil
      desc 'The number of threads to flush the buffer.'
      config_param :num_threads, :integer, default: 1
      desc 'The interval between data flushes for queued chunk.'
      config_param :queued_chunk_flush_interval, :time, default: 1

      desc 'The time format used as part of the file name.'
      config_param :time_slice_format, :string, default: '%Y%m%d'
      desc 'The amount of time Fluentd will wait for old logs to arrive.'
      config_param :time_slice_wait, :time, default: 10*60
      desc 'Parse the time value in the specified timezone'
      config_param :timezone, :string, default: nil

      desc 'The size of each buffer chunk.'
      config_param :buffer_chunk_limit, :size, default: 256*1024*1024
      desc 'The length limit of the chunk queue.'
      config_param :buffer_queue_limit, :integer, default: 256
      desc 'The action when the size of buffer queue exceeds the buffer_queue_limit.'
      config_param :buffer_queue_full_action, :enum, list: [:exception, :block], default: :exception

      config_param :flush_at_shutdown, :bool, default: false

      attr_accessor :localtime

      config_section :buffer, param_name: :buffer_config do
        config_set_default :@type, 'file2'
      end

      PARAMS_MAP = [
        ["buffer_type", "@type"],
        ["buffer_path", "path"],
        ["num_threads",    "flush_threads"],
        ["flush_interval", "flush_interval"],
        ["try_flush_interval",          "flush_thread_interval"],
        ["queued_chunk_flush_interval", "flush_burst_interval"],
        ["disable_retry_limit",         "retry_forever"],
        ["retry_limit",                 "retry_max_times"],
        ["max_retry_wait",              "retry_max_interval"],
        ["buffer_chunk_limit", "chunk_bytes_limit"],
        ["buffer_queue_limit", "queue_length_limit"],
        ["buffer_queue_full_action", nil], # TODO: implement this on fluent/plugin/buffer
        ["flush_at_shutdown",  "flush_at_shutdown"],
        ["time_slice_wait", "timekey_wait"],
      ]

      def initialize
        super
        @localtime = true
      end

      def configure(conf)
        bufconf = CompatOutputUtils.buffer_section(conf)
        config_style = (bufconf ? :v1 : :v0)
        if config_style == :v0
          buf_params = {
            "@type"      => "file2",
            "flush_mode" => (conf['flush_interval'] ? "fast" : "none"),
            "retry_type" => "expbackoff",
          }
          PARAMS_MAP.each do |older, newer|
            buf_params[newer] = conf[older] if conf.has_key?(older)
          end
          unless buf_params.has_key?("@type")
            buf_params["@type"] = "file2"
          end

          if conf['timezone']
            @timezone = conf['timezone']
            Fluent::Timezone.validate!(@timezone)
          elsif conf['utc']
            @timezone = "+0000"
            @localtime = false
          elsif conf['localtime']
            @timezone = Time.now.strftime('%z')
            @localtime = true
          else
            @timezone = "+0000" # v0.12 assumes UTC without any configuration
            @localtime = false
          end

          @_timekey_range = case conf['time_slice_format']
                            when /\%S/ then 1
                            when /\%M/ then 60
                            when /\%H/ then 3600
                            when /\%d/ then 86400
                            when nil   then 86400 # default value of TimeSlicedOutput.time_slice_format is '%Y%m%d'
                            else
                              raise Fluent::ConfigError, "time_slice_format only with %Y or %m is too long"
                            end
          buf_params["timekey_range"] = @_timekey_range

          bufconf = Fluent::Config::Element.new('buffer', 'time', buf_params, [])

          conf.elements << bufconf

          secconf = CompatOutputUtils.secondary_section(conf)
          if secconf
            if secconf['type'] && !secconf['@type']
              secconf['@type'] = secconf['type']
              log.warn "'type' is deprecated, and will be ignored in v1: use '@type' instead."
            end
          end
        end

        super

        if config_style == :v1
          if @buffer_config.chunk_keys == ['tag']
            raise Fluent::ConfigError, "this plugin '#{self.class}' allows <buffer tag> only"
          end
        end

        (class << self; self; end).module_eval do
          prepend TimeSliceChunkMixin
        end
      end

      # Original TimeSlicedOutput#emit doesn't call #format_stream

      # #format MUST be implemented in plugin
      # #write is also

      def extract_placeholders(str, metadata)
        raise "BUG: compat plugin does not support extract_placeholders: use newer plugin API"
      end
    end
  end
end
