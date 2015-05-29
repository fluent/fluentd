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

require 'fluent/plugin/buffered_output'
require 'fluent/timezone'

module Fluent
  module Plugin
    class TimeSlicedOutput < BufferedOutput
      DEFAULT_CHUNK_BYTES_LIMIT = 256 * 1024 * 1024 # 256MB for file

      desc 'The time format used as part of the file name.'
      config_param :time_slice_format, :string, default: '%Y%m%d'
      desc 'The amount of time Fluentd will wait for old logs to arrive.'
      config_param :time_slice_wait, :time, default: 10*60

      desc 'Parse the time value in the specified timezone'
      config_param :timezone, :string, default: nil # most authoritive if specified
      config_param :localtime, :bool, default: true # localtime and utc are exclusive
      config_param :utc, :bool, default: false

      config_section :buffer, param_name: :buffer_config do
        config_set_default :type, 'file' # overwrite default buffer_type
        config_set_default :chunk_bytes_limit, DEFAULT_CHUNK_BYTES_LIMIT
        config_set_default :flush_interval, nil
      end

      attr_reader :time_slicer # for test

      def configure(conf)
        super

        if @timezone
          Fluent::Timezone.validate!(@timezone)
          @time_slicer = Timezone.formatter(@timezone, @time_slice_format)
        else
          if @utc
            @localtime = false # if utc is set true explicitly, @localtime should be false
          elsif !@localtime # if localtime is set false explicitly, @utc should be true
            @utc = true
          end

          if @localtime
            @time_slicer = Proc.new {|time|
              Time.at(time).strftime(@time_slice_format)
            }
          else # UTC
            @time_slicer = Proc.new {|time|
              Time.at(time).utc.strftime(@time_slice_format)
            }
          end
        end

        @time_slice_cache_interval = time_slice_cache_interval
        @before_tc = nil
        @before_key = nil

        @flush_interval = @buffer_config.flush_interval

        if @flush_interval
          if @time_slice_wait
            log.warn "time_slice_wait is ignored if flush_interval is specified"
          end
          @chunk_enqueue_rule = ->(chunk){ chunk.created_at + @flush_interval > Time.now }
        else
          @flush_interval = [60, @time_slice_cache_interval].min
          @chunk_enqueue_rule = ->(chunk){
            current_slice = @time_slicer.call(Time.now.to_i - @time_slice_wait)
            chunk.metadata.timekey < current_slice
          }
        end
      end

      def metadata(timekey, tag)
        @tag_chunked ? @buffer.metadata(timekey: timekey, tag: tag) : @buffer.metadata(timekey: timekey)
      end

      def handle_stream(tag, es)
        @emit_count += 1
        emitted_meta = {}

        es.each do |time, record|
          ts = time / @time_slice_cache_interval
          timekey = if @before_tc == ts # same time_slice with event just before
                      @before_key
                    else # new time_slice, so update cache by calling @time_slicer (heavy)
                      @before_tc = ts
                      @before_key = @time_slicer.call(time)
                    end
          data = format(tag, time, record)
          meta = metadata
          @buffer.emit(meta, data)
          emitted_meta[meta] = true
        end

        emitted_meta.keys
      end

      def enqueue_buffer(force: false, test: nil)
        super(force: force, test: @chunk_enqueue_rule)
      end

      def time_slice_cache_interval
        if @time_slicer.call(0) != @time_slicer.call(60-1) # time slice length is seconds (1-59)
          1
        elsif @time_slicer.call(0) != @time_slicer.call(60*60-1) # time slice length is minutes
          30
        elsif @time_slicer.call(0) != @time_slicer.call(24*60*60-1) # time slice length is hours
          60*30
        else # longer than day
          24*60*30
        end
      end
    end
  end
end
