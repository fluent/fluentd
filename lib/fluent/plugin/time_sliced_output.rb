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

      def initialize
        super
        @localtime = true
        #@ignore_old = false   # TODO
      end

      config_param :time_slice_format, :string, :default => '%Y%m%d'
      config_param :time_slice_wait, :time, :default => 10*60
      config_param :timezone, :string, :default => nil
      config_set_default :buffer_type, 'file'  # overwrite default buffer_type
      config_set_default :buffer_chunk_limit, 256*1024*1024  # overwrite default buffer_chunk_limit
      config_set_default :flush_interval, nil

      attr_accessor :localtime
      attr_reader :time_slicer # for test

      def configure(conf)
        super

        if conf['utc']
          @localtime = false
        elsif conf['localtime']
          @localtime = true
        end

        if conf['timezone']
          @timezone = conf['timezone']
          Fluent::Timezone.validate!(@timezone)
        end

        if @timezone
          @time_slicer = Timezone.formatter(@timezone, @time_slice_format)
        elsif @localtime
          @time_slicer = Proc.new {|time|
            Time.at(time).strftime(@time_slice_format)
          }
        else
          @time_slicer = Proc.new {|time|
            Time.at(time).utc.strftime(@time_slice_format)
          }
        end

        @time_slice_cache_interval = time_slice_cache_interval
        @before_tc = nil
        @before_key = nil

        if @flush_interval
          if conf['time_slice_wait']
            $log.warn "time_slice_wait is ignored if flush_interval is specified: #{conf}"
          end
          @enqueue_buffer_proc = Proc.new do
            @buffer.keys.each {|key|
              @buffer.push(key)
            }
          end

        else
          @flush_interval = [60, @time_slice_cache_interval].min
          @enqueue_buffer_proc = Proc.new do
            nowslice = @time_slicer.call(Engine.now.to_i - @time_slice_wait)
            @buffer.keys.each {|key|
              if key < nowslice
                @buffer.push(key)
              end
            }
          end
        end
      end

      def emit(tag, es, chain)
        @emit_count += 1
        es.each {|time,record|
          tc = time / @time_slice_cache_interval
          if @before_tc == tc
            key = @before_key
          else
            @before_tc = tc
            key = @time_slicer.call(time)
            @before_key = key
          end
          data = format(tag, time, record)
          if @buffer.emit(key, data, chain)
            submit_flush
          end
        }
      end

      def enqueue_buffer(force = false)
        if force
          @buffer.keys.each {|key|
            @buffer.push(key)
          }
        else
          @enqueue_buffer_proc.call
        end
      end

      #def format(tag, event)
      #end

      private
      def time_slice_cache_interval
        if @time_slicer.call(0) != @time_slicer.call(60-1)
          return 1
        elsif @time_slicer.call(0) != @time_slicer.call(60*60-1)
          return 30
        elsif @time_slicer.call(0) != @time_slicer.call(24*60*60-1)
          return 60*30
        else
          return 24*60*30
        end
      end
    end
  end
end
