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
module Fluentd
  module Plugin

    class TimeSlicedOutput < BufferedOutput
      def initialize
        super
      end

      config_param :utc, :bool, :default => false
      config_param :time_slice_format, :string, :default => nil
      config_param :time_slice_interval, :time, :default => 60*60  # 1h
      config_param :time_slice_wait, :time, :default => 10*60
      # TODO
      #config_set_default :buffer_type, 'file'  # overwrite default buffer_type
      #config_set_default :buffer_chunk_limit, 256*1024*1024  # overwrite default buffer_chunk_limit
      config_set_default :flush_interval, nil

      def configure(conf)
        super

        tsi = @time_slice_interval.to_i

        tsf = @time_slice_format
        if tsf.nil?
          # set default value of time_slice_format
          if tsi < 60
            tsf = '%Y%m%d_%H%M%S'
          elsif tsi < 60*60
            tsf = '%Y%m%d_%H%M'
          elsif tsi < 60*60*24
            tsf = '%Y%m%d_%H'
          else
            tsf = '%Y%m%d'
          end
        end

        @time_slicer =
          if @utc
            Proc.new {|time|
              Time.at(time / tsi * tsi).utc.strftime(tsf)
            }
          else
            Proc.new {|time|
              Time.at(time / tsi * tsi).strftime(tsf)
            }
          end

        @time_slice_cache_interval = tsi
        @last_time = nil
        @last_key = nil

        if @flush_interval
          if conf['time_slice_wait']
            log.warn "time_slice_wait is ignored if flush_interval is specified: #{conf}"
          end

        else
          # set default flush interval
          @flush_interval = [60, @time_slice_cache_interval].min
          @buffer.early_flush_strategy = proc {|key,status|
            now_slice = @time_slicer.call(Time.now.to_i - @time_slice_wait)
            key < now_slice
          }
        end
      end

      # overwrides BufferedOutput#buffer_key
      def buffer_key(tag, time, record)
        tc = time / @time_slice_cache_interval
        if @last_time == tc
          return @last_key
        else
          key = @time_slicer.call(time)
          @last_time = tc
          return @last_key = key
        end
      end
    end

  end
end
