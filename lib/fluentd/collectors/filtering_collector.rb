#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
  module Collectors

    class FilteringCollector
      include Collector

      class FilteringWriter
        include Writer

        def initialize(writers)
          @writers = writers
        end

        def append(time, record)
          @writers.each {|w|
            time, record = w.append(time, record)
            break if time == nil
          }
        end

        def write(chunk)
          @writers.each {|w|
            chunk = w.write(chunk)
            break if chunk == nil
          }
        end

        def close
          @writers.reverse_each {|w|
            w.close  # TODO log
          }
        end
      end

      def initialize(collectors)
        @collectors = collectors
      end

      def open(tag)
        return ensure_close(open(tag), &proc) if block_given?

        writers = []
        begin
          @collectors.each {|c|
            writers << c.open(tag)
          }
          w = FilteringWriter.new(writers)
          writers = nil
          return w
        ensure
          if writers
            writers.reverse_each {|w|
              w.close  # TODO log
            }
          end
        end
      end
    end

  end
end
