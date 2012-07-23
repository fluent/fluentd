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


  class FilteredCollector
    include Collector

    class Writer
      include Collector::Writer

      def initialize(writers)
        @writers = writers
      end

      def append(time, record)
        @writers.each {|s|
          time, record = s.append(time, record)
        }
      end

      def write(chunk)
        @writers.each {|z|
          chunk = z.write(chunk)
        }
      end
    end

    def initialize(collectors)
      code = %[lambda do |tag,&block|\n]
      code << %[stack = Array.new(#{collectors.size})\n]
      collectors.size.times {|i|
        code << %[collectors[#{i}].open(tag) {|c|\n]
        code << %[stack[#{i}] = c\n]
      }
      code << %[block.call Writer.new(stack)\n]
      collectors.size.times {|i|
        code << %[}\n]
      }
      code << %[end\n]
      @proc = eval(code)
    end

    def open(tag, &block)
      @proc.call(tag, &block)
    end
  end


end
