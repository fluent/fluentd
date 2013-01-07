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
  module Plugins

    class CopyOutput < Outputs::BasicOutput
      include StreamSource

      Plugin.register_output(:copy, self)

      class CopyWriter
        include Writer

        def initialize(writers)
          @writers = writers
        end

        def append(time, record)
          @writers.each {|w|
            w.append(time, record)
          }
        end

        def write(chunk)
          @writers.each {|w|
            w.write(chunk)
          }
        end

        def close
          @writers.each {|w|
            w.close
          }
        end
      end

      def open(tag)
        writers = []
        writers << stream_source.open(tag)
        writers << stream_source.default_collector.open(tag)  # parent bus
        return CopyWriter.new(writers)
      end
    end

  end
end
