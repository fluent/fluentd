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
  module Builtin

    class CopyFilter < Agent
      # TODO create base class for Output plugins
      include Collector
      include StreamSource

      Plugin.register_filter(:copy, self)

      class CopyWriter
        include Collector::Writer

        def initialize(writer)
          @writer = writer
        end

        def append(time, record)
          @writer.append(time, record)
          return time, record
        end

        def write(chunk)
          @writer.write(time, record)
          return time, record
        end
      end

      def open(tag, &block)
        stream_source.open(tag) do |w|
          yield CopyWriter.new(w)
        end
      end
    end

  end
end

