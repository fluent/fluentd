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

module Fluent
  module Plugin
    class ObjectBufferedOutput < BufferedOutput
      def initialize
        super
      end

      def emit(tag, es, chain)
        @emit_count += 1
        data = es.to_msgpack_stream
        key = tag
        if @buffer.emit(key, data, chain)
          submit_flush
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
      end

      def write(chunk)
        chunk.extend(BufferedEventStreamMixin)
        write_objects(chunk.key, chunk)
      end
    end
  end
end

