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

      config_param :tag_chunked, :bool, default: true # actually, always true (this configuration value is ignored)

      def write_objects(chunk)
        raise NotImplementedError
      end

      def metadata(tag)
        @buffer.metadata(tag: tag)
      end

      def format_stream(tag, es)
        es.to_msgpack_stream
      end

      def write(chunk)
        chunk.extend(BufferedEventStreamMixin)
        write_objects(chunk)
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
    end
  end
end

