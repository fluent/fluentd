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

    require 'fluentd/event_collection'

    class ObjectBufferedOutput < BufferedOutput
      # override
      def emits(tag, es)
        @buffer.open do |a|
          begin
            data = es.to_msgpack_stream
            a.append(tag, data)
          rescue
            es.each {|time,record|
              data = handle_error(tag, time, record) {
                [tag, time, record].to_msgpack
              }
              a.append(tag, data) if data
            }
          end
        end
      end

      def write(chunk)
        chunk.extend(BufferChunkMixin)
        write_objects(chunk.key, chunk)
      end

      def write_objects(tag, chunk)
        raise NoMethodError, "#{self.class}#write_objects(tag, chunk) is not implemented"
      end

      module BufferChunkMixin
        include EventCollection

        def destructive_repeatable?
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
