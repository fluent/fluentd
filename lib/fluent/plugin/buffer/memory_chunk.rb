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

require 'fluent/plugin/buffer/chunk'

module Fluent
  module Plugin
    class Buffer
      class MemoryChunk < Chunk
        def initialize(metadata)
          super
          @chunk = ''.force_encoding(Encoding::ASCII_8BIT)
          @chunk_bytes = 0
          @adding_bytes = 0
          @adding_records = 0
        end

        def append(data)
          adding = data.join.force_encoding(Encoding::ASCII_8BIT)
          @chunk << adding
          @adding_bytes += adding.bytesize
          @adding_records += data.size
          true
        end

        def concat(bulk, records)
          bulk.force_encoding(Encoding::ASCII_8BIT)
          @chunk << bulk
          @adding_bytes += bulk.bytesize
          @adding_records += records
          true
        end

        def commit
          @records += @adding_records
          @chunk_bytes += @adding_bytes

          @adding_bytes = @adding_records = 0
          @modified_at = Time.now
          true
        end

        def rollback
          @chunk.slice!(@chunk_bytes, @adding_bytes)
          @adding_bytes = @adding_records = 0
          true
        end

        def size
          @chunk_bytes + @adding_bytes
        end

        def records
          @records + @adding_records
        end

        def empty?
          @chunk.empty?
        end

        def close
          true
        end

        def purge
          @chunk = ''.force_encoding("ASCII-8BIT")
          @chunk_bytes = @records = @adding_bytes = @adding_records = 0
          true
        end

        def read
          @chunk
        end

        def open(&block)
          StringIO.open(@chunk, &block)
        end
      end
    end
  end
end
