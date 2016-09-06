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
        def initialize(metadata, compress: :text)
          super
          @chunk = ''.force_encoding(Encoding::ASCII_8BIT)
          @chunk_bytes = 0
          @adding_bytes = 0
          @adding_size = 0
        end

        def concat(bulk, bulk_size)
          raise "BUG: concatenating to unwritable chunk, now '#{self.state}'" unless self.writable?

          bulk.force_encoding(Encoding::ASCII_8BIT)
          @chunk << bulk
          @adding_bytes += bulk.bytesize
          @adding_size += bulk_size
          true
        end

        def commit
          @size += @adding_size
          @chunk_bytes += @adding_bytes

          @adding_bytes = @adding_size = 0
          @modified_at = Time.now
          true
        end

        def rollback
          @chunk.slice!(@chunk_bytes, @adding_bytes)
          @adding_bytes = @adding_size = 0
          true
        end

        def bytesize
          @chunk_bytes + @adding_bytes
        end

        def size
          @size + @adding_size
        end

        def empty?
          @chunk.empty?
        end

        def purge
          super
          @chunk = ''.force_encoding("ASCII-8BIT")
          @chunk_bytes = @size = @adding_bytes = @adding_size = 0
          true
        end

        def read(**kwargs)
          @chunk
        end

        def open(**kwargs, &block)
          StringIO.open(@chunk, &block)
        end

        def write_to(io, **kwargs)
          # re-implementation to optimize not to create StringIO
          io.write @chunk
        end
      end
    end
  end
end
