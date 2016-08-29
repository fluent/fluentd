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

require 'fluent/plugin/buffer'
require 'fluent/plugin/compressable'
require 'fluent/unique_id'
require 'fluent/event'

require 'monitor'

module Fluent
  module Plugin
    class Buffer # fluent/plugin/buffer is alread loaded
      class Chunk
        include MonitorMixin
        include UniqueId::Mixin
        include ChunkMessagePackEventStreamer

        # Chunks has 2 part:
        # * metadata: contains metadata which should be restored after resume (if possible)
        #             v: {key=>value,key=>value,...} (optional)
        #             t: tag as string (optional)
        #             k: time slice key (optional)
        #
        #             id: unique_id of chunk (*)
        #             s: size (number of events in chunk) (*)
        #             c: created_at as unix time (*)
        #             m: modified_at as unix time (*)
        #              (*): fields automatically injected by chunk itself
        # * data: binary data, combined records represented as String, maybe compressed

        # NOTE: keys of metadata are named with a single letter
        #       to decread bytesize of metadata I/O

        # TODO: CompressedPackedMessage of forward protocol?

        def initialize(metadata, compress: :text)
          super()
          @unique_id = generate_unique_id
          @metadata = metadata

          # state: unstaged/staged/queued/closed
          @state = :unstaged

          @size = 0
          @created_at = Time.now
          @modified_at = Time.now

          extend Decompressable if compress == :gzip
        end

        attr_reader :unique_id, :metadata, :created_at, :modified_at, :state

        # data is array of formatted record string
        def append(data)
          adding = ''.b
          data.each do |d|
            adding << d.b
          end
          concat(adding, data.size)
        end

        # for event streams which is packed or zipped (and we want not to unpack/uncompress)
        def concat(bulk, records)
          raise NotImplementedError, "Implement this method in child class"
        end

        def commit
          raise NotImplementedError, "Implement this method in child class"
        end

        def rollback
          raise NotImplementedError, "Implement this method in child class"
        end

        def bytesize
          raise NotImplementedError, "Implement this method in child class"
        end

        def size
          raise NotImplementedError, "Implement this method in child class"
        end
        alias :length :size

        def empty?
          size == 0
        end

        def writable?
          @state == :staged || @state == :unstaged
        end

        def unstaged?
          @state == :unstaged
        end

        def staged?
          @state == :staged
        end

        def queued?
          @state == :queued
        end

        def closed?
          @state == :closed
        end

        def staged!
          @state = :staged
          self
        end

        def unstaged!
          @state = :unstaged
          self
        end

        def enqueued!
          @state = :queued
          self
        end

        def close
          @state = :closed
          self
        end

        def purge
          @state = :closed
          self
        end

        def read(**kwargs)
          raise NotImplementedError, "Implement this method in child class"
        end

        def open(&block)
          raise NotImplementedError, "Implement this method in child class"
        end

        def write_to(io, **kwargs)
          open do |i|
            IO.copy_stream(i, io)
          end
        end

        module Decompressable
          include Fluent::Plugin::Compressable

          def read(**kwargs)
            return super if kwargs[:compress] == :gzip

            # avoid creating duplicated IO
            if @chunk.is_a?(IO)
              # reset io(@chunk) to read
              @chunk.seek(0, IO::SEEK_SET)
              decompress('', io: @chunk) # not to call IO#read
            else
              decompress(super)
            end
          end

          def write_to(io, **kwargs)
            io.write(read(kwargs))
          end
        end
      end
    end
  end
end
