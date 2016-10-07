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
require 'tempfile'
require 'zlib'

module Fluent
  module Plugin
    class Buffer # fluent/plugin/buffer is already loaded
      class Chunk
        include MonitorMixin
        include UniqueId::Mixin

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
        def append(data, **kwargs)
          raise ArgumentError, '`compress: gzip` can be used for Compressable module' if kwargs[:compress] == :gzip
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
          raise ArgumentError, '`compressed: gzip` can be used for Compressable module' if kwargs[:compressed] == :gzip
          raise NotImplementedError, "Implement this method in child class"
        end

        def open(**kwargs, &block)
          raise ArgumentError, '`compressed: gzip` can be used for Compressable module' if kwargs[:compressed] == :gzip
          raise NotImplementedError, "Implement this method in child class"
        end

        def write_to(io, **kwargs)
          raise ArgumentError, '`compressed: gzip` can be used for Compressable module' if kwargs[:compressed] == :gzip
          open do |i|
            IO.copy_stream(i, io)
          end
        end

        module Decompressable
          include Fluent::Plugin::Compressable

          def append(data, **kwargs)
            if kwargs[:compress] == :gzip
              io = StringIO.new
              Zlib::GzipWriter.wrap(io) do |gz|
                data.each do |d|
                  gz.write d
                end
              end
              concat(io.string, data.size)
            else
              super
            end
          end

          def open(**kwargs, &block)
            if kwargs[:compressed] == :gzip
              super
            else
              super(kwargs) do |chunk_io|
                output_io = if chunk_io.is_a?(StringIO)
                              StringIO.new
                            else
                              Tempfile.new('decompressed-data')
                            end
                decompress(input_io: chunk_io, output_io: output_io)
                output_io.seek(0, IO::SEEK_SET)
                yield output_io
              end
            end
          end

          def read(**kwargs)
            if kwargs[:compressed] == :gzip
              super
            else
              decompress(super)
            end
          end

          def write_to(io, **kwargs)
            open(compressed: :gzip) do |chunk_io|
              if kwargs[:compressed] == :gzip
                IO.copy_stream(chunk_io, io)
              else
                decompress(input_io: chunk_io, output_io: io)
              end
            end
          end
        end
      end
    end
  end
end
