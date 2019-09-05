p #
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
require 'fluent/unique_id'
require 'fluent/msgpack_factory'
require 'chunkio'

module Fluent
  module Plugin
    class Buffer
      class ChunkioChunk < Chunk
        ## buffer chunk path : /${path}/${stream_name}/cio.b513b61c9791029c2513b61c9791029c2.${suffix}

        include SystemConfig::Mixin
        include MessagePackFactory::Mixin

        class ChunkioChunkError < StandardError; end

        # TODO: FILE_PERMISSION = 0644
        attr_reader :path

        def initialize(metadata, path, mode, chunk: c, compress: :text)
          super(metadata, compress: compress)
          @size = @adding_size = 0
          @cgenerator = chunk

          case mode
          when :create then create_new_chunk(path)
          when :staged, :queued, :assume then load_existing_chunk(path)
          else
            raise ArgumentError, "Invalid file chunk mode: #{mode}"
          end
        end

        def concat(bulk, bulk_size)
          unless writable?
            raise "BUG: concatenating to unwritable chunk, now '#{state}'"
          end

          if @adding_size == 0
            @chunk.tx_begin
          end

          bulk.force_encoding(Encoding::ASCII_8BIT)
          @adding_size += bulk_size
          @chunk.write(bulk)
          true
        end

        def commit
          n = Fluent::Clock.real_now
          write_metadata(update_at: n, size: @size + @adding_size)

          @chunk.tx_commit
          @size += @adding_size
          @adding_size = 0
          @modified_at = n
          @modified_at_object = nil
          true
        end

        def rollback
          if @adding_size == 0
            return
          end

          @chunk.tx_rollback
          @adding_size = 0

          true
        end

        def staged!
          write_metadata(enq: false, size: @size + @adding_size)

          super
        end

        def bytesize
          @chunk.bytesize
        end

        def size
          @size + @adding_size
        end

        def empty?
          bytesize == 0
        end

        def enqueued!
          unless staged?
            return
          end

          write_metadata(enq: true)

          super
        end

        def close
          super

          if @chunk
            @chunk.close
          end
        end

        def purge
          super

          if @chunk
            @chunk.unlink
          end
        end

        def read(**kwargs)
          @chunk.data
        end

        def open(**kwargs, &block)
          StringIO.open(@chunk.data, &block)
        end

        private

        def load_existing_chunk(path)
          @path = path
          @chunk =
            begin
              @cgenerator.create_chunk(name: File.basename(@path))
            rescue => e
              raise ChunkioChunkError, e
            end

          raw_meta = @chunk.metadata
          unless raw_meta.empty?
            restore_metadata(raw_meta)
          end
        end

        def create_new_chunk(path)
          @path = generate_chunk_path(path, @unique_id)
          begin
            @chunk = @cgenerator.create_chunk(name: File.basename(@path)) # TODO
          rescue => e
            raise ChunkioChunkError, e
          end
          @state = :unstaged
        end

        def restore_metadata(bindata)
          data =
            begin
              msgpack_unpacker(symbolize_keys: true).feed(bindata).read
            rescue => _
              {}
            end
          now = Fluent::Clock.real_now

          @unique_id = data[:id] || unique_id_from_path(@path) || @unique_id
          @size = data[:s] || 0
          @created_at = data.fetch(:c, now.to_i)
          @modified_at = data.fetch(:m, now.to_i)

          @metadata.timekey = data[:timekey]
          @metadata.tag = data[:tag]
          @metadata.variables = data[:variables]
          @state = data[:enq] ? :queued : :staged
        end

        def write_metadata(update_at: nil, size: nil, enq: false)
          data = @metadata.to_h.merge(
            id: @unique_id,
            s: (size || @size),
            c: @created_at,
            m: (update_at || @modified_at),
            enq: enq,
          )
          d = Fluent::MessagePackFactory.thread_local_msgpack_packer.pack(data).full_pack
          @chunk.set_metadata(d)
        end

        def unique_id_from_path(path)
          if /\.([0-9a-f]+)\.[^\/]*\Z/n =~ path # //n switch means explicit 'ASCII-8BIT' pattern
            return $2.scan(/../).map{ |x| x.to_i(16) }.pack('C*')
          end

          nil
        end

        def generate_chunk_path(path, unique_id)
          prefix, suffix = path.split('.*.', 2)
          unless suffix
            raise "BUG: buffer chunk path on stage MUST have '.*.'"
          end

          chunk_id = Fluent::UniqueId.hex(unique_id)
          "#{prefix}.#{chunk_id}.#{suffix}"
        end
      end
    end
  end
end
