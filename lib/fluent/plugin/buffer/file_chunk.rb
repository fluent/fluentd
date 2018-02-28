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
require 'fluent/unique_id'
require 'fluent/msgpack_factory'

module Fluent
  module Plugin
    class Buffer
      class FileChunk < Chunk
        class FileChunkError < StandardError; end

        ### buffer path user specified : /path/to/directory/user_specified_prefix.*.log
        ### buffer chunk path          : /path/to/directory/user_specified_prefix.b513b61c9791029c2513b61c9791029c2.log
        ### buffer chunk metadata path : /path/to/directory/user_specified_prefix.b513b61c9791029c2513b61c9791029c2.log.meta

        # NOTE: Old style buffer path of time sliced output plugins had a part of key: prefix.20150414.b513b61...suffix
        #       But this part is not used now for any purpose. (Now metadata is used instead.)

        # state: b/q - 'b'(on stage, compatible with v0.12), 'q'(enqueued)
        # path_prefix: path prefix string, ended with '.'
        # path_suffix: path suffix string, like '.log' (or any other user specified)

        include SystemConfig::Mixin
        include MessagePackFactory::Mixin

        FILE_PERMISSION = 0644

        attr_reader :path, :permission

        def initialize(metadata, path, mode, perm: system_config.file_permission || FILE_PERMISSION, compress: :text)
          super(metadata, compress: compress)
          @permission = perm.is_a?(String) ? perm.to_i(8) : perm
          @bytesize = @size = @adding_bytes = @adding_size = 0
          @meta = nil

          case mode
          when :create then create_new_chunk(path, @permission)
          when :staged then load_existing_staged_chunk(path)
          when :queued then load_existing_enqueued_chunk(path)
          else
            raise ArgumentError, "Invalid file chunk mode: #{mode}"
          end
        end

        def concat(bulk, bulk_size)
          raise "BUG: concatenating to unwritable chunk, now '#{self.state}'" unless self.writable?

          bulk.force_encoding(Encoding::ASCII_8BIT)
          @chunk.write bulk
          @adding_bytes += bulk.bytesize
          @adding_size += bulk_size
          true
        end

        def commit
          write_metadata # this should be at first: of course, this operation may fail

          @commit_position = @chunk.pos
          @size += @adding_size
          @bytesize += @adding_bytes
          @adding_bytes = @adding_size = 0
          @modified_at = Time.now

          true
        end

        def rollback
          if @chunk.pos != @commit_position
            @chunk.seek(@commit_position, IO::SEEK_SET)
            @chunk.truncate(@commit_position)
          end
          @adding_bytes = @adding_size = 0
          true
        end

        def bytesize
          @bytesize + @adding_bytes
        end

        def size
          @size + @adding_size
        end

        def empty?
          @bytesize == 0
        end

        def enqueued!
          return unless self.staged?

          new_chunk_path = self.class.generate_queued_chunk_path(@path, @unique_id)
          new_meta_path = new_chunk_path + '.meta'

          write_metadata(update: false) # re-write metadata w/ finalized records

          begin
            file_rename(@chunk, @path, new_chunk_path, ->(new_io) { @chunk = new_io })
          rescue => e
            begin
              file_rename(@chunk, new_chunk_path, @path, ->(new_io) { @chunk = new_io }) if File.exist?(new_chunk_path)
            rescue => re
              # In this point, restore buffer state is hard because previous `file_rename` failed by resource problem.
              # Retry is one possible approach but it may cause livelock under limited resources or high load environment.
              # So we ignore such errors for now and log better message instead.
              # "Too many open files" should be fixed by proper buffer configuration and system setting.
              raise "can't enqueue buffer file and failed to restore. This may causes inconsistent state: path = #{@path}, error = '#{e}', retry error = '#{re}'"
            else
              raise "can't enqueue buffer file: path = #{@path}, error = '#{e}'"
            end
          end

          begin
            file_rename(@meta, @meta_path, new_meta_path, ->(new_io) { @meta = new_io })
          rescue => e
            begin
              file_rename(@chunk, new_chunk_path, @path, ->(new_io) { @chunk = new_io }) if File.exist?(new_chunk_path)
              file_rename(@meta, new_meta_path, @meta_path, ->(new_io) { @meta = new_io }) if File.exist?(new_meta_path)
            rescue => re
              # See above
              raise "can't enqueue buffer metadata and failed to restore. This may causes inconsistent state: path = #{@meta_path}, error = '#{e}', retry error = '#{re}'"
            else
              raise "can't enqueue buffer metadata: path = #{@meta_path}, error = '#{e}'"
            end
          end

          @path = new_chunk_path
          @meta_path = new_meta_path

          super
        end

        def close
          super
          size = @chunk.size
          @chunk.close
          @meta.close if @meta # meta may be missing if chunk is queued at first
          if size == 0
            File.unlink(@path, @meta_path)
          end
        end

        def purge
          super
          @chunk.close
          @meta.close if @meta
          @bytesize = @size = @adding_bytes = @adding_size = 0
          File.unlink(@path, @meta_path)
        end

        def read(**kwargs)
          @chunk.seek(0, IO::SEEK_SET)
          @chunk.read
        end

        def open(**kwargs, &block)
          @chunk.seek(0, IO::SEEK_SET)
          val = yield @chunk
          @chunk.seek(0, IO::SEEK_END) if self.staged?
          val
        end

        def self.assume_chunk_state(path)
          if /\.(b|q)([0-9a-f]+)\.[^\/]*\Z/n =~ path # //n switch means explicit 'ASCII-8BIT' pattern
            $1 == 'b' ? :staged : :queued
          else
            # files which matches to glob of buffer file pattern
            # it includes files which are created by out_file
            :unknown
          end
        end

        def self.generate_stage_chunk_path(path, unique_id)
          pos = path.index('.*.')
          raise "BUG: buffer chunk path on stage MUST have '.*.'" unless pos

          prefix = path[0...pos]
          suffix = path[(pos+3)..-1]

          chunk_id = Fluent::UniqueId.hex(unique_id)
          state = 'b'
          "#{prefix}.#{state}#{chunk_id}.#{suffix}"
        end

        def self.generate_queued_chunk_path(path, unique_id)
          chunk_id = Fluent::UniqueId.hex(unique_id)
          if path.index(".b#{chunk_id}.")
            path.sub(".b#{chunk_id}.", ".q#{chunk_id}.")
          else # for unexpected cases (ex: users rename files while opened by fluentd)
            path + ".q#{chunk_id}.chunk"
          end
        end

        # used only for queued v0.12 buffer path
        def self.unique_id_from_path(path)
          if /\.(b|q)([0-9a-f]+)\.[^\/]*\Z/n =~ path # //n switch means explicit 'ASCII-8BIT' pattern
            return $2.scan(/../).map{|x| x.to_i(16) }.pack('C*')
          end
          nil
        end

        def restore_metadata(bindata)
          data = msgpack_unpacker(symbolize_keys: true).feed(bindata).read rescue {}

          now = Time.now

          @unique_id = data[:id] || self.class.unique_id_from_path(@path) || @unique_id
          @size = data[:s] || 0
          @created_at = Time.at(data.fetch(:c, now.to_i))
          @modified_at = Time.at(data.fetch(:m, now.to_i))

          @metadata.timekey = data[:timekey]
          @metadata.tag = data[:tag]
          @metadata.variables = data[:variables]
        end

        def restore_metadata_partially(chunk)
          @unique_id = self.class.unique_id_from_path(chunk.path) || @unique_id
          @size = 0
          @created_at = chunk.ctime # birthtime isn't supported on Windows (and Travis?)
          @modified_at = chunk.mtime

          @metadata.timekey = nil
          @metadata.tag = nil
          @metadata.variables = nil
        end

        def write_metadata(update: true)
          data = @metadata.to_h.merge({
              id: @unique_id,
              s: (update ? @size + @adding_size : @size),
              c: @created_at.to_i,
              m: (update ? Time.now : @modified_at).to_i,
          })
          bin = msgpack_packer.pack(data).to_s
          @meta.seek(0, IO::SEEK_SET)
          @meta.write(bin)
          @meta.truncate(bin.bytesize)
        end

        def file_rename(file, old_path, new_path, callback=nil)
          pos = file.pos
          if Fluent.windows?
            file.close
            File.rename(old_path, new_path)
            file = File.open(new_path, 'rb', @permission)
          else
            File.rename(old_path, new_path)
            file.reopen(new_path, 'rb')
          end
          file.set_encoding(Encoding::ASCII_8BIT)
          file.sync = true
          file.binmode
          file.pos = pos
          callback.call(file) if callback
        end

        def create_new_chunk(path, perm)
          @path = self.class.generate_stage_chunk_path(path, @unique_id)
          @meta_path = @path + '.meta'
          begin
            @chunk = File.open(@path, 'wb+', perm)
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.sync = true
            @chunk.binmode
          rescue => e
            # Here assumes "Too many open files" like recoverable error so raising BufferOverflowError.
            # If other cases are possible, we will change erorr handling with proper classes.
            raise BufferOverflowError, "can't create buffer file for #{path}. Stop creating buffer files: error = #{e}"
          end
          begin
            @meta = File.open(@meta_path, 'wb', perm)
            @meta.set_encoding(Encoding::ASCII_8BIT)
            @meta.sync = true
            @meta.binmode
            write_metadata(update: false)
          rescue => e
            # This case is easier than enqueued!. Just removing pre-create buffer file
            @chunk.close rescue nil
            File.unlink(@path) rescue nil
            # Same as @chunk case. See above
            raise BufferOverflowError, "can't create buffer metadata for #{path}. Stop creating buffer files: error = #{e}"
          end

          @state = :unstaged
          @bytesize = 0
          @commit_position = @chunk.pos # must be 0
          @adding_bytes = 0
          @adding_size = 0
        end

        def load_existing_staged_chunk(path)
          @path = path
          @meta_path = @path + '.meta'

          @meta = nil
          # staging buffer chunk without metadata is classic buffer chunk file
          # and it should be enqueued immediately
          if File.exist?(@meta_path)
            raise FileChunkError, "staged file chunk is empty" if File.size(@path).zero?

            @chunk = File.open(@path, 'rb+')
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.sync = true
            @chunk.seek(0, IO::SEEK_END)
            @chunk.binmode

            @meta = File.open(@meta_path, 'rb+')
            @meta.set_encoding(Encoding::ASCII_8BIT)
            @meta.sync = true
            @meta.binmode
            begin
              restore_metadata(@meta.read)
            rescue => e
              @chunk.close
              @meta.close
              raise FileChunkError, "staged meta file is broken. #{e.message}"
            end
            @meta.seek(0, IO::SEEK_SET)

            @state = :staged
            @bytesize = @chunk.size
            @commit_position = @chunk.pos
            @adding_bytes = 0
            @adding_size = 0
          else
            # classic buffer chunk - read only chunk
            @chunk = File.open(@path, 'rb')
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.binmode
            @chunk.seek(0, IO::SEEK_SET)
            @state = :queued
            @bytesize = @chunk.size

            restore_metadata_partially(@chunk)

            @commit_position = @chunk.size
            @unique_id = self.class.unique_id_from_path(@path) || @unique_id
          end
        end

        def load_existing_enqueued_chunk(path)
          @path = path
          raise FileChunkError, "enqueued file chunk is empty" if File.size(@path).zero?

          @chunk = File.open(@path, 'rb')
          @chunk.set_encoding(Encoding::ASCII_8BIT)
          @chunk.binmode
          @chunk.seek(0, IO::SEEK_SET)
          @bytesize = @chunk.size
          @commit_position = @chunk.size

          @meta_path = @path + '.meta'
          if File.readable?(@meta_path)
            begin
              restore_metadata(File.open(@meta_path){|f| f.set_encoding(Encoding::ASCII_8BIT); f.binmode; f.read })
            rescue => e
              @chunk.close
              raise FileChunkError, "enqueued meta file is broken. #{e.message}"
            end
          else
            restore_metadata_partially(@chunk)
          end
          @state = :queued
        end
      end
    end
  end
end
