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

require 'uri'
require 'fluent/plugin/buffer/chunk'
require 'fluent/unique_id'
require 'fluent/msgpack_factory'

module Fluent
  module Plugin
    class Buffer
      class FileSingleChunk < Chunk
        class FileChunkError < StandardError; end

        ## buffer path user specified : /path/to/directory
        ## buffer chunk path          : /path/to/directory/fsb.key.b513b61c9791029c2513b61c9791029c2.buf
        ## state: b/q - 'b'(on stage), 'q'(enqueued)

        PATH_EXT = 'buf'
        PATH_SUFFIX = ".#{PATH_EXT}"
        PATH_REGEXP = /\.(b|q)([0-9a-f]+)\.#{PATH_EXT}*\Z/n  # //n switch means explicit 'ASCII-8BIT' pattern

        attr_reader :path, :permission

        def initialize(metadata, path, mode, key, perm: Fluent::DEFAULT_FILE_PERMISSION, compress: :text)
          super(metadata, compress: compress)
          @key = key
          perm ||= Fluent::DEFAULT_FILE_PERMISSION
          @permission = perm.is_a?(String) ? perm.to_i(8) : perm
          @bytesize = @size = @adding_bytes = @adding_size = 0

          case mode
          when :create then create_new_chunk(path, metadata, @permission)
          when :staged then load_existing_staged_chunk(path)
          when :queued then load_existing_enqueued_chunk(path)
          else
            raise ArgumentError, "Invalid file chunk mode: #{mode}"
          end
        end

        def concat(bulk, bulk_size)
          raise "BUG: concatenating to unwritable chunk, now '#{self.state}'" unless self.writable?

          bulk.force_encoding(Encoding::ASCII_8BIT)
          @chunk.write(bulk)
          @adding_bytes += bulk.bytesize
          @adding_size += bulk_size
          true
        end

        def commit
          @commit_position = @chunk.pos
          @size += @adding_size
          @bytesize += @adding_bytes
          @adding_bytes = @adding_size = 0
          @modified_at = Fluent::Clock.real_now

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
          @bytesize.zero?
        end

        def enqueued!
          return unless self.staged?

          new_chunk_path = self.class.generate_queued_chunk_path(@path, @unique_id)

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

          @path = new_chunk_path

          super
        end

        def close
          super
          size = @chunk.size
          @chunk.close
          if size == 0
            File.unlink(@path)
          end
        end

        def purge
          super
          @chunk.close
          @bytesize = @size = @adding_bytes = @adding_size = 0
          File.unlink(@path)
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
          return :unknown unless path.end_with?(PATH_SUFFIX)

          if PATH_REGEXP =~ path
            $1 == 'b' ? :staged : :queued
          else
            # files which matches to glob of buffer file pattern
            # it includes files which are created by out_file
            :unknown
          end
        end

        def self.unique_id_and_key_from_path(path)
          base = File.basename(path)
          res = PATH_REGEXP =~ base
          return nil unless res

          key = base[4..res - 1] # remove 'fsb.' and '.'
          hex_id = $2            # remove '.' and '.buf'
          unique_id = hex_id.scan(/../).map {|x| x.to_i(16) }.pack('C*')
          [unique_id, key]
        end

        def self.generate_stage_chunk_path(path, key, unique_id)
          pos = path.index('.*.')
          raise "BUG: buffer chunk path on stage MUST have '.*.'" unless pos

          prefix = path[0...pos]
          suffix = path[(pos + 3)..-1]

          chunk_id = Fluent::UniqueId.hex(unique_id)
          "#{prefix}.#{key}.b#{chunk_id}.#{suffix}"
        end

        def self.generate_queued_chunk_path(path, unique_id)
          chunk_id = Fluent::UniqueId.hex(unique_id)
          staged_path = ".b#{chunk_id}."
          if path.index(staged_path)
            path.sub(staged_path, ".q#{chunk_id}.")
          else # for unexpected cases (ex: users rename files while opened by fluentd)
            path + ".q#{chunk_id}.chunk"
          end
        end

        def restore_metadata
          if res = self.class.unique_id_and_key_from_path(@path)
            @unique_id = res.first
            key = decode_key(res.last)
            if @key
              @metadata.variables = {@key => key}
            else
              @metadata.tag = key
            end
          else
            raise FileChunkError, "Invalid chunk found. unique_id and key not exist: #{@path}"
          end
          @size = 0

          stat = File.stat(@path)
          @created_at = stat.ctime.to_i
          @modified_at = stat.mtime.to_i
        end

        def restore_size(chunk_format)
          count = 0
          File.open(@path, 'rb') { |f|
            if chunk_format == :msgpack
              Fluent::MessagePackFactory.msgpack_unpacker(f).each { |d| count += 1 }
            else
              f.each_line { |l| count += 1 }
            end
          }
          @size = count
        end

        def file_rename(file, old_path, new_path, callback = nil)
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

        ESCAPE_REGEXP = /[^-_.a-zA-Z0-9]/n

        def encode_key(metadata)
          k = @key ? metadata.variables[@key] : metadata.tag
          k ||= ''
          URI::RFC2396_PARSER.escape(k, ESCAPE_REGEXP)
        end

        def decode_key(key)
          URI::RFC2396_PARSER.unescape(key)
        end

        def create_new_chunk(path, metadata, perm)
          @path = self.class.generate_stage_chunk_path(path, encode_key(metadata), @unique_id)
          begin
            @chunk = File.open(@path, 'wb+', perm)
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.sync = true
            @chunk.binmode
          rescue => e
            # Here assumes "Too many open files" like recoverable error so raising BufferOverflowError.
            # If other cases are possible, we will change error handling with proper classes.
            raise BufferOverflowError, "can't create buffer file for #{path}. Stop creating buffer files: error = #{e}"
          end

          @state = :unstaged
          @bytesize = 0
          @commit_position = @chunk.pos # must be 0
          @adding_bytes = 0
          @adding_size = 0
        end

        def load_existing_staged_chunk(path)
          @path = path
          raise FileChunkError, "staged file chunk is empty" if File.size(@path).zero?

          @chunk = File.open(@path, 'rb+')
          @chunk.set_encoding(Encoding::ASCII_8BIT)
          @chunk.sync = true
          @chunk.binmode
          @chunk.seek(0, IO::SEEK_END)

          restore_metadata

          @state = :staged
          @bytesize = @chunk.size
          @commit_position = @chunk.pos
          @adding_bytes = 0
          @adding_size = 0
        end

        def load_existing_enqueued_chunk(path)
          @path = path
          raise FileChunkError, "enqueued file chunk is empty" if File.size(@path).zero?

          @chunk = File.open(@path, 'rb')
          @chunk.set_encoding(Encoding::ASCII_8BIT)
          @chunk.binmode
          @chunk.seek(0, IO::SEEK_SET)

          restore_metadata

          @state = :queued
          @bytesize = @chunk.size
          @commit_position = @chunk.size
        end
      end
    end
  end
end
