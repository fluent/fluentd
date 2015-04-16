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
require 'fluent/env'

require 'msgpack'

module Fluent
  module Plugin
    class Buffer
      class FileChunk < Chunk
        ### TODO: buf_file MUST refer the process global buffer directory path configuration, ex: <system>buffer_storage_path</system>


        ### buffer path user specified : /path/to/directory/user_specified_prefix.*.log
        ### buffer chunk path          : /path/to/directory/user_specified_prefix.b513b61c9791029c2513b61c9791029c2.log
        ### buffer chunk metadata path : /path/to/directory/user_specified_prefix.b513b61c9791029c2513b61c9791029c2.log.meta

        # NOTE: Old style buffer path of time sliced output plugins had a part of key: prefix.20150414.b513b61...suffix
        #       But this part is not used for any purpose. (Now metadata variable is used instead.)

        # state: b/q - 'b'(on stage), 'q'(enqueued)
        # path_prefix: path prefix string, ended with '.'
        # path_suffix: path suffix string, like '.log' (or any other user specified)

        # mode: 'w+'(newly created/on stage), 'r+'(already exists/on stage), 'r'(already exists/enqueued)
        def initialize(metadata, path, mode, perm=DEFAULT_FILE_PERMISSION)
          super

          # state: stage/blocked/queued
          #   blocked is internal status for chunks waiting to be enqueued

          @path = path
          @state = nil

          case mode
          when 'w+' # newly created / on stage
            @chunk_path = generate_stage_chunk_path(path)
            @meta_path = @chunk_path + '.meta'
            @chunk = File.open(@chunk_path, mode, perm)
            @chunk.sync = true
            @meta = File.open(@meta_path, 'w', perm)
            @meta.sync = true

            @state = :staged
            @bytesize = 0
            @commit_position = @chunk.pos # must be 0
            @adding_bytes = 0
            @adding_records = 0

          when 'r+' # already exists / on stage
            @chunk_path = path
            @meta_path = @chunk_path + '.meta'

            @chunk = File.open(@chunk_path, mode)
            @chunk.sync = true
            @chunk.seek(0, IO::SEEK_END)

            @bytesize = @chunk.size

            @meta = nil
            # staging buffer chunk without metadata is classic buffer chunk file
            # and it should be enqueued immediately
            if File.exist?(@meta_path)
              @meta = File.open(@meta_path, 'r+')
              data = MessagePack.unpack(@meta.read)
              restore_metadata(data)
              @meta.seek(0, IO::SEEK_SET)
              @meta.sync = true
              @state = :staged
              @commit_position = @chunk.pos
              @adding_bytes = 0
              @adding_records = 0
            else
              @state = :blocked # for writing
              @unique_id = unique_id_from_path(@chunk_path) || @unique_id
            end

          when 'r'  # already exists / enqueued
            @chunk_path = path
            @chunk = File.open(@chunk_path, mode)
            @bytesize = @chunk.size

            @meta_path = @chunk_path + '.meta'
            if File.readable?(@meta_path)
              data = MessagePack.unpack(File.open(@meta_path){|f| f.read })
              restore_metadata(data)
            end
            @state = :queued
          end
        end

        def generate_stage_chunk_path(path)
          prefix = path
          suffix = '.log'
          if pos = path.index('*')
            prefix = path[0...pos]
            suffix = path[(pos+1)..-1]
          end

          chunk_id = @unique_id.unpack('N*').map{|n| n.to_s(16)}.join
          state = 'b'
          "#{prefix}#{state}#{chunk_id}#{suffix}"
        end

        def generate_queued_chunk_path(path)
          if pos = path.index('b' + @unique_id)
            path.sub('b' + @unique_id, 'q' + @unique_id)
          else
            path + 'q' + @unique_id + '.log'
          end
        end

        # used only for queued buffer path
        def unique_id_from_path(path)
          if /\.q([0-9a-f]+)\.[a-zA-Z]\Z/ =~ path
            return $1.scan(/../).map{|x| x.to_i(16) }.pack('C*')
          end
          nil
        end

        def restore_metadata(data)
          @unique_id = data.delete('id') || unique_id_from_path(@chunk_path) || @unique_id
          @records = data.delete('r') || 0
          @created_at = Time.at(data.delete('c') || Time.now.to_i)
          @modified_at = Time.at(data.delete('m') || Time.now.to_i)
          @metadata.update(data)
        end

        def write_metadata(try_update: false)
          data = @metadata.merge({
              'id' => @unique_id,
              'r' => (try_update ? @records + @adding_records : @records),
              'c' => @created_at.to_i,
              'm' => (try_update ? Time.now.to_i : @modified_at.to_i)
          })
          @meta.seek(0, IO::SEEK_SET)
          @meta.truncate(0)
          @meta.write data
        end

        def enqueued!
          new_chunk_path = generate_queued_chunk_path(@chunk_path)
          new_meta_path = new_chunk_path + '.meta'

          write_metadata # re-write metadata w/ finalized records

          @chunk.rename(new_chunk_path)
          @meta.rename(new_meta_path)

          @chunk_path = new_chunk_path
          @meta_path = new_meta_path
          @state = :enqueued
        end

        def append(data)
          raise "BUG: appending to non-staged chunk, now '#{@state}'" unless @state == :staged

          adding = data.join.force_encoding('ASCII-8BIT')
          @chunk.write adding

          @adding_bytes = adding.bytesize
          @adding_records = data.size
          write_metadata(try_update: true) # of course, this operation may fail

          true
        end

        def commit
          @commit_position = @chunk.pos
          @records += @adding_records
          @bytesize += @adding_bytes
          @modified_at = Time.now

          @adding_bytes = @adding_records = 0
          true
        end

        def rollback
          @chunk.seek(@commit_position, IO::SEEK_SET)
          @chunk.truncate(@commit_position)
          @adding_bytes = @adding_records = 0
          true
        end

        def size
          @bytesize
        end

        def records
          @records + @adding_records
        end

        def empty?
          @bytesize == 0
        end

        def close
          @state = :closed
          size = @chunk.size
          @chunk.close
          @meta.close
          if size == 0
            File.unlink(@chunk_path, @meta_path)
          end
        end

        def purge
          @state = :closed
          @chunk.close
          @meta.close
          File.unlink(@chunk_path, @meta_path)
        end

        def read
          @chunk.seek(0, IO::SEEK_SET)
          @chunk.read
        end

        def open(&block) # TODO: check whether used or not
          @chunk.seek(0, IO::SEEK_SET)
          val = yield @chunk
          @chunk.seek(0, IO::SEEK_END) if @state == :stage
          val
        end

        ### TODO: fixit
        def write_to(io)
          open {|i|
            FileUtils.copy_stream(i, io)
          }
        end

        ### TODO: fixit
        def msgpack_each(&block)
          open {|io|
            u = MessagePack::Unpacker.new(io)
            begin
              u.each(&block)
            rescue EOFError
            end
          }
        end
      end
    end
  end
end
