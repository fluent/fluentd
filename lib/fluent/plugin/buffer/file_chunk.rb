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
        ### buffer path user specified : /path/to/directory/user_specified_prefix.*.log
        ### buffer chunk path          : /path/to/directory/user_specified_prefix.b513b61c9791029c2513b61c9791029c2.log
        ### buffer chunk metadata path : /path/to/directory/user_specified_prefix.b513b61c9791029c2513b61c9791029c2.log.meta

        # NOTE: Old style buffer path of time sliced output plugins had a part of key: prefix.20150414.b513b61...suffix
        #       But this part is not used for any purpose. (Now metadata is used instead.)

        # state: b/q - 'b'(on stage), 'q'(enqueued)
        # path_prefix: path prefix string, ended with '.'
        # path_suffix: path suffix string, like '.log' (or any other user specified)

        attr_reader :path, :state

        # mode: 'w+'(newly created/on stage), 'r+'(already exists/on stage), 'r'(already exists/enqueued)
        def initialize(metadata, path, mode, perm=DEFAULT_FILE_PERMISSION) # TODO: DEFAULT_FILE_PERMISSION is obsolete
          super

          # state: staged/queued/closed
          #   blocked is internal status for chunks waiting to be enqueued

          @state = nil

          case mode
          when 'w+' # newly created / on stage
            @path = generate_stage_chunk_path(path)
            @meta_path = @path + '.meta'
            @chunk = File.open(@path, mode, perm)
            @chunk.sync = true
            @meta = File.open(@meta_path, 'w', perm)
            @meta.sync = true

            @state = :staged
            @bytesize = 0
            @commit_position = @chunk.pos # must be 0
            @adding_bytes = 0
            @adding_records = 0

          when 'r+' # already exists / on stage
            @path = path
            @meta_path = @path + '.meta'

            @meta = nil
            # staging buffer chunk without metadata is classic buffer chunk file
            # and it should be enqueued immediately
            if File.exist?(@meta_path)
              @chunk = File.open(@path, mode)
              @chunk.sync = true
              @chunk.seek(0, IO::SEEK_END)

              @meta = File.open(@meta_path, 'r+')
              @meta.sync = true
              restore_metadata(@meta.read)
              @meta.seek(0, IO::SEEK_SET)

              @state = :staged
              @bytesize = @chunk.size
              @commit_position = @chunk.pos
              @adding_bytes = 0
              @adding_records = 0
            else
              # classic buffer chunk - read only chunk
              @chunk = File.open(@path, 'r')
              @state = :queued
              @bytesize = @chunk.size

              restore_metadata_partially(@chunk)

              @unique_id = unique_id_from_path(@path) || @unique_id
            end

          when 'r'  # already exists / enqueued
            @path = path
            @chunk = File.open(@path, mode)
            @bytesize = @chunk.size

            @meta_path = @path + '.meta'
            if File.readable?(@meta_path)
              restore_metadata(File.open(@meta_path){|f| f.read })
            else
              restore_metadata_partially(@chunk)
            end
            @state = :queued
          end
        end

        # MUST be called in critical section
        def append(data)
          raise "BUG: appending to non-staged chunk, now '#{@state}'" unless @state == :staged

          adding = data.join.force_encoding('ASCII-8BIT')
          @chunk.write adding

          @adding_bytes += adding.bytesize
          @adding_records += data.size
          write_metadata(try_update: true) # of course, this operation may fail

          true
        end

        # MUST be called in critical section
        def commit
          @commit_position = @chunk.pos
          @records += @adding_records
          @bytesize += @adding_bytes
          @modified_at = Time.now

          @adding_bytes = @adding_records = 0
          true
        end

        # MUST be called in critical section
        def rollback
          if @chunk.pos == @commit_position
            @chunk.seek(@commit_position, IO::SEEK_SET)
            @chunk.truncate(@commit_position)
          end
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
            File.unlink(@path, @meta_path)
          end
        end

        # MUST be called in critical section
        def purge
          @state = :closed
          @chunk.close
          @meta.close
          File.unlink(@path, @meta_path)
        end

        def read
          @chunk.seek(0, IO::SEEK_SET)
          @chunk.read
        end

        def open(&block) # TODO: check whether used or not
          @chunk.seek(0, IO::SEEK_SET)
          val = yield @chunk
          @chunk.seek(0, IO::SEEK_END) if @state == :staged
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

        # methods for FileBuffer operations

        def self.assume_chunk_state(path)
          if /\.(b|q)([0-9a-f]+)\.[^\/]*\Z/n =~ path # //n switch means explicit 'ASCII-8BIT' pattern
            $1 == 'b' ? :staged : :queued
          else
            :queued
          end
        end

        def generate_stage_chunk_path(path)
          pos = path.index('*')
          raise "BUG: buffer chunk path on stage MUST have '*'" unless pos

          prefix = path[0...pos]
          suffix = path[(pos+1)..-1]

          chunk_id = @unique_id.unpack('N*').map{|n| n.to_s(16)}.join
          state = 'b'
          "#{prefix}#{state}#{chunk_id}#{suffix}"
        end

        def generate_queued_chunk_path(path)
          chunk_id = @unique_id.unpack('N*').map{|n| n.to_s(16)}.join
          if pos = path.index('b' + chunk_id)
            path.sub('b' + chunk_id, 'q' + chunk_id)
          else # for unexpected cases (ex: users rename files while opened by fluentd)
            path + 'q' + chunk_id + '.log'
          end
        end

        # used only for queued buffer path
        def unique_id_from_path(path)
          if /\.(b|q)([0-9a-f]+)\.[^\/]*\Z/n =~ path # //n switch means explicit 'ASCII-8BIT' pattern
            return $2.scan(/../).map{|x| x.to_i(16) }.pack('C*')
          end
          nil
        end

        def restore_metadata(bindata)
          # Any input errors are ignored
          data = MessagePack.unpack(bindata, symbolize_keys: true) rescue {}

          @unique_id = data[:id] || unique_id_from_path(@path) || @unique_id
          @records = data[:r] || 0
          @created_at = Time.at(data.fetch(:c, Time.now.to_i))
          @modified_at = Time.at(data.fetch(:m, Time.now.to_i))

          @metadata.timekey = data[:timekey]
          @metadata.tag = data[:tag]
          @metadata.variables = data.fetch(:variables, [])
        end

        def restore_metadata_partially(chunk)
          @unique_id = unique_id_from_path(chunk.path) || @unique_id
          @records = 0
          @created_at = chunk.birthtime rescue chunk.ctime # birthtime isn't supported on Windows
          @modified_at = chunk.mtime

          @metadata.timekey = nil
          @metadata.tag = nil
          @metadata.variables = []
        end

        def write_metadata(try_update: false)
          data = @metadata.to_h.merge({
              id: @unique_id,
              r: (try_update ? @records + @adding_records : @records),
              c: @created_at.to_i,
              m: (try_update ? Time.now.to_i : @modified_at.to_i)
          })
          @meta.seek(0, IO::SEEK_SET)
          @meta.truncate(0)
          @meta.write MessagePack.pack(data)
        end

        def enqueued!
          return unless @state == :staged

          new_chunk_path = generate_queued_chunk_path(@path)
          new_meta_path = new_chunk_path + '.meta'

          write_metadata # re-write metadata w/ finalized records

          @chunk.rename(new_chunk_path)
          @meta.rename(new_meta_path)

          @path = new_chunk_path
          @meta_path = new_meta_path
          @state = :queued
        end
      end
    end
  end
end
