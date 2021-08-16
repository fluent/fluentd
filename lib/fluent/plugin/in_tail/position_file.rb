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

require 'fluent/plugin/input'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    class PositionFile
      UNWATCHED_POSITION = 0xffffffffffffffff
      POSITION_FILE_ENTRY_REGEX = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.freeze

      def self.load(file, follow_inodes, existing_targets, logger:)
        pf = new(file, follow_inodes, logger: logger)
        pf.load(existing_targets)
        pf
      end

      def initialize(file, follow_inodes, logger: nil)
        @file = file
        @logger = logger
        @file_mutex = Mutex.new
        @map = {}
        @follow_inodes = follow_inodes
      end

      def [](target_info)
        if m = @map[@follow_inodes ? target_info.ino : target_info.path]
          return m
        end

        @file_mutex.synchronize {
          @file.seek(0, IO::SEEK_END)
          seek = @file.pos + target_info.path.bytesize + 1
          @file.write "#{target_info.path}\t0000000000000000\t0000000000000000\n"
          if @follow_inodes
            @map[target_info.ino] = FilePositionEntry.new(@file, @file_mutex, seek, 0, 0)
          else
            @map[target_info.path] = FilePositionEntry.new(@file, @file_mutex, seek, 0, 0)
          end
        }
      end

      def unwatch(target_info)
        if (entry = @map.delete(@follow_inodes ? target_info.ino : target_info.path))
          entry.update_pos(UNWATCHED_POSITION)
        end
      end

      def load(existing_targets = nil)
        compact(existing_targets)

        map = {}
        @file_mutex.synchronize do
          @file.pos = 0

          @file.each_line do |line|
            m = POSITION_FILE_ENTRY_REGEX.match(line)
            next if m.nil?

            path = m[1]
            pos = m[2].to_i(16)
            ino = m[3].to_i(16)
            seek = @file.pos - line.bytesize + path.bytesize + 1
            if @follow_inodes
              map[ino] = FilePositionEntry.new(@file, @file_mutex, seek, pos, ino)
            else
              map[path] = FilePositionEntry.new(@file, @file_mutex, seek, pos, ino)
            end
          end
        end

        @map = map
      end

      # This method is similer to #compact but it tries to get less lock to avoid a lock contention
      def try_compact
        last_modified = nil
        size = nil

        @file_mutex.synchronize do
          size = @file.size
          last_modified = @file.mtime
        end

        entries = fetch_compacted_entries

        @file_mutex.synchronize do
          if last_modified == @file.mtime && size == @file.size
            @file.pos = 0
            @file.truncate(0)
            @file.write(entries.values.map(&:to_entry_fmt).join)

            # entry contains path/ino key and value.
            entries.each do |key, val|
              if (m = @map[key])
                m.seek = val.seek
              end
            end
          else
            # skip
          end
        end
      end

      private

      def compact(existing_targets = nil)
        @file_mutex.synchronize do
          entries = fetch_compacted_entries(existing_targets).values.map(&:to_entry_fmt)

          @file.pos = 0
          @file.truncate(0)
          @file.write(entries.join)
        end
      end

      def fetch_compacted_entries(existing_targets = nil)
        entries = {}

        @file.pos = 0
        file_pos = 0
        @file.each_line do |line|
          m = POSITION_FILE_ENTRY_REGEX.match(line)
          if m.nil?
            @logger.warn "Unparsable line in pos_file: #{line}" if @logger
            next
          end

          path = m[1]
          pos = m[2].to_i(16)
          ino = m[3].to_i(16)
          if pos == UNWATCHED_POSITION
            @logger.debug "Remove unwatched line from pos_file: #{line}" if @logger
          else
            if entries.include?(path)
              @logger.warn("#{path} already exists. use latest one: deleted #{entries[path]}") if @logger
            end

            if @follow_inodes
              entries[ino] = Entry.new(path, pos, ino, file_pos + path.bytesize + 1)
            else
              entries[path] = Entry.new(path, pos, ino, file_pos + path.bytesize + 1)
            end
            file_pos += line.size
          end
        end

        entries = remove_deleted_files_entries(entries, existing_targets)
        entries
      end

      def remove_deleted_files_entries(existent_entries, existing_targets)
        if existing_targets
          existent_entries.select { |path_or_ino|
            existing_targets.key?(path_or_ino)
          }
        else
          existent_entries
        end
      end
    end

    Entry = Struct.new(:path, :pos, :ino, :seek) do
      POSITION_FILE_ENTRY_FORMAT = "%s\t%016x\t%016x\n".freeze

      def to_entry_fmt
        POSITION_FILE_ENTRY_FORMAT % [path, pos, ino]
      end
    end

    # pos               inode
    # ffffffffffffffff\tffffffffffffffff\n
    class FilePositionEntry
      POS_SIZE = 16
      INO_OFFSET = 17
      INO_SIZE = 16
      LN_OFFSET = 33
      SIZE = 34

      def initialize(file, file_mutex, seek, pos, inode)
        @file = file
        @file_mutex = file_mutex
        @seek = seek
        @pos = pos
        @inode = inode
      end

      attr_writer :seek

      def update(ino, pos)
        @file_mutex.synchronize {
          @file.pos = @seek
          @file.write "%016x\t%016x" % [pos, ino]
        }
        @pos = pos
        @inode = ino
      end

      def update_pos(pos)
        @file_mutex.synchronize {
          @file.pos = @seek
          @file.write "%016x" % pos
        }
        @pos = pos
      end

      def read_inode
        @inode
      end

      def read_pos
        @pos
      end
    end

    class MemoryPositionEntry
      def initialize
        @pos = 0
        @inode = 0
      end

      def update(ino, pos)
        @inode = ino
        @pos = pos
      end

      def update_pos(pos)
        @pos = pos
      end

      def read_pos
        @pos
      end

      def read_inode
        @inode
      end
    end

    TargetInfo = Struct.new(:path, :ino)
  end
end
