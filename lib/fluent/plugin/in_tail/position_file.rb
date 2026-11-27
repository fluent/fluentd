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
require 'fluent/file_wrapper'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    class PositionFile
      UNWATCHED_POSITION = 0xffffffffffffffff
      POSITION_FILE_ENTRY_REGEX = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)(?:\t([0-9a-fA-F]+))?/.freeze

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
        key = target_key(target_info)

        @file_mutex.synchronize do
          entry = @map[key]
          return entry if entry

          @file.seek(0, IO::SEEK_END)
          seek = @file.pos + target_info.path.bytesize + 1
          # Persist the key as soon as the row exists, including if compaction
          # runs before the watcher initializes its position.
          inode = @follow_inodes ? target_info.ino : 0
          device = @follow_inodes ? target_info.dev : nil
          @file.write Entry.new(target_info.path, 0, inode, device).to_entry_fmt
          @map[key] = FilePositionEntry.new(@file, @file_mutex, seek, 0, 0)
        end
      end

      def unwatch_removed_targets(existing_targets)
        @map.reject { |key, entry|
          existing_targets.key?(key)
        }.each_key { |key|
          unwatch_key(key)
        }
      end

      def unwatch(target_info)
        unwatch_key(target_key(target_info))
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
            dev = m[4]&.to_i(16)
            key = target_key(TargetInfo.new(path, ino, dev))
            map[key] = FilePositionEntry.new(@file, @file_mutex, seek, pos, ino)
          end
        end

        @map = map
      end

      # This method is similar to #compact but it tries to get less lock to avoid a lock contention
      def try_compact
        last_modified = nil
        size = nil

        @file_mutex.synchronize do
          size = @file.size
          last_modified = @file.mtime
        end

        entries = fetch_compacted_entries
        @logger&.debug "Compacted entries: ", entries.keys

        @file_mutex.synchronize do
          if last_modified == @file.mtime && size == @file.size
            write_entries(entries)
          else
            # skip
          end
        end
      end

      private

      def unwatch_key(key)
        if (entry = @map.delete(key))
          entry.update_pos(UNWATCHED_POSITION)
        end
      end

      def target_key(target_info)
        @follow_inodes ? target_info.identity : target_info.path
      end

      def compact(existing_targets = nil)
        @file_mutex.synchronize do
          entries = fetch_compacted_entries(existing_targets)
          @logger&.debug "Compacted entries: ", entries.keys

          write_entries(entries)
        end
      end

      def write_entries(entries)
        contents = String.new
        seeks = {}
        entries.each do |key, entry|
          if (position_entry = @map[key])
            seeks[position_entry] = contents.bytesize + entry.path.bytesize + 1
          end
          contents << entry.to_entry_fmt.b
        end

        @file.pos = 0
        @file.write(contents)
        @file.truncate(contents.bytesize)
        seeks.each { |position_entry, seek| position_entry.seek = seek }
      end

      def fetch_compacted_entries(existing_targets = nil)
        entries = {}
        exact_matches = {}
        if @follow_inodes && existing_targets
          targets_by_inode = existing_targets.values.group_by(&:ino)
        end

        @file.pos = 0
        @file.each_line do |line|
          m = POSITION_FILE_ENTRY_REGEX.match(line)
          if m.nil?
            @logger.warn "Unparsable line in pos_file: #{line}" if @logger
            next
          end

          path = m[1]
          pos = m[2].to_i(16)
          ino = m[3].to_i(16)
          dev = m[4]&.to_i(16)
          if pos == UNWATCHED_POSITION
            @logger.debug "Remove unwatched line from pos_file: #{line}" if @logger
            next
          end

          if targets_by_inode
            exact_target = dev && existing_targets[[dev, ino]]
            candidates = targets_by_inode.fetch(ino, [])
            # Device numbers can change across restarts. A unique inode
            # still identifies the saved position, as in older versions.
            target = exact_target || (candidates.first if candidates.one?)
            target ||= resolve_legacy_target(path, ino, candidates) if dev.nil?
            next unless target

            path = target.path unless exact_target
            dev = target.dev
          end

          key = target_key(TargetInfo.new(path, ino, dev))
          next if existing_targets && !existing_targets.key?(key)
          next if exact_matches[key] && !exact_target
          exact_matches[key] = true if exact_target

          if entries.key?(key)
            description = @follow_inodes ? "#{path} (device: #{dev}, inode: #{ino})" : path
            @logger&.warn("#{description} already exists. use latest one: deleted #{entries[key]}")
          end
          entries[key] = Entry.new(path, pos, ino, @follow_inodes ? dev : nil)
        end

        entries
      end

      def resolve_legacy_target(path, ino, candidates)
        return if candidates.empty?

        target = candidates.find { |candidate| candidate.path.b == path.b }
        return target if target

        # A live alias can identify the device when several targets share an inode.
        stat = Fluent::FileWrapper.stat(path)
        candidates.find { |candidate| candidate.dev == stat.dev } if stat.ino == ino
      rescue SystemCallError, ArgumentError
        nil
      end
    end

    Entry = Struct.new(:path, :pos, :ino, :dev) do
      def to_entry_fmt
        fields = "%s\t%016x\t%016x" % [path, pos, ino]
        fields << "\t%016x" % dev unless dev.nil?
        fields << "\n"
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

    TargetInfo = Struct.new(:path, :ino, :dev) do
      def identity
        [dev, ino]
      end
    end
  end
end
