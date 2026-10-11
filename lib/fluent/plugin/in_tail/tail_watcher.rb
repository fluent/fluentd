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
require 'fluent/plugin/in_tail/position_file'
require 'fluent/plugin/in_tail/compatibility'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    class TailWatcher
      def initialize(target_info, pe, log, read_from_head, follow_inodes, update_watcher, file_feed, io_handler_build, metrics)
        @path = target_info.path
        @ino = target_info.ino
        @pe = pe || MemoryPositionEntry.new
        @read_from_head = read_from_head
        @follow_inodes = follow_inodes
        @update_watcher = update_watcher
        @log = log
        @rotate_handler = RotateHandler.new(log, &method(:on_rotate))
        @file_feed = file_feed
        @io_handler = nil
        @io_handler_build = io_handler_build
        @metrics = metrics
        @watchers = []
      end

      attr_reader :path, :ino
      attr_reader :pe
      attr_reader :file_feed
      attr_accessor :unwatched  # This is used for removing position entry from PositionFile
      attr_reader :watchers
      attr_accessor :group_watcher

      def tag
        @parsed_tag ||= @path.tr('/', '.').squeeze('.').gsub(/^\./, '')
      end

      def register_watcher(watcher)
        @watchers << watcher
      end

      def detach(shutdown_start_time = nil)
        if @io_handler
          ready_to_shutdown(shutdown_start_time)
          @io_handler.on_notify
        end
        @file_feed&.close(self)
      end

      def ready_to_shutdown(shutdown_start_time)
        return if shutdown_start_time && @shutdown_start_time == shutdown_start_time
        @shutdown_start_time = shutdown_start_time
        @io_handler.ready_to_shutdown(shutdown_start_time) if @io_handler
      end

      def close
        if @io_handler
          @io_handler.close
          @io_handler = nil
        end
      end

      def eof?
        @io_handler.nil? || @io_handler.eof?
      end

      def on_notify
        begin
          stat = Fluent::FileWrapper.stat(@path)
        rescue Errno::ENOENT, Errno::EACCES
          # moved or deleted
          stat = nil
        end

        @rotate_handler.on_notify(stat) if @rotate_handler
        read_more
      end

      def read_more
        @file_feed&.on_notify(self)
        @io_handler.on_notify if @io_handler
      end

      def on_rotate(stat)
        if @io_handler.nil?
          if stat
            # first time
            fsize = stat.size
            inode = stat.ino

            last_inode = @pe.read_inode
            if inode == last_inode
              # rotated file has the same inode number with the last file.
              # assuming following situation:
              #   a) file was once renamed and backed, or
              #   b) symlink or hardlink to the same file is recreated
              # in either case of a and b, seek to the saved position
              #   c) file was once renamed, truncated and then backed
              # in this case, consider it truncated
              @pe.update(inode, 0) if fsize < @pe.read_pos
            elsif last_inode != 0
              # this is FilePositionEntry and fluentd once started.
              # read data from the head of the rotated file.
              # logs never duplicate because this file is a rotated new file.
              @pe.update(inode, 0)
            else
              # this is MemoryPositionEntry or this is the first time fluentd started.
              # seek to the end of the any files.
              # logs may duplicate without this seek because it's not sure the file is
              # existent file or rotated new file.
              pos = @read_from_head ? 0 : fsize
              @pe.update(inode, pos)
            end
            @io_handler = io_handler
          else
            @io_handler = NullIOHandler.new
          end
        else
          watcher_needs_update = false

          if stat
            inode = stat.ino
            if inode == @pe.read_inode # truncated
              @pe.update_pos(0)
              @io_handler.close
            elsif !@io_handler.opened? # There is no previous file. Reuse TailWatcher
              @pe.update(inode, 0)
            else # file is rotated and new file found
              watcher_needs_update = true
              # Handle the old log file before renewing TailWatcher [fluentd#1055]
              @io_handler.on_notify
            end
          else # file is rotated and new file not found
            # Clear RotateHandler to avoid duplicated file watch in same path.
            @rotate_handler = nil
            watcher_needs_update = true
          end

          if watcher_needs_update
            if @follow_inodes
              # If stat is nil (file not present), NEED to stop and discard this watcher.
              #   When the file is disappeared but is resurrected soon, then `#refresh_watcher`
              #   can't recognize this TailWatcher needs to be stopped.
              #   This can happens when the file is rotated.
              #   If a notify comes before the new file for the path is created during rotation,
              #   then it appears as if the file was resurrected once it disappeared.
              # Don't want to swap state because we need latest read offset in pos file even after rotate_wait
              @update_watcher.call(self, @pe, stat&.ino)
            else
              # Permit to handle if stat is nil (file not present).
              # If a file is mv-ed and a new file is created during
              # calling `#refresh_watchers`s, and `#refresh_watchers` won't run `#start_watchers`
              # and `#stop_watchers()` for the path because `target_paths_hash`
              # always contains the path.
              @update_watcher.call(self, swap_state(@pe), stat&.ino)
            end
          else
            @log.info "detected rotation of #{@path}"
            @io_handler = io_handler
          end
          @metrics.rotated.inc
        end
      end

      def io_handler
        handler = @io_handler_build.call(self, @path)
        handler.ready_to_shutdown(@shutdown_start_time) if @shutdown_start_time
        handler
      end

      def swap_state(pe)
        # Use MemoryPositionEntry for rotated file temporary
        mpe = MemoryPositionEntry.new
        mpe.update(pe.read_inode, pe.read_pos)
        @pe = mpe
        pe # This pe will be updated in on_rotate after TailWatcher is initialized
      end

      class RotateHandler
        def initialize(log, &on_rotate)
          @log = log
          @inode = nil
          @fsize = -1  # first
          @on_rotate = on_rotate
        end

        def on_notify(stat)
          if stat.nil?
            inode = nil
            fsize = 0
          else
            inode = stat.ino
            fsize = stat.size
          end

          if @inode != inode || fsize < @fsize
            @on_rotate.call(stat)
          end
          @inode = inode
          @fsize = fsize
        rescue
          @log.error $!.to_s
          @log.error_backtrace
        end
      end
    end
  end
end
