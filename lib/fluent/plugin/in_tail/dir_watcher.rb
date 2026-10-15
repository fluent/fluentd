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
    # Discover files matching the configured paths and refresh file watchers.
    # Periodic refreshes reconcile the discovered files with the active watchers.
    module DirWatcher
      def extended_glob_pattern(path)
        path.include?('*') || path.include?('?') || /\[.*\]/.match?(path)
      end

      # Curly braces is not supported with default path_delimiter
      # because the default delimiter of path is ",".
      # This should be collided for wildcard pattern for curly braces and
      # be handled as an error on #configure.
      def use_glob?(path)
        if @glob_policy == :always
          # For future extensions, we decided to use `always' term to handle
          # regular expressions as much as possible.
          # This is because not using `true' as a returning value
          # when choosing :always here.
          extended_glob_pattern(path) || /\{.*,.*\}/.match?(path)
        elsif @glob_policy == :extended
          extended_glob_pattern(path)
        elsif @glob_policy == :backward_compatible
          path.include?('*')
        end
      end

      def expand_paths_raw
        date = Fluent::EventTime.now
        paths = []
        @paths.each { |path|
          path = if @path_timezone
                   @path_formatters[path].call(date)
                 else
                   date.to_time.strftime(path)
                 end
          if use_glob?(path)
            paths += Dir.glob(path).select { |p|
              begin
                is_file = !File.directory?(p)
                if (File.readable?(p) || have_read_capability?) && is_file
                  if @limit_recently_modified && File.mtime(p) < (date.to_time - @limit_recently_modified)
                    false
                  else
                    true
                  end
                else
                  if is_file
                    unless @ignore_list.include?(p)
                      log.warn "#{p} unreadable. It is excluded and would be examined next time."
                      @ignore_list << p if @ignore_repeated_permission_error
                    end
                  end
                  false
                end
              rescue Errno::ENOENT, Errno::EACCES
                log.debug { "#{p} is missing after refresh file list" }
                false
              end
            }
          else
            # When file is not created yet, Dir.glob returns an empty array. So just add when path is static.
            paths << path
          end
        }
        excluded = @exclude_path.map { |path|
          path = if @path_timezone
                   @exclude_path_formatters[path].call(date)
                 else
                   date.to_time.strftime(path)
                 end
          use_glob?(path) ? Dir.glob(path) : path
        }.flatten.uniq
        paths - excluded
      end

      def expand_paths
        # filter out non existing files, so in case pattern is without '*' we don't do unnecessary work
        hash = {}
        expand_paths_raw.select { |path|
          File.exist?(path)
        }.each { |path|
          # Even we just checked for existence, there is a race condition here as
          # of which stat() might fail with ENOENT. See #3224.
          begin
            target_info = TargetInfo.new(path, Fluent::FileWrapper.stat(path).ino)
            if @follow_inodes
              hash[target_info.ino] = target_info
            else
              hash[target_info.path] = target_info
            end
          rescue Errno::ENOENT, Errno::EACCES  => e
            log.warn "expand_paths: stat() for #{path} failed with #{e.class.name}. Skip file."
          end
        }
        hash
      end

      def existence_path
        hash = {}
        @tails.each {|path, tw|
          if @follow_inodes
            hash[tw.ino] = TargetInfo.new(tw.path, tw.ino)
          else
            hash[tw.path] = TargetInfo.new(tw.path, tw.ino)
          end
        }
        hash
      end

      # in_tail with '*' path doesn't check rotation file equality at refresh phase.
      # So you should not use '*' path when your logs will be rotated by another tool.
      # It will cause log duplication after updated watch files.
      # In such case, you should separate log directory and specify two paths in path parameter.
      # e.g. path /path/to/dir/*,/path/to/rotated_logs/target_file
      def refresh_watchers
        @watcher_refresh_monitor.synchronize do
          return if before_shutdown? || @shutdown_start_time

          refresh_watchers_raw
        end
      end

      def refresh_watchers_raw
        target_paths_hash = expand_paths
        existence_paths_hash = existence_path

        log.debug {
          target_paths_str = target_paths_hash.collect { |key, target_info| target_info.path }.join(",")
          existence_paths_str = existence_paths_hash.collect { |key, target_info| target_info.path }.join(",")
          "tailing paths: target = #{target_paths_str} | existing = #{existence_paths_str}"
        }

        removed_hash = existence_paths_hash.reject {|key, value| target_paths_hash.key?(key)}
        added_hash = target_paths_hash.reject {|key, value| existence_paths_hash.key?(key)}

        if @follow_inodes
          # A TailWatcher waiting for `rotate_wait` still reads its inode, so do not start a second one for it.
          @tails_rotate_wait.each_value do |v|
            target = added_hash.delete(v[:ino])
            log.debug { "skip #{target.path} (inode: #{target.ino}) because a watcher waiting rotate_wait still reads it" } if target
          end
        end

        # If an existing TailWatcher already follows a target path with the different inode,
        # it means that the TailWatcher following the rotated file still exists. In this case,
        # `refresh_watcher` can't start the new TailWatcher for the new current file. So, we
        # should output a warning log in order to prevent silent collection stops.
        # (Such as https://github.com/fluent/fluentd/pull/4327)
        # (Usually, such a TailWatcher should be removed from `@tails` in `update_watcher`.)
        # (The similar warning may work for `@follow_inodes true` too. Just limiting the case
        # to suppress the impact to existing logics.)
        unless @follow_inodes
          target_paths_hash.each do |path, target|
            next unless @tails.key?(path)
            # We can't use `existence_paths_hash[path].ino` because it is from `TailWatcher.ino`,
            # which is very unstable parameter. (It can be `nil` or old).
            # So, we need to use `TailWatcher.pe.read_inode`.
            existing_watcher_inode = @tails[path].pe.read_inode
            if existing_watcher_inode != target.ino
              log.warn "Could not follow a file (inode: #{target.ino}) because an existing watcher for that filepath follows a different inode: #{existing_watcher_inode} (e.g. keeps watching a already rotated file). If you keep getting this message, please restart Fluentd.",
                filepath: target.path
            end
          end
        end

        stop_watchers(removed_hash, unwatched: !@follow_inodes) unless removed_hash.empty?
        unwatch_removed_inodes(target_paths_hash) if @follow_inodes
        start_watchers(added_hash) unless added_hash.empty?
        @metrics.tracked.set(@tails.size)
        @startup = false if @startup
      end

      # When using @follow_inodes, need this to unwatch the rotated old inode when it disappears.
      # After `update_watcher` detaches an old TailWatcher, the inode is lost from the `@tails`.
      # So that inode can't be contained in `removed_hash`, and can't be unwatched by `stop_watchers`.
      #
      # Inodes still read by a watcher waiting for `rotate_wait` are kept. Compaction only updates
      # the entries that remain in the position file, so such a watcher would otherwise write its
      # position into the line of another file.
      def unwatch_removed_inodes(target_paths_hash)
        return unless @pf
        draining_hash = @tails_rotate_wait.to_h { |tw, v| [v[:ino], TargetInfo.new(tw.path, v[:ino])] }
        @pf.unwatch_removed_targets(draining_hash.merge(target_paths_hash))
      end

    end
  end
end
