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
require 'fluent/plugin/in_tail/stat_watcher'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    # Discover files matching the configured paths and refresh file watchers.
    # Periodic refreshes always run; enable_dir_watcher adds directory events
    # to discover new files sooner than the next refresh_interval check.
    #
    # Coolio::StatWatcher uses libev's ev_stat, with inotify on Linux where
    # available and stat polling as a fallback. Detection latency depends on
    # the platform and file system.
    #
    # ev_stat compares several stat fields, including second-resolution
    # timestamps. Changes can be missed when those fields remain unchanged;
    # delaying a refresh does not guarantee detection. Periodic refreshes
    # remain the fallback for missed changes.
    module DirWatcher
      # Directory changes may arrive close together. Collect requests during
      # this interval and refresh once, without extending the pending timer.
      DIR_WATCH_DEBOUNCE_INTERVAL = 1

      # Brace alternatives can multiply the paths whose directories must be
      # examined. Bound their expansion as well as the number of watchers.
      MAX_DIR_PATTERNS = 100

      # A component of a path which contains one of these characters may match a
      # directory which is not named by the component itself, that is a
      # directory which may be created later.
      WILDCARD_CHARS = /[*?\[{]/

      # A component of `**' stands for no directory at all or for directories at
      # any depth, so it is expanded recursively instead of as a wildcard of one
      # directory name.
      GLOB_STAR = '**'

      def initialize_dir_watchers
        @dir_watchers = {}
        @dir_watcher_offset = 0
        @dir_watcher_start_dir = nil
        @dir_watcher_limit_exceeded = false
        @truncated_dir_patterns = {}
        # The event loop schedules refreshes; shutdown detaches the pending
        # timer from another thread. Protect only this scheduling state.
        @refresh_request_mutex = Mutex.new
        @refresh_timer = nil
        @rotate_dir_watchers = false
        # Directory watcher updates and shutdown detachment share this lock.
        # Do not hold it while refreshing files: initial reads can take a long
        # time, and shutdown must be able to signal the IO handlers promptly.
        @dir_watchers_mutex = Mutex.new
      end

      def extended_glob_pattern(path)
        path.include?('*') || path.include?('?') || /\[.*\]/.match?(path)
      end

      # Brace alternatives conflict with the default ',' path delimiter;
      # configure rejects that combination with glob_policy :always.
      def use_glob?(path)
        if @glob_policy == :always
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

      # Rotated files matching path can be discovered as new files and reread.
      # follow_inodes avoids duplicate watchers for the same inode after rename,
      # but not for a copytruncate archive, which has a different inode. Exclude
      # archives from path to avoid rediscovering them. Directory-triggered
      # refreshes can discover them sooner than periodic refreshes.
      def refresh_watchers
        @watcher_refresh_monitor.synchronize do
          return if before_shutdown? || @shutdown_start_time

          rotate = take_dir_watcher_rotation_request
          # Arm directory watchers before scanning so changes during initial
          # reads cannot become part of a new watcher's baseline.
          update_dir_watchers(rotate: rotate)
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
      # Keep inodes still read by watchers waiting for rotate_wait or worker completion.
      # Compaction only updates entries remaining in the position file, so removing one
      # early would let its watcher write into another file's line.
      def unwatch_removed_inodes(target_paths_hash)
        return unless @pf
        draining_hash = @tails_rotate_wait.to_h { |tw, v| [v[:ino], TargetInfo.new(tw.path, v[:ino])] }
        @worker_detached_watchers&.each do |tw|
          draining_hash[tw.ino] = TargetInfo.new(tw.path, tw.ino)
        end
        @worker_deferred_unwatch&.each_value do |target|
          draining_hash[target.ino] = target
        end
        @pf.unwatch_removed_targets(draining_hash.merge(target_paths_hash))
      end

      # Without directory events, keep the ordinary periodic refresh path.
      def refresh_watchers_on_timer
        if @enable_dir_watcher
          request_refresh_watchers(:refresh_interval)
        else
          refresh_watchers
        end
      end

      # Consume the rotation request before reading files, so a request arriving
      # during that read remains pending for the next refresh.
      def take_dir_watcher_rotation_request
        return false unless @enable_dir_watcher

        @refresh_request_mutex.synchronize do
          requested = @rotate_dir_watchers
          @rotate_dir_watchers = false
          requested
        end
      end

      # Merge directory changes and periodic requests into a pending refresh.
      # With delay, schedule a one-shot timer without extending an existing one.
      def request_refresh_watchers(trigger, delay: nil)
        log.debug { "in_tail: #{trigger} requested to refresh watchers" }
        refresh_now = false
        @refresh_request_mutex.synchronize do
          # Detaching a watcher cannot cancel an already dispatched callback.
          return if before_shutdown? || @shutdown_start_time

          # Preserve periodic rotation when it joins a directory-triggered refresh.
          @rotate_dir_watchers = true if trigger == :refresh_interval
          if @refresh_timer
            log.debug { "in_tail: merge the request by #{trigger} into the scheduled refresh" }
          elsif delay
            @refresh_timer = TimerTrigger.new(delay, log, repeat: false) { trigger_refresh_watchers }
            event_loop_attach(@refresh_timer)
          else
            # Do not hold the scheduling lock while reading files.
            refresh_now = true
          end
        end
        trigger_refresh_watchers if refresh_now
      end

      def trigger_refresh_watchers
        # Detach the pending timer before refreshing, including immediate refreshes.
        detach_refresh_timer
        refresh_watchers
      end

      def detach_refresh_timer
        timer = nil
        @refresh_request_mutex.synchronize do
          timer = @refresh_timer
          @refresh_timer = nil
        end
        event_loop_detach(timer) if timer
      end

      # Derive watch directories from path patterns, not existing files, so
      # files created later can also be discovered.
      def expand_watch_dirs
        date = Fluent::EventTime.now
        dirs_by_path = @paths.map { |path|
          expanded = if @path_timezone
                       @path_formatters[path].call(date)
                     else
                       date.to_time.strftime(path)
                     end
          # Whether the path is expanded as a glob is decided on the whole path,
          # exactly as #expand_paths_raw does. Deciding it on the directory part
          # only would take `/base/[a]/*.log' as a literal directory name while
          # #expand_paths_raw matches it as a wildcard.
          expand_watch_dirs_of_pattern(path, expanded, use_glob?(expanded))
        }
        interleave_watch_dirs(dirs_by_path)
      end

      # The directories to watch for one path pattern. `path' is the pattern as
      # it is configured and `expanded' is the pattern with the strftime
      # directives of the current time replaced.
      #
      # Walk existing directories one component at a time, separately for each
      # parent. Watch the final directories where files can be created, and
      # parents where another matching directory can appear. For wildcard or
      # time-dependent components, keep all parents; for literal components,
      # keep only parents where that component is missing.
      #
      # For example, `/base/*/logs/*.log' with `/base/one/logs' and
      # `/base/two' existing gives `/base', to notice a new directory matching
      # `*', `/base/one/logs', the directory of the files, and `/base/two', to
      # notice the creation of its own `logs'.
      def expand_watch_dirs_of_pattern(path, expanded, glob)
        # Expand braces before File.dirname; see #expand_brace_patterns.
        patterns = glob ? expand_brace_patterns(expanded, warning_key: path) : [expanded]
        configured_patterns = []
        if glob
          expand_brace_patterns_into(path, configured_patterns)
        else
          configured_patterns << path
        end
        patterns.each_with_index.flat_map { |pattern, index|
          # Brace alternatives can have different depths. Compare each
          # alternative with its own unformatted path, not the unsplit group.
          time_dependent_from = time_dependent_component_index(configured_patterns[index] || path, pattern)
          watch_dirs_of_path(pattern, glob, time_dependent_from)
        }
      end

      # Walk one path after brace expansion, using the parent-selection rules above.
      def watch_dirs_of_path(path, glob, time_dependent_from)
        root, components = dir_path_parts(File.dirname(path))
        dirs = []
        # The existing directories which match the prefix walked so far.
        parents = [root]
        components.each_with_index do |component, i|
          matched = []
          missing = []
          parents.each { |parent|
            found = existing_dirs(parent, component, glob) { |link_parent|
              # realpath follows a link, but replacing the link changes its
              # parent, not the old target directory.
              dirs << link_parent
            }
            if found.empty?
              missing << parent
            else
              matched.concat(found)
            end
          }
          if i >= time_dependent_from || (glob && WILDCARD_CHARS.match?(component))
            dirs.concat(parents)
          else
            dirs.concat(missing)
          end
          parents = matched
          break if parents.empty?
        end
        # Keep unresolved prefixes while walking: resolving a symbolic link
        # before a later '..' can change Windows path lookup semantics.
        dirs.concat(parents).filter_map { |dir| real_path(dir) }
      end

      # The existing directories matching one component of a path pattern inside
      # one of the directories matched by the components above it. The component
      # is expanded as a glob only when the path it comes from is expanded by
      # Dir.glob in #expand_paths_raw: otherwise its glob characters are parts
      # of the directory name. Even a component without wildcard characters
      # must go through Dir.glob when glob is enabled, because backslashes
      # escape characters there. Yield the parent of each matched symbolic
      # link so that replacing the link can also be detected.
      def existing_dirs(parent, component, glob)
        dirs = if !glob
                 dir = File.join(parent, component)
                 File.directory?(dir) ? [dir] : []
               elsif component == GLOB_STAR
                 # `**' stands for no directory at all or for directories at any
                 # depth. Dir.glob takes it recursively only when it is followed
                 # by a separator: `dir/**' gives the same as `dir/*' there.
                 Dir.glob(File.join(literal_glob_pattern(parent), GLOB_STAR) + File::SEPARATOR)
               else
                 Dir.glob(File.join(literal_glob_pattern(parent), component))
               end
        dirs.filter_map { |dir|
          next unless File.directory?(dir)
          if block_given? && File.symlink?(dir.chomp(File::SEPARATOR))
            yield File.dirname(dir.chomp(File::SEPARATOR))
          end
          dir
        }
      end

      # Escape glob characters in matched directory names so later components
      # do not reinterpret those names as patterns.
      def literal_glob_pattern(path)
        path.gsub(/[*?{}\[\]\\]/) { |char| "\\#{char}" }
      end

      # Resolve symbolic links and `..' through the file system and return a
      # canonical path for deduplication, or nil if resolution fails. Callers
      # check whether the path is a directory.
      def real_path(dir)
        # Win32 normalizes '..' before following symbolic links. These are
        # already matched directory names, not unexpanded glob patterns.
        dir = File.expand_path(dir) if Fluent.windows?
        File.realpath(dir)
      rescue SystemCallError
        # A matched path may disappear or become inaccessible before resolution.
        nil
      end

      # Split a directory path into the root directory it starts from and the
      # components below it, so that the path can be walked from its topmost
      # directory: `/base/logs' gives `/' and [`base', `logs'], and `C:/base'
      # gives `C:/' and [`base'].
      #
      # Resolve relative paths from the working directory, like #expand_paths_raw.
      # Preserve `..' until lookup so the platform decides its meaning. On
      # Unix it refers to a symbolic link target's parent, unlike expand_path.
      def dir_path_parts(dir_path)
        components = path_components(dir_path)
        if File::ALT_SEPARATOR && components[0, 2] == ['', ''] && components.size >= 4
          # A UNC share is the root; walking from '/' would use the current
          # drive and lose the server/share portion of the path.
          ["//#{components[2]}/#{components[3]}/", components.drop(4)]
        elsif components.empty?
          # The path is a root directory itself, e.g. the directory of
          # `/review.log', and there is no component to walk below it.
          [File.expand_path(dir_path), []]
        elsif components.first == ''
          components.shift
          [File.expand_path(File::SEPARATOR), components]
        elsif File::ALT_SEPARATOR && components.first.match?(/\A[a-zA-Z]:\z/)
          [components.shift + File::SEPARATOR, components]
        else
          dir_path_parts(File.join(Dir.pwd, dir_path))
        end
      end

      # Normalize Windows file system separators before splitting. This is not
      # glob parsing: Dir.glob patterns use '/' and treat backslashes as escapes.
      def path_components(path)
        path = path.tr(File::ALT_SEPARATOR, File::SEPARATOR) if File::ALT_SEPARATOR
        path.split(File::SEPARATOR)
      end

      # The index of the first component of the directory part of a path whose
      # value depends on the current time, or the number of the components when
      # none of them does. Such a component may name another directory later,
      # e.g. the directory of the next month of a `%Y%m' path, and that
      # directory may be created inside one of the directories above it.
      # If it already exists, the time boundary itself generates no directory
      # event; periodic refreshes select the newly current directory.
      def time_dependent_component_index(path, expanded)
        configured = dir_path_parts(File.dirname(path))[1]
        current = dir_path_parts(File.dirname(expanded))[1]
        index = 0
        while index < configured.size && index < current.size && configured[index] == current[index]
          index += 1
        end
        index
      end

      # Expand the brace groups of a path pattern into the paths they stand for,
      # the same way Dir.glob does: `{a,b}' stands for `a' and `b', the groups
      # can be nested, and a group may contain a separator. The directories of
      # `/base/{a/x.log,b/x.log}' are `/base/a' and `/base/b', which
      # File.dirname can't give because it cuts the path inside the group.
      def expand_brace_patterns(pattern, warning_key: pattern)
        patterns = []
        # One extra result distinguishes truncation from an exact fit.
        expand_brace_patterns_into(pattern, patterns, limit: MAX_DIR_PATTERNS + 1)
        if patterns.size > MAX_DIR_PATTERNS
          patterns.pop
          if @truncated_dir_patterns[warning_key]
            log.debug {
              "Directory watch pattern expansion remains limited to #{MAX_DIR_PATTERNS}" +
              " paths for `#{pattern}'"
            }
          else
            log.warn(
              "Too many paths expanded from the braces of `#{pattern}'." +
              " Only the first #{MAX_DIR_PATTERNS} paths are considered for directory watching;" +
              " remaining paths rely on refresh_interval (#{@refresh_interval}s)"
            )
          end
          @truncated_dir_patterns[warning_key] = true
        else
          @truncated_dir_patterns.delete(warning_key)
        end
        patterns
      end

      def expand_brace_patterns_into(pattern, patterns, limit: MAX_DIR_PATTERNS)
        return if patterns.size >= limit
        start, stop = brace_group(pattern)
        unless start
          patterns << pattern
          return
        end
        head = pattern[0...start]
        tail = pattern[(stop + 1)..]
        split_brace_alternatives(pattern[(start + 1)...stop]).each { |alternative|
          expand_brace_patterns_into(head + alternative + tail, patterns, limit: limit)
        }
      end

      # Return the positions of the first unescaped '{' and its closing '}',
      # or nil if there is no complete group. In that case the caller leaves
      # the pattern unchanged for subsequent path/glob processing.
      def brace_group(pattern)
        start = nil
        depth = 0
        index = 0
        while index < pattern.length
          char = pattern[index]
          if char == '\\'
            index += 2
            next
          end
          if char == '{'
            start ||= index
            depth += 1
          elsif char == '}' && start
            depth -= 1
            return [start, index] if depth == 0
          end
          index += 1
        end
        nil
      end

      # Split the content of a brace group at the commas which are not inside
      # another group.
      def split_brace_alternatives(group)
        alternatives = []
        alternative = +''
        depth = 0
        index = 0
        while index < group.length
          char = group[index]
          if char == '\\' && index + 1 < group.length
            alternative << char << group[index + 1]
            index += 2
            next
          end
          case char
          when '{'
            depth += 1
          when '}'
            depth -= 1
          when ','
            if depth == 0
              alternatives << alternative
              alternative = +''
              index += 1
              next
            end
          end
          alternative << char
          index += 1
        end
        alternatives << alternative
      end

      # Interleave candidates so a large glob cannot monopolize the watcher
      # limit and leave later paths entirely unwatched.
      def interleave_watch_dirs(dirs_by_path)
        # Deduplicate across paths only after interleaving. Otherwise a static
        # path already included late in a large glob loses its own turn.
        groups = dirs_by_path.map(&:uniq)

        dirs = []
        until groups.all?(&:empty?)
          groups.each { |group|
            dirs << group.shift unless group.empty?
          }
        end
        dirs.uniq
      end

      # Take a circular window of the interleaved candidates. Advance only on
      # periodic refreshes, not on directory events. Keep the window's starting
      # directory when candidates change, or fall back to its previous index
      # if that directory disappeared. Called under @dir_watchers_mutex.
      def limit_dir_watchers(dirs, rotate:)
        if dirs.size <= @dir_watcher_limit
          @dir_watcher_offset = 0
          @dir_watcher_start_dir = dirs.first
          return dirs
        end

        offset = dirs.index(@dir_watcher_start_dir) || @dir_watcher_offset
        offset += @dir_watcher_limit if rotate
        @dir_watcher_offset = offset % dirs.size
        @dir_watcher_start_dir = dirs[@dir_watcher_offset]
        dirs.rotate(@dir_watcher_offset).first(@dir_watcher_limit)
      end

      # Attach the stat watchers of the directories which are needed now and
      # detach the ones which are not needed anymore.
      #
      # Recompute and watch directories before discovering files, so changes
      # during discovery or synchronous initial reads can request another refresh.
      def update_dir_watchers(rotate: false)
        return unless @enable_dir_watcher
        # Do not add directory watchers once shutdown has begun.
        return if before_shutdown? || @shutdown_start_time

        dirs = expand_watch_dirs
        @dir_watchers_mutex.synchronize do
          # Shutdown may have detached the watchers during path expansion.
          return if before_shutdown? || @shutdown_start_time

          over_limit = dirs.size > @dir_watcher_limit
          if over_limit
            if @dir_watcher_limit_exceeded
              log.debug {
                "Directory watcher limit remains exceeded: #{dirs.size} candidates," +
                " watching #{@dir_watcher_limit}"
              }
            else
              log.warn(
                "Too many directories to watch: #{dirs.size}." +
                " Watching #{@dir_watcher_limit} of them, rotating at each periodic refresh," +
                " so new files may not be detected until refresh_interval (#{@refresh_interval}s) passes"
              )
            end
          end
          @dir_watcher_limit_exceeded = over_limit

          dirs = limit_dir_watchers(dirs, rotate: rotate)
          # Release the old window before attaching the new one so rotation
          # does not temporarily exceed the configured watcher limit.
          (@dir_watchers.keys - dirs).each do |dir|
            detach_dir_watcher(dir)
          end
          dirs.each do |dir|
            attach_dir_watcher(dir) unless @dir_watchers.key?(dir)
          end
        end
      end

      def attach_dir_watcher(dir)
        watcher = StatWatcher.new(dir, log) { on_dir_changed(dir) }
        @dir_watchers[dir] = watcher
        event_loop_attach(watcher)
        log.debug { "in_tail: watching directory #{dir} to detect new files" }
      rescue SystemCallError => e
        # Failing to watch a directory only means that the new files in it are
        # detected by refresh_interval, like before.
        @dir_watchers.delete(dir)
        log.debug { "in_tail: failed to watch directory #{dir}: #{e}" }
      end

      def detach_dir_watcher(dir)
        watcher = @dir_watchers.delete(dir)
        return unless watcher
        event_loop_detach(watcher)
        log.debug { "in_tail: stop watching directory #{dir}" }
      end

      def detach_dir_watchers
        @dir_watchers_mutex.synchronize do
          @dir_watchers.keys.each do |dir|
            detach_dir_watcher(dir)
          end
        end
      end

      def on_dir_changed(dir)
        # The directory may have been removed. #update_dir_watchers will drop it
        # from the watched list at the next refresh.
        request_refresh_watchers("change of #{dir}", delay: DIR_WATCH_DEBOUNCE_INTERVAL)
      end
    end
  end
end
