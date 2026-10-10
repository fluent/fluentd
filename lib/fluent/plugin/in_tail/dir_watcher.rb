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
    # Watch the directories of the configured paths to discover new files
    # sooner than the next refresh_interval check.
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

      # Build the list of the directories which have to be watched to notice the
      # creation of the files matching @paths.
      #
      # The directories are derived from the path patterns, not from the files
      # which exist now, because the directories of the files which are going to
      # be created are the ones to watch.
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
        select_dir_watchers(dirs_by_path)
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
        # A brace group may contain a separator and File.dirname would cut the
        # path at it, so the groups are expanded into the paths they stand for
        # before the directories are derived from them.
        patterns = glob ? expand_brace_patterns(expanded) : [expanded]
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

      # The directories to watch for one of the paths which a path pattern
      # stands for, that is for a path with no brace group left.
      def watch_dirs_of_path(path, glob, time_dependent_from)
        root, components = dir_path_parts(File.dirname(path))
        dirs = []
        # The existing directories which match the prefix walked so far.
        parents = [root]
        components.each_with_index do |component, i|
          matched = []
          # The parents which don't have this component in them.
          missing = []
          parents.each { |parent|
            found = existing_dirs(parent, component, glob) { |link_parent|
              # realpath follows a link, but replacing the link changes its
              # parent, not the old target directory.
              dirs << link_parent
            }
            if found.empty?
              # This directory is not created in that parent yet, and creating
              # it changes that parent. Nothing deeper of this branch can be
              # watched until it exists.
              missing << parent
            else
              matched.concat(found)
            end
          }
          if i >= time_dependent_from || (glob && WILDCARD_CHARS.match?(component))
            # Another directory may be created with a name which is not in the
            # pattern, inside any of the parents.
            dirs.concat(parents)
          else
            # Only the name of this component is meant, so watching the parents
            # which don't have it yet is enough.
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

      # The path of an existing directory as a pattern which matches only that
      # directory. The glob characters of a name are parts of the name, so
      # taking them as wildcards again at the next level would look into
      # another directory than the one the path has.
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
      def expand_brace_patterns(pattern)
        patterns = []
        expand_brace_patterns_into(pattern, patterns)
        if patterns.size >= MAX_DIR_PATTERNS
          log.warn "Too many paths expanded from the braces of `#{pattern}'. Watching their directories is given up, so new files of them may not be detected until refresh_interval (#{@refresh_interval}s) passes"
        end
        patterns
      end

      def expand_brace_patterns_into(pattern, patterns)
        return if patterns.size >= MAX_DIR_PATTERNS
        start, stop = brace_group(pattern)
        unless start
          patterns << pattern
          return
        end
        head = pattern[0...start]
        tail = pattern[(stop + 1)..]
        split_brace_alternatives(pattern[(start + 1)...stop]).each { |alternative|
          expand_brace_patterns_into(head + alternative + tail, patterns)
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

      # Choose the directories to watch from the ones derived from each path.
      #
      # They are taken by turn instead of path by path: taking the first
      # dir_watcher_limit of a list built path by path would leave a path
      # completely unwatched when an earlier path has a wildcard which matches a
      # large number of directories.
      def select_dir_watchers(dirs_by_path)
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
      # Recompute the directories after refreshing the files, within the same
      # refresh, so newly created directories are watched from then on.
      def update_dir_watchers(rotate: false)
        return unless @enable_dir_watcher
        # Do not add directory watchers once shutdown has begun.
        return if before_shutdown? || @shutdown_start_time

        dirs = expand_watch_dirs
        if dirs.size > @dir_watcher_limit
          log.warn "Too many directories to watch: #{dirs.size}. Watching #{@dir_watcher_limit} of them, rotating at each periodic refresh, so new files may not be detected until refresh_interval (#{@refresh_interval}s) passes"
        end

        @dir_watchers_mutex.synchronize do
          # Shutdown may have detached the watchers during path expansion.
          return if before_shutdown? || @shutdown_start_time

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
