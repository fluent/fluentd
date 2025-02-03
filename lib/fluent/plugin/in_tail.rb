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

require 'cool.io'

require 'fluent/plugin/input'
require 'fluent/config/error'
require 'fluent/event'
require 'fluent/plugin/buffer'
require 'fluent/plugin/parser_multiline'
require 'fluent/variable_store'
require 'fluent/capability'
require 'fluent/plugin/in_tail/position_file'
require 'fluent/plugin/in_tail/group_watch'
require 'fluent/file_wrapper'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    include GroupWatch

    Fluent::Plugin.register_input('tail', self)

    helpers :timer, :event_loop, :parser, :compat_parameters

    RESERVED_CHARS = ['/', '*', '%'].freeze
    MetricsInfo = Struct.new(:opened, :closed, :rotated, :throttled)

    class WatcherSetupError < StandardError
      def initialize(msg)
        @message = msg
      end

      def to_s
        @message
      end
    end

    def initialize
      super
      @paths = []
      @tails = {}
      @tails_rotate_wait = {}
      @pf_file = nil
      @pf = nil
      @ignore_list = []
      @shutdown_start_time = nil
      @metrics = nil
      @startup = true
    end

    desc 'The paths to read. Multiple paths can be specified, separated by comma.'
    config_param :path, :string
    desc 'path delimiter used for spliting path config'
    config_param :path_delimiter, :string, default: ','
    desc 'Choose using glob patterns. Adding capabilities to handle [] and ?, and {}.'
    config_param :glob_policy, :enum, list: [:backward_compatible, :extended, :always], default: :backward_compatible
    desc 'The tag of the event.'
    config_param :tag, :string
    desc 'The paths to exclude the files from watcher list.'
    config_param :exclude_path, :array, default: []
    desc 'Specify interval to keep reference to old file when rotate a file.'
    config_param :rotate_wait, :time, default: 5
    desc 'Fluentd will record the position it last read into this file.'
    config_param :pos_file, :string, default: nil
    desc 'The cleanup interval of pos file'
    config_param :pos_file_compaction_interval, :time, default: nil
    desc 'Start to read the logs from the head of file, not bottom.'
    config_param :read_from_head, :bool, default: false
    # When the program deletes log file and re-creates log file with same filename after passed refresh_interval,
    # in_tail may raise a pos_file related error. This is a known issue but there is no such program on production.
    # If we find such program / application, we will fix the problem.
    desc 'The interval of refreshing the list of watch file.'
    config_param :refresh_interval, :time, default: 60
    desc 'The number of reading lines at each IO.'
    config_param :read_lines_limit, :integer, default: 1000
    desc 'The number of reading bytes per second'
    config_param :read_bytes_limit_per_second, :size, default: -1
    desc 'The interval of flushing the buffer for multiline format'
    config_param :multiline_flush_interval, :time, default: nil
    desc 'Enable the option to emit unmatched lines.'
    config_param :emit_unmatched_lines, :bool, default: false
    desc 'Enable the additional watch timer.'
    config_param :enable_watch_timer, :bool, default: true
    desc 'Enable the stat watcher based on inotify.'
    config_param :enable_stat_watcher, :bool, default: true
    desc 'The encoding after conversion of the input.'
    config_param :encoding, :string, default: nil
    desc 'The encoding of the input.'
    config_param :from_encoding, :string, default: nil
    desc 'Add the log path being tailed to records. Specify the field name to be used.'
    config_param :path_key, :string, default: nil
    desc 'Open and close the file on every update instead of leaving it open until it gets rotated.'
    config_param :open_on_every_update, :bool, default: false
    desc 'Limit the watching files that the modification time is within the specified time range (when use \'*\' in path).'
    config_param :limit_recently_modified, :time, default: nil
    desc 'Enable the option to skip the refresh of watching list on startup.'
    config_param :skip_refresh_on_startup, :bool, default: false
    desc 'Ignore repeated permission error logs'
    config_param :ignore_repeated_permission_error, :bool, default: false
    desc 'Format path with the specified timezone'
    config_param :path_timezone, :string, default: nil
    desc 'Follow inodes instead of following file names. Guarantees more stable delivery and allows to use * in path pattern with rotating files'
    config_param :follow_inodes, :bool, default: false
    desc 'Maximum length of line. The longer line is just skipped.'
    config_param :max_line_size, :size, default: nil

    config_section :parse, required: false, multi: true, init: true, param_name: :parser_configs do
      config_argument :usage, :string, default: 'in_tail_parser'
    end

    attr_reader :paths

    def configure(conf)
      @variable_store = Fluent::VariableStore.fetch_or_build(:in_tail)
      compat_parameters_convert(conf, :parser)
      parser_config = conf.elements('parse').first
      unless parser_config
        raise Fluent::ConfigError, "<parse> section is required."
      end

      (1..Fluent::Plugin::MultilineParser::FORMAT_MAX_NUM).each do |n|
        parser_config["format#{n}"] = conf["format#{n}"] if conf["format#{n}"]
      end

      parser_config['unmatched_lines'] = conf['emit_unmatched_lines']

      super

      if !@enable_watch_timer && !@enable_stat_watcher
        raise Fluent::ConfigError, "either of enable_watch_timer or enable_stat_watcher must be true"
      end

      if @glob_policy == :always && @path_delimiter == ','
        raise Fluent::ConfigError, "cannot use glob_policy as always with the default path_delimitor: `,\""
      end

      if @glob_policy == :extended && /\{.*,.*\}/.match?(@path) && extended_glob_pattern(@path)
        raise Fluent::ConfigError, "cannot include curly braces with glob patterns in `#{@path}\". Use glob_policy always instead."
      end

      if RESERVED_CHARS.include?(@path_delimiter)
        rc = RESERVED_CHARS.join(', ')
        raise Fluent::ConfigError, "#{rc} are reserved words: #{@path_delimiter}"
      end

      @paths = @path.split(@path_delimiter).map(&:strip).uniq
      if @paths.empty?
        raise Fluent::ConfigError, "tail: 'path' parameter is required on tail input"
      end
      if @path_timezone
        Fluent::Timezone.validate!(@path_timezone)
        @path_formatters = @paths.map{|path| [path, Fluent::Timezone.formatter(@path_timezone, path)]}.to_h
        @exclude_path_formatters = @exclude_path.map{|path| [path, Fluent::Timezone.formatter(@path_timezone, path)]}.to_h
      end

      # TODO: Use plugin_root_dir and storage plugin to store positions if available
      if @pos_file
        if @variable_store.key?(@pos_file) && !called_in_test?
          plugin_id_using_this_path = @variable_store[@pos_file]
          raise Fluent::ConfigError, "Other 'in_tail' plugin already use same pos_file path: plugin_id = #{plugin_id_using_this_path}, pos_file path = #{@pos_file}"
        end
        @variable_store[@pos_file] = self.plugin_id
      else
        if @follow_inodes
          raise Fluent::ConfigError, "Can't follow inodes without pos_file configuration parameter"
        end
        $log.warn "'pos_file PATH' parameter is not set to a 'tail' source."
        $log.warn "this parameter is highly recommended to save the position to resume tailing."
      end

      configure_tag
      configure_encoding

      @multiline_mode = parser_config["@type"] =~ /multiline/
      @receive_handler = if @multiline_mode
                           method(:parse_multilines)
                         else
                           method(:parse_singleline)
                         end
      @file_perm = system_config.file_permission || Fluent::DEFAULT_FILE_PERMISSION
      @dir_perm = system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION
      # parser is already created by parser helper
      @parser = parser_create(usage: parser_config['usage'] || @parser_configs.first.usage)
      @capability = Fluent::Capability.new(:current_process)
      if @read_bytes_limit_per_second > 0
        if !@enable_watch_timer
          raise Fluent::ConfigError, "Need to enable watch timer when using log throttling feature"
        end
        min_bytes = TailWatcher::IOHandler::BYTES_TO_READ
        if @read_bytes_limit_per_second < min_bytes
          log.warn "Should specify greater equal than #{min_bytes}. Use #{min_bytes} for read_bytes_limit_per_second"
          @read_bytes_limit_per_second = min_bytes
        end
      end
      opened_file_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "files_opened_total", help_text: "Total number of opened files")
      closed_file_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "files_closed_total", help_text: "Total number of closed files")
      rotated_file_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "files_rotated_total", help_text: "Total number of rotated files")
      throttling_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "files_throttled_total", help_text: "Total number of times throttling occurs per file when throttling enabled")
      @metrics = MetricsInfo.new(opened_file_metrics, closed_file_metrics, rotated_file_metrics, throttling_metrics)
    end

    def configure_tag
      if @tag.index('*')
        @tag_prefix, @tag_suffix = @tag.split('*')
        @tag_prefix ||= ''
        @tag_suffix ||= ''
      else
        @tag_prefix = nil
        @tag_suffix = nil
      end
    end

    def configure_encoding
      unless @encoding
        if @from_encoding
          raise Fluent::ConfigError, "tail: 'from_encoding' parameter must be specified with 'encoding' parameter."
        end
      end

      @encoding = parse_encoding_param(@encoding) if @encoding
      @from_encoding = parse_encoding_param(@from_encoding) if @from_encoding
      if @encoding && (@encoding == @from_encoding)
        log.warn "'encoding' and 'from_encoding' are same encoding. No effect"
      end
    end

    def parse_encoding_param(encoding_name)
      begin
        Encoding.find(encoding_name) if encoding_name
      rescue ArgumentError => e
        raise Fluent::ConfigError, e.message
      end
    end

    def start
      super

      if @pos_file
        pos_file_dir = File.dirname(@pos_file)
        FileUtils.mkdir_p(pos_file_dir, mode: @dir_perm) unless Dir.exist?(pos_file_dir)
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT|File::BINARY, @file_perm)
        @pf_file.sync = true
        @pf = PositionFile.load(@pf_file, @follow_inodes, expand_paths, logger: log)

        if @pos_file_compaction_interval
          timer_execute(:in_tail_refresh_compact_pos_file, @pos_file_compaction_interval) do
            log.info('Clean up the pos file')
            @pf.try_compact
          end
        end
      end

      refresh_watchers unless @skip_refresh_on_startup
      timer_execute(:in_tail_refresh_watchers, @refresh_interval, &method(:refresh_watchers))
    end

    def stop
      if @variable_store
        @variable_store.delete(@pos_file)
      end

      super
    end

    def shutdown
      @shutdown_start_time = Fluent::Clock.now
      # during shutdown phase, don't close io. It should be done in close after all threads are stopped. See close.
      stop_watchers(existence_path, immediate: true, remove_watcher: false)
      @tails_rotate_wait.keys.each do |tw|
        detach_watcher(tw, @tails_rotate_wait[tw][:ino], false)
      end
      @pf_file.close if @pf_file

      super
    end

    def close
      super
      # close file handles after all threads stopped (in #close of thread plugin helper)
      # It may be because we need to wait IOHanlder.ready_to_shutdown()
      close_watcher_handles
    end

    def have_read_capability?
      @capability.have_capability?(:effective, :dac_read_search) ||
        @capability.have_capability?(:effective, :dac_override)
    end

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

    def expand_paths
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
              log.debug("#{p} is missing after refresh file list")
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
      # filter out non existing files, so in case pattern is without '*' we don't do unnecessary work
      hash = {}
      (paths - excluded).select { |path|
        FileTest.exist?(path)
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
          $log.warn "expand_paths: stat() for #{path} failed with #{e.class.name}. Skip file."
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
      target_paths_hash = expand_paths
      existence_paths_hash = existence_path

      log.debug {
        target_paths_str = target_paths_hash.collect { |key, target_info| target_info.path }.join(",")
        existence_paths_str = existence_paths_hash.collect { |key, target_info| target_info.path }.join(",")
        "tailing paths: target = #{target_paths_str} | existing = #{existence_paths_str}"
      }

      if !@follow_inodes
        need_unwatch_in_stop_watchers = true
      else
        # When using @follow_inodes, need this to unwatch the rotated old inode when it disappears.
        # After `update_watcher` detaches an old TailWatcher, the inode is lost from the `@tails`.
        # So that inode can't be contained in `removed_hash`, and can't be unwatched by `stop_watchers`.
        #
        # This logic may work for `@follow_inodes false` too.
        # Just limiting the case to suppress the impact to existing logics.
        @pf&.unwatch_removed_targets(target_paths_hash)
        need_unwatch_in_stop_watchers = false
      end

      removed_hash = existence_paths_hash.reject {|key, value| target_paths_hash.key?(key)}
      added_hash = target_paths_hash.reject {|key, value| existence_paths_hash.key?(key)}

      # If an exisiting TailWatcher already follows a target path with the different inode,
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

      stop_watchers(removed_hash, unwatched: need_unwatch_in_stop_watchers) unless removed_hash.empty?
      start_watchers(added_hash) unless added_hash.empty?
      @startup = false if @startup
    end

    def setup_watcher(target_info, pe)
      line_buffer_timer_flusher = @multiline_mode ? TailWatcher::LineBufferTimerFlusher.new(log, @multiline_flush_interval, &method(:flush_buffer)) : nil
      read_from_head = !@startup || @read_from_head
      tw = TailWatcher.new(target_info, pe, log, read_from_head, @follow_inodes, method(:update_watcher), line_buffer_timer_flusher, method(:io_handler), @metrics)

      if @enable_watch_timer
        tt = TimerTrigger.new(1, log) { tw.on_notify }
        tw.register_watcher(tt)
      end

      if @enable_stat_watcher
        tt = StatWatcher.new(target_info.path, log) { tw.on_notify }
        tw.register_watcher(tt)
      end

      tw.watchers.each do |watcher|
        event_loop_attach(watcher)
      end

      tw.group_watcher = add_path_to_group_watcher(target_info.path)

      tw
    rescue => e
      if tw
        tw.watchers.each do |watcher|
          event_loop_detach(watcher)
        end

        tw.detach(@shutdown_start_time)
        tw.close
      end
      raise e
    end

    def construct_watcher(target_info)
      path = target_info.path

      # The file might be rotated or removed after collecting paths, so check inode again here.
      begin
        target_info.ino = Fluent::FileWrapper.stat(path).ino
      rescue Errno::ENOENT, Errno::EACCES
        $log.warn "stat() for #{path} failed. Continuing without tailing it."
        return
      end

      pe = nil
      if @pf
        pe = @pf[target_info]
        pe.update(target_info.ino, 0) if @read_from_head && pe.read_inode.zero?
      end

      begin
        tw = setup_watcher(target_info, pe)
      rescue WatcherSetupError => e
        log.warn "Skip #{path} because unexpected setup error happens: #{e}"
        return
      end

      @tails[path] = tw
      tw.on_notify
    end

    def start_watchers(targets_info)
      targets_info.each_value {|target_info|
        construct_watcher(target_info)
        break if before_shutdown?
      }
    end

    def stop_watchers(targets_info, immediate: false, unwatched: false, remove_watcher: true)
      targets_info.each_value { |target_info|
        remove_path_from_group_watcher(target_info.path)

        if remove_watcher
          tw = @tails.delete(target_info.path)
        else
          tw = @tails[target_info.path]
        end
        if tw
          tw.unwatched = unwatched
          if immediate
            detach_watcher(tw, target_info.ino, false)
          else
            detach_watcher_after_rotate_wait(tw, target_info.ino)
          end
        end
      }
    end

    def close_watcher_handles
      @tails.keys.each do |path|
        tw = @tails.delete(path)
        if tw
          tw.close
        end
      end
      @tails_rotate_wait.keys.each do |tw|
        tw.close
      end
    end

    # refresh_watchers calls @tails.keys so we don't use stop_watcher -> start_watcher sequence for safety.
    def update_watcher(tail_watcher, pe, new_inode)
      # TODO we should use another callback for this.
      # To supress impact to existing logics, limit the case to `@follow_inodes`.
      # We may not need `@follow_inodes` condition.
      if @follow_inodes && new_inode.nil?
        # nil inode means the file disappeared, so we only need to stop it.
        @tails.delete(tail_watcher.path)
        # https://github.com/fluent/fluentd/pull/4237#issuecomment-1633358632
        # Because of this problem, log duplication can occur during `rotate_wait`.
        # Need to set `rotate_wait 0` for a workaround.
        # Duplication will occur if `refresh_watcher` is called during the `rotate_wait`.
        # In that case, `refresh_watcher` will add the new TailWatcher to tail the same target,
        # and it causes the log duplication.
        # (Other `detach_watcher_after_rotate_wait` may have the same problem.
        #  We need the mechanism not to add duplicated TailWathcer with detaching TailWatcher.)
        detach_watcher_after_rotate_wait(tail_watcher, pe.read_inode)
        return
      end

      path = tail_watcher.path

      log.info("detected rotation of #{path}; waiting #{@rotate_wait} seconds")

      if @pf
        pe_inode = pe.read_inode
        target_info_from_position_entry = TargetInfo.new(path, pe_inode)
        unless pe_inode == @pf[target_info_from_position_entry].read_inode
          log.warn "Skip update_watcher because watcher has been already updated by other inotify event",
                   path: path, inode: pe.read_inode, inode_in_pos_file: @pf[target_info_from_position_entry].read_inode
          return
        end
      end

      new_target_info = TargetInfo.new(path, new_inode)

      if @follow_inodes
        new_position_entry = @pf[new_target_info]
        # If `refresh_watcher` find the new file before, this will not be zero.
        # In this case, only we have to do is detaching the current tail_watcher.
        if new_position_entry.read_inode == 0
          @tails[path] = setup_watcher(new_target_info, new_position_entry)
          @tails[path].on_notify
        end
      else
        @tails[path] = setup_watcher(new_target_info, pe)
        @tails[path].on_notify
      end

      detach_watcher_after_rotate_wait(tail_watcher, pe.read_inode)
    end

    def detach_watcher(tw, ino, close_io = true)
      if @follow_inodes && tw.ino != ino
        log.warn("detach_watcher could be detaching an unexpected tail_watcher with a different ino.",
                  path: tw.path, actual_ino_in_tw: tw.ino, expect_ino_to_close: ino)
      end
      tw.watchers.each do |watcher|
        event_loop_detach(watcher)
      end
      tw.detach(@shutdown_start_time)

      tw.close if close_io

      if @pf && tw.unwatched && (@follow_inode || !@tails[tw.path])
        target_info = TargetInfo.new(tw.path, ino)
        @pf.unwatch(target_info)
      end
    end

    def throttling_is_enabled?(tw)
      return true if @read_bytes_limit_per_second > 0
      return true if tw.group_watcher && tw.group_watcher.limit >= 0
      false
    end

    def detach_watcher_after_rotate_wait(tw, ino)
      # Call event_loop_attach/event_loop_detach is high-cost for short-live object.
      # If this has a problem with large number of files, use @_event_loop directly instead of timer_execute.
      if @open_on_every_update
        # Detach now because it's already closed, waiting it doesn't make sense.
        detach_watcher(tw, ino)
      end

      return if @tails_rotate_wait[tw]

      if throttling_is_enabled?(tw)
        # When the throttling feature is enabled, it might not reach EOF yet.
        # Should ensure to read all contents before closing it, with keeping throttling.
        start_time_to_wait = Fluent::Clock.now
        timer = timer_execute(:in_tail_close_watcher, 1, repeat: true) do
          elapsed = Fluent::Clock.now - start_time_to_wait
          if tw.eof? && elapsed >= @rotate_wait
            timer.detach
            @tails_rotate_wait.delete(tw)
            detach_watcher(tw, ino)
          end
        end
        @tails_rotate_wait[tw] = { ino: ino, timer: timer }
      else
        # when the throttling feature isn't enabled, just wait @rotate_wait
        timer = timer_execute(:in_tail_close_watcher, @rotate_wait, repeat: false) do
          @tails_rotate_wait.delete(tw)
          detach_watcher(tw, ino)
        end
        @tails_rotate_wait[tw] = { ino: ino, timer: timer }
      end
    end

    def flush_buffer(tw, buf)
      buf.chomp!
      @parser.parse(buf) { |time, record|
        if time && record
          tag = if @tag_prefix || @tag_suffix
                  @tag_prefix + tw.tag + @tag_suffix
                else
                  @tag
                end
          record[@path_key] ||= tw.path unless @path_key.nil?
          router.emit(tag, time, record)
        else
          if @emit_unmatched_lines
            record = { 'unmatched_line' => buf }
            record[@path_key] ||= tail_watcher.path unless @path_key.nil?
            tag = if @tag_prefix || @tag_suffix
                    @tag_prefix + tw.tag + @tag_suffix
                  else
                    @tag
                  end
            router.emit(tag, Fluent::EventTime.now, record)
          end
          log.warn "got incomplete line at shutdown from #{tw.path}: #{buf.inspect}"
        end
      }
    end

    # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
    def receive_lines(lines, tail_watcher)
      es = @receive_handler.call(lines, tail_watcher)
      unless es.empty?
        tag = if @tag_prefix || @tag_suffix
                @tag_prefix + tail_watcher.tag + @tag_suffix
              else
                @tag
              end
        begin
          router.emit_stream(tag, es)
        rescue Fluent::Plugin::Buffer::BufferOverflowError
          return false
        rescue
          # ignore non BufferQueueLimitError errors because in_tail can't recover. Engine shows logs and backtraces.
          return true
        end
      end

      return true
    end

    def convert_line_to_event(line, es, tail_watcher)
      begin
        line.chomp!  # remove \n
        @parser.parse(line) { |time, record|
          if time && record
            record[@path_key] ||= tail_watcher.path unless @path_key.nil?
            es.add(time, record)
          else
            if @emit_unmatched_lines
              record = {'unmatched_line' => line}
              record[@path_key] ||= tail_watcher.path unless @path_key.nil?
              es.add(Fluent::EventTime.now, record)
            end
            log.warn "pattern not matched: #{line.inspect}"
          end
        }
      rescue => e
        log.warn 'invalid line found', file: tail_watcher.path, line: line, error: e.to_s
        log.debug_backtrace(e.backtrace)
      end
    end

    def parse_singleline(lines, tail_watcher)
      es = Fluent::MultiEventStream.new
      lines.each { |line|
        convert_line_to_event(line, es, tail_watcher)
      }
      es
    end

    # No need to check if line_buffer_timer_flusher is nil, since line_buffer_timer_flusher should exist
    def parse_multilines(lines, tail_watcher)
      lb = tail_watcher.line_buffer_timer_flusher.line_buffer
      es = Fluent::MultiEventStream.new
      if @parser.has_firstline?
        tail_watcher.line_buffer_timer_flusher.reset_timer
        lines.each { |line|
          if @parser.firstline?(line)
            if lb
              convert_line_to_event(lb, es, tail_watcher)
            end
            lb = line
          else
            if lb.nil?
              if @emit_unmatched_lines
                convert_line_to_event(line, es, tail_watcher)
              end
              log.warn "got incomplete line before first line from #{tail_watcher.path}: #{line.inspect}"
            else
              lb << line
            end
          end
        }
      else
        lb ||= ''
        lines.each do |line|
          lb << line
          @parser.parse(lb) { |time, record|
            if time && record
              convert_line_to_event(lb, es, tail_watcher)
              lb = ''
            end
          }
        end
      end
      tail_watcher.line_buffer_timer_flusher.line_buffer = lb
      es
    end

    def statistics
      stats = super

      stats = {
        'input' => stats["input"].merge({
          'opened_file_count' => @metrics.opened.get,
          'closed_file_count' => @metrics.closed.get,
          'rotated_file_count' => @metrics.rotated.get,
          'throttled_log_count' => @metrics.throttled.get,
        })
      }
      stats
    end

    private

    def io_handler(watcher, path)
      TailWatcher::IOHandler.new(
        watcher,
        path: path,
        log: log,
        read_lines_limit: @read_lines_limit,
        read_bytes_limit_per_second: @read_bytes_limit_per_second,
        open_on_every_update: @open_on_every_update,
        from_encoding: @from_encoding,
        encoding: @encoding,
        metrics: @metrics,
        max_line_size: @max_line_size,
        &method(:receive_lines)
      )
    end

    class StatWatcher < Coolio::StatWatcher
      def initialize(path, log, &callback)
        @callback = callback
        @log = log
        super(path)
      end

      def on_change(prev, cur)
        @callback.call
      rescue
        @log.error $!.to_s
        @log.error_backtrace
      end
    end

    class TimerTrigger < Coolio::TimerWatcher
      def initialize(interval, log, &callback)
        @log = log
        @callback = callback
        super(interval, true)
      end

      def on_timer
        @callback.call
      rescue => e
        @log.error e.to_s
        @log.error_backtrace
      end
    end

    class TailWatcher
      def initialize(target_info, pe, log, read_from_head, follow_inodes, update_watcher, line_buffer_timer_flusher, io_handler_build, metrics)
        @path = target_info.path
        @ino = target_info.ino
        @pe = pe || MemoryPositionEntry.new
        @read_from_head = read_from_head
        @follow_inodes = follow_inodes
        @update_watcher = update_watcher
        @log = log
        @rotate_handler = RotateHandler.new(log, &method(:on_rotate))
        @line_buffer_timer_flusher = line_buffer_timer_flusher
        @io_handler = nil
        @io_handler_build = io_handler_build
        @metrics = metrics
        @watchers = []
      end

      attr_reader :path, :ino
      attr_reader :pe
      attr_reader :line_buffer_timer_flusher
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
          @io_handler.ready_to_shutdown(shutdown_start_time)
          @io_handler.on_notify
        end
        @line_buffer_timer_flusher&.close(self)
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
        @line_buffer_timer_flusher.on_notify(self) if @line_buffer_timer_flusher
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
        @io_handler_build.call(self, @path)
      end

      def swap_state(pe)
        # Use MemoryPositionEntry for rotated file temporary
        mpe = MemoryPositionEntry.new
        mpe.update(pe.read_inode, pe.read_pos)
        @pe = mpe
        pe # This pe will be updated in on_rotate after TailWatcher is initialized
      end

      class FIFO
        def initialize(from_encoding, encoding, log, max_line_size=nil)
          @from_encoding = from_encoding
          @encoding = encoding
          @need_enc = from_encoding != encoding
          @buffer = ''.force_encoding(from_encoding)
          @eol = "\n".encode(from_encoding).freeze
          @max_line_size = max_line_size
          @skip_current_line = false
          @skipping_current_line_bytesize = 0
          @log = log
        end

        attr_reader :from_encoding, :encoding, :buffer, :max_line_size

        def <<(chunk)
          @buffer << chunk
        end

        def convert(s)
          if @need_enc
            s.encode!(@encoding, @from_encoding)
          else
            s
          end
        rescue
          s.encode!(@encoding, @from_encoding, :invalid => :replace, :undef => :replace)
        end

        def read_lines(lines)
          idx = @buffer.index(@eol)
          has_skipped_line = false

          until idx.nil?
            # Using freeze and slice is faster than slice!
            # See https://github.com/fluent/fluentd/pull/2527
            @buffer.freeze
            slice_position = idx + 1
            rbuf = @buffer.slice(0, slice_position)
            @buffer = @buffer.slice(slice_position, @buffer.size - slice_position)
            idx = @buffer.index(@eol)

            is_long_line = @max_line_size && (
              @skip_current_line || rbuf.bytesize > @max_line_size
            )

            if is_long_line
              @log.warn "received line length is longer than #{@max_line_size}"
              if @skip_current_line
                @log.debug("The continuing line is finished. Finally discarded data: ") { convert(rbuf).chomp }
              else
                @log.debug("skipped line: ") { convert(rbuf).chomp }
              end
              has_skipped_line = true
              @skip_current_line = false
              @skipping_current_line_bytesize = 0
              next
            end

            lines << convert(rbuf)
          end

          is_long_current_line = @max_line_size && (
            @skip_current_line || @buffer.bytesize > @max_line_size
          )

          if is_long_current_line
            @log.debug(
              "The continuing current line length is longer than #{@max_line_size}." +
              " The received data will be discarded until this line is finished." +
              " Discarded data: "
            ) { convert(@buffer).chomp }
            @skip_current_line = true
            @skipping_current_line_bytesize += @buffer.bytesize
            @buffer.clear
          end

          return has_skipped_line
        end

        def reading_bytesize
          return @skipping_current_line_bytesize if @skip_current_line
          @buffer.bytesize
        end
      end

      class IOHandler
        BYTES_TO_READ = 8192
        SHUTDOWN_TIMEOUT = 5

        attr_accessor :shutdown_timeout

        def initialize(watcher, path:, read_lines_limit:, read_bytes_limit_per_second:, max_line_size: nil, log:, open_on_every_update:, from_encoding: nil, encoding: nil, metrics:, &receive_lines)
          @watcher = watcher
          @path = path
          @read_lines_limit = read_lines_limit
          @read_bytes_limit_per_second = read_bytes_limit_per_second
          @receive_lines = receive_lines
          @open_on_every_update = open_on_every_update
          @fifo = FIFO.new(from_encoding || Encoding::ASCII_8BIT, encoding || Encoding::ASCII_8BIT, log, max_line_size)
          @lines = []
          @io = nil
          @notify_mutex = Mutex.new
          @log = log
          @start_reading_time = nil
          @number_bytes_read = 0
          @shutdown_start_time = nil
          @shutdown_timeout = SHUTDOWN_TIMEOUT
          @shutdown_mutex = Mutex.new
          @eof = false
          @metrics = metrics

          @log.info "following tail of #{@path}"
        end

        def group_watcher
          @watcher.group_watcher
        end

        def on_notify
          @notify_mutex.synchronize { handle_notify }
        end

        def ready_to_shutdown(shutdown_start_time = nil)
          @shutdown_mutex.synchronize {
            @shutdown_start_time =
              shutdown_start_time || Fluent::Clock.now
          }
        end

        def close
          if @io && !@io.closed?
            @io.close
            @io = nil
            @metrics.closed.inc
          end
        end

        def opened?
          !!@io
        end

        def eof?
          @eof
        end

        private

        def limit_bytes_per_second_reached?
          return false if @read_bytes_limit_per_second < 0 # not enabled by conf
          return false if @number_bytes_read < @read_bytes_limit_per_second

          @start_reading_time ||= Fluent::Clock.now
          time_spent_reading = Fluent::Clock.now - @start_reading_time
          @log.debug("time_spent_reading: #{time_spent_reading} #{ @watcher.path}")

          if time_spent_reading < 1
            true
          else
            @start_reading_time = nil
            @number_bytes_read = 0
            false
          end
        end

        def should_shutdown_now?
          # Ensure to read all remaining lines, but abort immediately if it
          # seems to take too long time.
          @shutdown_mutex.synchronize {
            return false if @shutdown_start_time.nil?
            return Fluent::Clock.now - @shutdown_start_time > @shutdown_timeout
          }
        end

        def handle_notify
          if limit_bytes_per_second_reached? || group_watcher&.limit_lines_reached?(@path)
            @metrics.throttled.inc
            return
          end

          with_io do |io|
            iobuf = ''.force_encoding('ASCII-8BIT')
            begin
              read_more = false
              has_skipped_line = false

              if !io.nil? && @lines.empty?
                begin
                  while true
                    @start_reading_time ||= Fluent::Clock.now
                    group_watcher&.update_reading_time(@path)

                    data = io.readpartial(BYTES_TO_READ, iobuf)
                    @eof = false
                    @number_bytes_read += data.bytesize
                    @fifo << data

                    n_lines_before_read = @lines.size
                    has_skipped_line = @fifo.read_lines(@lines) || has_skipped_line
                    group_watcher&.update_lines_read(@path, @lines.size - n_lines_before_read)

                    group_watcher_limit = group_watcher&.limit_lines_reached?(@path)
                    @log.debug "Reading Limit exceeded #{@path} #{group_watcher.number_lines_read}" if group_watcher_limit

                    if group_watcher_limit || limit_bytes_per_second_reached? || should_shutdown_now?
                      # Just get out from tailing loop.
                      @metrics.throttled.inc if group_watcher_limit || limit_bytes_per_second_reached?
                      read_more = false
                      break
                    end

                    if @lines.size >= @read_lines_limit
                      # not to use too much memory in case the file is very large
                      read_more = true
                      break
                    end
                  end
                rescue EOFError
                  @eof = true
                end
              end

              if @lines.empty?
                @watcher.pe.update_pos(io.pos - @fifo.reading_bytesize) if has_skipped_line
              else
                if @receive_lines.call(@lines, @watcher)
                  @watcher.pe.update_pos(io.pos - @fifo.reading_bytesize)
                  @lines.clear
                else
                  read_more = false
                end
              end
            end while read_more
          end
        end

        def open
          io = Fluent::FileWrapper.open(@path)
          io.seek(@watcher.pe.read_pos + @fifo.reading_bytesize)
          @metrics.opened.inc
          io
        rescue RangeError
          io.close if io
          raise WatcherSetupError, "seek error with #{@path}: file position = #{@watcher.pe.read_pos.to_s(16)}, reading bytesize = #{@fifo.reading_bytesize.to_s(16)}"
        rescue Errno::EACCES => e
          @log.warn "#{e}"
          nil
        rescue Errno::ENOENT
          nil
        end

        def with_io
          if @open_on_every_update
            io = open
            begin
              yield io
            ensure
              io.close unless io.nil?
            end
          else
            @io ||= open
            yield @io
            @eof = true if @io.nil?
          end
        rescue WatcherSetupError => e
          close
          @eof = true
          raise e
        rescue
          @log.error $!.to_s
          @log.error_backtrace
          close
          @eof = true
        end
      end

      class NullIOHandler
        def initialize
        end

        def io
        end

        def on_notify
        end

        def close
        end

        def opened?
          false
        end

        def eof?
          true
        end
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

      class LineBufferTimerFlusher
        attr_accessor :line_buffer

        def initialize(log, flush_interval, &flush_method)
          @log = log
          @flush_interval = flush_interval
          @flush_method = flush_method
          @start = nil
          @line_buffer = nil
        end

        def on_notify(tw)
          unless @start && @flush_method
            return
          end

          if Time.now - @start >= @flush_interval
            @flush_method.call(tw, @line_buffer) if @line_buffer
            @line_buffer = nil
            @start = nil
          end
        end

        def close(tw)
          return unless @line_buffer

          @flush_method.call(tw, @line_buffer)
          @line_buffer = nil
        end

        def reset_timer
          return unless @flush_interval

          @start = Time.now
        end
      end
    end
  end
end
