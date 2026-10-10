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
require 'fluent/plugin/in_tail/stat_watcher'
require 'fluent/plugin/in_tail/io_handler'
require 'fluent/plugin/in_tail/tail_watcher'
require 'fluent/plugin/in_tail/line_feeder'
require 'fluent/plugin/in_tail/compatibility'
require 'fluent/plugin/in_tail/worker_pool'
require 'fluent/file_wrapper'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    include GroupWatch
    include Compatibility

    Fluent::Plugin.register_input('tail', self)

    helpers :timer, :event_loop, :parser, :compat_parameters

    RESERVED_CHARS = ['/', '*', '%'].freeze
    MetricsInfo = Struct.new(:opened, :closed, :rotated, :throttled, :tracked)

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
      @capability = Fluent::Capability.new(:current_process)
    end

    desc 'The paths to read. Multiple paths can be specified, separated by comma.'
    config_param :path, :string
    desc 'path delimiter used for splitting path config'
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
    desc 'The number of threads used to parse tailed files. Set to 1 to keep parsing synchronous; 2 or more enables worker parsing. The default is 1.'
    config_param :num_threads, :integer, default: 1
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
    desc 'The encoding of the input.'
    config_param :encoding, :string, default: nil
    desc "The original encoding of the input. If set, in_tail tries to encode string from this to 'encoding'. Must be set with 'encoding'. "
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
      raise Fluent::ConfigError, "'num_threads' must be greater than 0" unless @num_threads.positive?

      if @glob_policy == :always && @path_delimiter == ','
        raise Fluent::ConfigError, "cannot use glob_policy as always with the default path_delimiter: `,\""
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
      check_dir_permission unless Fluent.windows?

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
        log.warn "'pos_file PATH' parameter is not set to a 'tail' source."
        log.warn "this parameter is highly recommended to save the position to resume tailing."
      end

      configure_tag
      configure_encoding

      @multiline_mode = parser_config["@type"].include?("multiline")
      @file_perm = system_config.file_permission || Fluent::DEFAULT_FILE_PERMISSION
      @dir_perm = system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION
      # parser is already created by parser helper
      @parser = parser_create(usage: parser_config['usage'] || @parser_configs.first.usage)
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
      # The metrics for currently tracking files. Since the value may decrease, it cannot be represented using the counter type, so 'prefer_gauge: true' is used instead.
      tracked_file_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "files_tracked_count", help_text: "Number of tracked files", prefer_gauge: true)

      @metrics = MetricsInfo.new(opened_file_metrics, closed_file_metrics, rotated_file_metrics, throttling_metrics, tracked_file_metrics)
    end

    def check_dir_permission
      expand_paths_raw.select { |path|
        not File.exist?(path)
      }.each { |path|
        inaccessible_dir = Pathname.new(File.expand_path(path))
          .ascend
          .reverse_each
          .find { |p| p.directory? && !p.executable? }
        if inaccessible_dir
          log.warn "Skip #{path} because '#{inaccessible_dir}' lacks execute permission."
        end
      }
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

      incompatibility_reasons = worker_incompatibility_reasons
      @worker_compatible = incompatibility_reasons.empty?
      @line_feeder = build_line_feeder
      if @num_threads > 1 && @worker_compatible
        setup_worker_pool
        log.info "in_tail worker parsing enabled with #{@num_threads} threads"
      elsif @num_threads > 1
        log.warn "in_tail worker parsing is unavailable; using synchronous parsing (#{incompatibility_reasons.join(', ')})"
      end

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
      shutdown_worker_pool

      super
    end

    def close
      super
      @worker_completion_watcher&.close
      # close file handles after all threads stopped (in #close of thread plugin helper)
      # It may be because we need to wait IOHandler.ready_to_shutdown()
      close_watcher_handles
      @pf_file.close if @pf_file
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

    # TailInput builds its LineFeeder in start, after the configure of a plugin
    # which inherits TailInput is done, so that the parameters set by that
    # plugin are used.
    def build_line_feeder
      LineFeeder.new(
        parser: @parser,
        router_provider: method(:router),
        log: log,
        tag: @tag,
        tag_prefix: @tag_prefix,
        tag_suffix: @tag_suffix,
        path_key: @path_key,
        emit_unmatched_lines: @emit_unmatched_lines,
        multiline_mode: @multiline_mode,
        parse_handler: method(@multiline_mode ? :parse_multilines : :parse_singleline),
        convert_handler: method(:convert_line_to_event),
        flush_handler: method(:flush_buffer),
      )
    end

    def setup_watcher(target_info, pe)
      # A plugin may build a watcher before start. The LineFeeder of that
      # moment has the same parameters, because the configure of every plugin
      # is done before the watchers are built.
      @line_feeder ||= build_line_feeder
      file_feed = @line_feeder.new_file_feed(flush_interval: @multiline_flush_interval)
      read_from_head = !@startup || @read_from_head
      tw = TailWatcher.new(target_info, pe, log, read_from_head, @follow_inodes, method(:update_watcher), file_feed, method(:io_handler), @metrics)

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

      tw.group_watcher = add_path_to_group_watcher(target_info.path, tw)

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
        log.warn "stat() for #{path} failed. Continuing without tailing it."
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
      @worker_deferred_unwatch&.each do |tw, target_info|
        tw.close_if_ready
        if tw.drained?
          @pf.unwatch(target_info) if @pf
          @worker_deferred_unwatch.delete(tw)
        end
      end
    end

    # refresh_watchers calls @tails.keys so we don't use stop_watcher -> start_watcher sequence for safety.
    def update_watcher(tail_watcher, pe, new_inode)
      # TODO we should use another callback for this.
      # To suppress impact to existing logics, limit the case to `@follow_inodes`.
      # We may not need `@follow_inodes` condition.
      if @follow_inodes && new_inode.nil?
        # nil inode means the file disappeared, so we only need to stop it.
        @tails.delete(tail_watcher.path)
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
      defer_worker_completion = tw.pending? || @worker_waiting_watchers&.include?(tw)
      deferred_unwatch = @pf && tw.unwatched && (@follow_inodes || !@tails[tw.path]) && defer_worker_completion
      tw.defer_close if close_io && defer_worker_completion
      if @follow_inodes && tw.ino != ino
        log.warn("detach_watcher could be detaching an unexpected tail_watcher with a different ino.",
                  path: tw.path, actual_ino_in_tw: tw.ino, expect_ino_to_close: ino)
      end
      tw.watchers.each do |watcher|
        event_loop_detach(watcher)
      end
      tw.detach(@shutdown_start_time)

      tw.close if close_io

      # A watcher waiting for `rotate_wait` cannot read once its path leaves the group watcher.
      tw.group_watcher&.delete(tw.path, tw)

      if @pf && tw.unwatched && (@follow_inodes || !@tails[tw.path])
        target_info = TargetInfo.new(tw.path, ino)
        if deferred_unwatch
          @worker_deferred_unwatch[tw] = target_info
        else
          @pf.unwatch(target_info)
        end
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
        return
      end

      return if @tails_rotate_wait[tw]

      if throttling_is_enabled?(tw)
        # When the throttling feature is enabled, it might not reach EOF yet.
        # Should ensure to read all contents before closing it, with keeping throttling.
        start_time_to_wait = Fluent::Clock.now
        timer = timer_execute(:in_tail_close_watcher, 1, repeat: true) do
          # Without the watch timer, nothing else notifies a watcher whose path has been rotated away.
          unless @enable_watch_timer
            begin
              tw.read_more
            rescue => e
              log.error e.to_s
              log.error_backtrace
            end
          end
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

    def statistics
      stats = super

      stats = {
        'input' => stats["input"].merge({
          'opened_file_count' => @metrics.opened.get,
          'closed_file_count' => @metrics.closed.get,
          'rotated_file_count' => @metrics.rotated.get,
          'throttled_log_count' => @metrics.throttled.get,
          'tracked_file_count' => @metrics.tracked.get,
        })
      }
      if @worker_pool
        stats['input'].merge!(
          'worker_pending_batch_count' => @worker_metrics[:pending_batches].get,
          'worker_pending_bytes' => @worker_metrics[:pending_bytes].get,
          'worker_parse_error_count' => @worker_metrics[:parse_errors].get,
        )
      end
      stats
    end

    private

    def io_handler(watcher, path)
      opts = {
        path: path,
        log: log,
        read_lines_limit: @read_lines_limit,
        read_bytes_limit_per_second: @read_bytes_limit_per_second,
        open_on_every_update: @open_on_every_update,
        metrics: @metrics,
        max_line_size: @max_line_size,
      }
      unless @encoding.nil?
        if @from_encoding.nil?
          opts[:encoding] = @encoding
        else
          opts[:encoding] = @from_encoding
          opts[:encoding_to_convert] = @encoding
        end
      end

      TailWatcher::IOHandler.new(
        watcher,
        **opts,
        &method(:receive_lines)
      )
    end
  end

  # Reopen TailInput to keep worker-mode setup and lifecycle helpers grouped
  # separately from the synchronous file-watching implementation above.
  class TailInput
    WORKER_COMPATIBILITY = WorkerCompatibility.new(self)
    WORKER_PARSER_TYPES = %w[
      none regexp json csv tsv ltsv msgpack apache apache2 apache_error nginx syslog
    ].freeze
    WORKER_QUEUE_BYTES = 64 * 1024 * 1024
    WorkerBatch = Struct.new(:watcher, :lines)

    private

    def worker_incompatibility_reasons
      reasons = WORKER_COMPATIBILITY.incompatible_hooks(self).map { |hook| "overridden hook: #{hook}" }
      reasons << 'multiline parsing is enabled' if @multiline_mode
      reasons << 'open_on_every_update is enabled' if @open_on_every_update
      parser_type = @parser_configs.first[:@type]
      reasons << "unsupported parser: #{parser_type}" unless WORKER_PARSER_TYPES.include?(parser_type)
      parser_usage = @parser_configs.first.usage
      reasons << 'the configured parser instance was replaced' unless @parser.equal?(@_parsers[parser_usage])
      reasons
    end

    def create_worker_parsers(num_threads)
      return [] unless @worker_compatible
      raise ArgumentError, 'worker count must be positive' unless num_threads.positive?

      parser_config = @parser_configs.first.corresponding_config_element
      Array.new(num_threads) do |index|
        usage = "__in_tail_worker_#{object_id}_#{index}"
        parser_create(usage: usage, conf: parser_config)
      end
    end

    def setup_worker_pool
      @worker_metrics = {
        pending_batches: metrics_create(namespace: 'fluentd', subsystem: 'input', name: 'worker_pending_batches',
                                        help_text: 'Number of worker batches awaiting completion or acknowledgment',
                                        prefer_gauge: true),
        pending_bytes: metrics_create(namespace: 'fluentd', subsystem: 'input', name: 'worker_pending_bytes',
                                       help_text: 'Estimated bytes accounted for by pending worker batches',
                                       prefer_gauge: true),
        parse_errors: metrics_create(namespace: 'fluentd', subsystem: 'input', name: 'worker_parse_errors_total',
                                     help_text: 'Total number of worker parse errors'),
      }
      @worker_parsers = create_worker_parsers(@num_threads)
      @worker_line_feeders = @worker_parsers.map { |parser| build_worker_line_feeder(parser) }
      @worker_waiting_watchers = []
      @worker_deferred_unwatch = {}
      @worker_completion_watcher = WorkerCompletionWatcher.new { process_worker_completions }
      event_loop_attach(@worker_completion_watcher)
      @worker_pool = WorkerPool.new(
        num_threads: @num_threads,
        task_limit: @num_threads * 2,
        byte_limit: WORKER_QUEUE_BYTES,
        thread_create: ->(title, &block) { Thread.new { Thread.current.name = title.to_s if Thread.current.respond_to?(:name=); block.call } },
        notify: @worker_completion_watcher.method(:signal)
      ) do |index, batch|
        @worker_line_feeders[index].parse(batch.lines.map(&:dup), batch.watcher)
      end
      @worker_pool.start
    rescue
      event_loop_detach(@worker_completion_watcher) if @worker_completion_watcher&.attached?
      @worker_completion_watcher&.close
      raise
    end

    def build_worker_line_feeder(parser)
      LineFeeder.new(
        parser: parser,
        router_provider: method(:router),
        log: log,
        tag: @tag,
        tag_prefix: @tag_prefix,
        tag_suffix: @tag_suffix,
        path_key: @path_key,
        emit_unmatched_lines: @emit_unmatched_lines,
        multiline_mode: false
      )
    end

    def async_receive_lines(lines, watcher)
      return @line_feeder.feed_lines(lines, watcher) unless @worker_pool.statistics[:state] == :running

      bytes = lines.sum(&:bytesize) * 3
      batch = WorkerBatch.new(watcher, lines)
      if @worker_pool.submit(watcher, batch, bytes: bytes)
        update_worker_metrics
        @worker_waiting_watchers.delete(watcher)
        TailWatcher::IOHandler::ASYNC_PENDING
      else
        @worker_waiting_watchers << watcher unless @worker_waiting_watchers.include?(watcher)
        false
      end
    end

    def process_worker_completions
      while (result = @worker_pool.next_result)
        batch = result.task.payload
        if result.error
          @worker_metrics[:parse_errors].inc
          log.warn 'worker parsing failed; retrying synchronously', path: batch.watcher.path, error: result.error
          emitted = @line_feeder.feed_lines(batch.lines, batch.watcher)
        else
          emitted = @line_feeder.emit(result.value, batch.watcher)
        end
        @worker_pool.acknowledge(result)
        update_worker_metrics
        batch.watcher.complete_async(emitted)
        finish_worker_deferred_close(batch.watcher)
      end

      waiting = @worker_waiting_watchers
      @worker_waiting_watchers = []
      waiting.each do |watcher|
        if watcher.detached?
          watcher.read_more
          finish_worker_deferred_close(watcher)
        else
          watcher.on_notify
        end
      end
    end

    def finish_worker_deferred_close(watcher)
      watcher.close_if_ready
      return unless watcher.drained?

      target_info = @worker_deferred_unwatch.delete(watcher)
      @pf.unwatch(target_info) if target_info && @pf
    end

    def update_worker_metrics
      statistics = @worker_pool.statistics
      @worker_metrics[:pending_batches].set(statistics[:tasks])
      @worker_metrics[:pending_bytes].set(statistics[:bytes])
    end

    def shutdown_worker_pool
      return unless @worker_pool

      deadline = Fluent::Clock.now + TailWatcher::IOHandler::SHUTDOWN_TIMEOUT
      watchers = (@tails.values + @tails_rotate_wait.keys + @worker_deferred_unwatch.keys).uniq
      while @worker_pool.statistics[:tasks].positive? || watchers.any? { |watcher| !watcher.drained? }
        break if Fluent::Clock.now >= deadline
        sleep 0.01
      end
      if @worker_pool.statistics[:tasks].positive? || watchers.any? { |watcher| !watcher.drained? }
        log.warn 'in_tail worker pool did not drain before shutdown timeout'
      end
      @worker_pool.stop
      unless @worker_pool.join(timeout: TailWatcher::IOHandler::SHUTDOWN_TIMEOUT)
        log.warn 'in_tail worker threads did not stop before timeout'
      end
    end
  end
end
