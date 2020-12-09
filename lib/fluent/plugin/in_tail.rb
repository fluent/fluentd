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

if Fluent.windows?
  require_relative 'file_wrapper'
else
  Fluent::FileWrapper = File
end

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('tail', self)

    helpers :timer, :event_loop, :parser, :compat_parameters

    RESERVED_CHARS = ['/', '*', '%'].freeze

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
      @pf_file = nil
      @pf = nil
      @ignore_list = []
    end

    desc 'The paths to read. Multiple paths can be specified, separated by comma.'
    config_param :path, :string
    desc 'path delimiter used for spliting path config'
    config_param :path_delimiter, :string, default: ','
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
      # during shutdown phase, don't close io. It should be done in close after all threads are stopped. See close.
      stop_watchers(existence_path, immediate: true, remove_watcher: false)
      @pf_file.close if @pf_file

      super
    end

    def close
      super
      # close file handles after all threads stopped (in #close of thread plugin helper)
      close_watcher_handles
    end

    def have_read_capability?
      @capability.have_capability?(:effective, :dac_read_search) ||
        @capability.have_capability?(:effective, :dac_override)
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
        if path.include?('*')
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
            rescue Errno::ENOENT
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
        path.include?('*') ? Dir.glob(path) : path
      }.flatten.uniq
      # filter out non existing files, so in case pattern is without '*' we don't do unnecessary work
      hash = {}
      (paths - excluded).select { |path|
        FileTest.exist?(path)
      }.each { |path|
        target_info = TargetInfo.new(path, Fluent::FileWrapper.stat(path).ino)
        if @follow_inodes
          hash[target_info.ino] = target_info
        else
          hash[target_info.path] = target_info
        end
      }
      hash
    end

    def existence_path
      hash = {}
      @tails.each_key {|path_ino|
        if @follow_inodes
          hash[path_ino.ino] = path_ino
        else
          hash[path_ino.path] = path_ino
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

      log.debug { "tailing paths: target = #{target_paths.join(",")} | existing = #{existence_paths.join(",")}" }

      unwatched_hash = existence_paths_hash.reject {|key, value| target_paths_hash.key?(key)}
      added_hash = target_paths_hash.reject {|key, value| existence_paths_hash.key?(key)}

      stop_watchers(unwatched_hash, immediate: false, unwatched: true) unless unwatched_hash.empty?
      start_watchers(added_hash) unless added_hash.empty?
    end

    def setup_watcher(path, ino, pe)
      line_buffer_timer_flusher = @multiline_mode ? TailWatcher::LineBufferTimerFlusher.new(log, @multiline_flush_interval, &method(:flush_buffer)) : nil
      tw = TailWatcher.new(path, ino, pe, log, @read_from_head, @follow_inodes, method(:update_watcher), line_buffer_timer_flusher, method(:io_handler))

      if @enable_watch_timer
        tt = TimerTrigger.new(1, log) { tw.on_notify }
        tw.register_watcher(tt)
      end

      if @enable_stat_watcher
        tt = StatWatcher.new(path, log) { tw.on_notify }
        tw.register_watcher(tt)
      end

      tw.on_notify

      tw.watchers.each do |watcher|
        event_loop_attach(watcher)
      end

      tw
    rescue => e
      if tw
        tw.watchers.each do |watcher|
          event_loop_detach(watcher)
        end

        tw.detach
        tw.close
      end
      raise e
    end

    def start_watchers(paths_with_inodes)
      paths_with_inodes.each_value { |path_with_inode|
        path = path_with_inode.path
        ino = path_with_inode.ino
        pe = nil
        if @pf
          pe = @pf[path, ino]
          if @read_from_head && pe.read_inode.zero?
            begin
              pe.update(Fluent::FileWrapper.stat(path).ino, 0)
            rescue Errno::ENOENT
              $log.warn "#{path} not found. Continuing without tailing it."
            end
          end
        end

        begin
          tw = setup_watcher(path, ino, pe)
        rescue WatcherSetupError => e
          log.warn "Skip #{path} because unexpected setup error happens: #{e}"
          next
        end
        target_info = TargetInfo.new(path, Fluent::FileWrapper.stat(path).ino)
        @tails[target_info] = tw
      }
    end

    def stop_watchers(paths_with_inodes, immediate: false, unwatched: false, remove_watcher: true)
      paths_with_inodes.each_value { |path_with_inode|
        if remove_watcher
          tw = @tails.delete(path_with_inode)
        else
          tw = @tails[path_with_inode]
        end
        if tw
          tw.unwatched = unwatched
          if immediate
            detach_watcher(tw, path_with_inode.ino, false)
          else
            detach_watcher_after_rotate_wait(tw, path_with_inode.ino)
          end
        end
      }
    end

    def close_watcher_handles
      @tails.keys.each do |path_with_inode|
        tw = @tails.delete(path_with_inode)
        if tw
          tw.close
        end
      end
    end

    # refresh_watchers calls @tails.keys so we don't use stop_watcher -> start_watcher sequence for safety.
    def update_watcher(path, inode, pe)
      log.info("detected rotation of #{path}; waiting #{@rotate_wait} seconds")

      if @pf
        unless pe.read_inode == @pf[path, pe.read_inode].read_inode
          log.debug "Skip update_watcher because watcher has been already updated by other inotify event"
          return
        end
      end

      target_info = TargetInfo.new(path, pe.read_inode)
      rotated_tw = @tails[target_info]

      new_target_info = TargetInfo.new(path, inode)

      if @follow_inodes
        new_position_entry = @pf[path, inode]

        if new_position_entry.read_inode == 0
          @tails[new_target_info] = setup_watcher(path, inode, new_position_entry)
        end
      else
        @tails[new_target_info] = setup_watcher(path, inode, pe)
      end
      detach_watcher_after_rotate_wait(rotated_tw, pe.read_inode) if rotated_tw
    end

    # TailWatcher#close is called by another thread at shutdown phase.
    # It causes 'can't modify string; temporarily locked' error in IOHandler
    # so adding close_io argument to avoid this problem.
    # At shutdown, IOHandler's io will be released automatically after detached the event loop
    def detach_watcher(tw, ino, close_io = true)
      tw.watchers.each do |watcher|
        event_loop_detach(watcher)
      end
      tw.detach

      tw.close if close_io

      if tw.unwatched && @pf
        @pf.unwatch(tw.path, ino)
      end
    end

    def detach_watcher_after_rotate_wait(tw, ino)
      # Call event_loop_attach/event_loop_detach is high-cost for short-live object.
      # If this has a problem with large number of files, use @_event_loop directly instead of timer_execute.
      timer_execute(:in_tail_close_watcher, @rotate_wait, repeat: false) do
        detach_watcher(tw, ino)
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

    private

    def io_handler(watcher, path)
      TailWatcher::IOHandler.new(
        watcher,
        path: path,
        log: log,
        read_lines_limit: @read_lines_limit,
        open_on_every_update: @open_on_every_update,
        from_encoding: @from_encoding,
        encoding: @encoding,
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
      def initialize(path, ino, pe, log, read_from_head, follow_inodes, update_watcher, line_buffer_timer_flusher, io_handler_build)
        @path = path
        @ino = ino
        @pe = pe || MemoryPositionEntry.new
        @read_from_head = read_from_head
        @follow_inodes = follow_inodes
        @update_watcher = update_watcher
        @log = log
        @rotate_handler = RotateHandler.new(log, &method(:on_rotate))
        @line_buffer_timer_flusher = line_buffer_timer_flusher
        @io_handler = nil
        @io_handler_build = io_handler_build
        @watchers = []
      end

      attr_reader :path, :ino
      attr_reader :pe
      attr_reader :line_buffer_timer_flusher
      attr_accessor :unwatched  # This is used for removing position entry from PositionFile
      attr_reader :watchers

      def tag
        @parsed_tag ||= @path.tr('/', '.').gsub(/\.+/, '.').gsub(/^\./, '')
      end

      def register_watcher(watcher)
        @watchers << watcher
      end

      def detach
        @io_handler.on_notify if @io_handler
        @line_buffer_timer_flusher&.close(self)
      end

      def close
        if @io_handler
          @io_handler.close
          @io_handler = nil
        end
      end

      def on_notify
        begin
          stat = Fluent::FileWrapper.stat(@path)
        rescue Errno::ENOENT
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
            # No need to update a watcher if stat is nil (file not present), because moving to inodes will create
            # new watcher, and old watcher will be closed by stop_watcher in refresh_watchers method
            if stat
              if @follow_inodes
                # don't want to swap state because we need latest read offset in pos file even after rotate_wait
                @update_watcher.call(@path, stat.ino, @pe)
              else
                @update_watcher.call(@path, stat.ino, swap_state(@pe))
              end
            end
          else
            @log.info "detected rotation of #{@path}"
            @io_handler = io_handler
          end
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
        def initialize(from_encoding, encoding)
          @from_encoding = from_encoding
          @encoding = encoding
          @need_enc = from_encoding != encoding
          @buffer = ''.force_encoding(from_encoding)
          @eol = "\n".encode(from_encoding).freeze
        end

        attr_reader :from_encoding, :encoding, :buffer

        def <<(chunk)
          # Although "chunk" is most likely transient besides String#force_encoding itself
          # won't affect the actual content of it, it is also probable that "chunk" is
          # a reused buffer and changing its encoding causes some problems on the caller side.
          #
          # Actually, the caller here is specific and "chunk" comes from IO#partial with
          # the second argument, which the function always returns as a return value.
          #
          # Feeding a string that has its encoding attribute set to any double-byte or
          # quad-byte encoding to IO#readpartial as the second arguments results in an
          # assertion failure on Ruby < 2.4.0 for unknown reasons.
          orig_encoding = chunk.encoding
          chunk.force_encoding(from_encoding)
          @buffer << chunk
          # Thus the encoding needs to be reverted back here
          chunk.force_encoding(orig_encoding)
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

          until idx.nil?
            # Using freeze and slice is faster than slice!
            # See https://github.com/fluent/fluentd/pull/2527
            @buffer.freeze
            rbuf = @buffer.slice(0, idx + 1)
            @buffer = @buffer.slice(idx + 1, @buffer.size)
            idx = @buffer.index(@eol)
            lines << convert(rbuf)
          end
        end

        def bytesize
          @buffer.bytesize
        end
      end

      class IOHandler
        def initialize(watcher, path:, read_lines_limit:, log:, open_on_every_update:, from_encoding: nil, encoding: nil, &receive_lines)
          @watcher = watcher
          @path = path
          @read_lines_limit = read_lines_limit
          @receive_lines = receive_lines
          @open_on_every_update = open_on_every_update
          @fifo = FIFO.new(from_encoding || Encoding::ASCII_8BIT, encoding || Encoding::ASCII_8BIT)
          @iobuf = ''.force_encoding('ASCII-8BIT')
          @lines = []
          @io = nil
          @notify_mutex = Mutex.new
          @log = log

          @log.info "following tail of #{@path}"
        end

        def on_notify
          @notify_mutex.synchronize { handle_notify }
        end

        def close
          if @io && !@io.closed?
            @io.close
            @io = nil
          end
        end

        def opened?
          !!@io
        end

        private

        def handle_notify
          with_io do |io|
            begin
              read_more = false

              if !io.nil? && @lines.empty?
                begin
                  while true
                    @fifo << io.readpartial(8192, @iobuf)
                    @fifo.read_lines(@lines)
                    if @lines.size >= @read_lines_limit
                      # not to use too much memory in case the file is very large
                      read_more = true
                      break
                    end
                  end
                rescue EOFError
                end
              end

              unless @lines.empty?
                if @receive_lines.call(@lines, @watcher)
                  @watcher.pe.update_pos(io.pos - @fifo.bytesize)
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
          io.seek(@watcher.pe.read_pos + @fifo.bytesize)
          io
        rescue RangeError
          io.close if io
          raise WatcherSetupError, "seek error with #{@path}: file position = #{@watcher.pe.read_pos.to_s(16)}, reading bytesize = #{@fifo.bytesize.to_s(16)}"
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
          end
        rescue WatcherSetupError => e
          close
          raise e
        rescue
          @log.error $!.to_s
          @log.error_backtrace
          close
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
