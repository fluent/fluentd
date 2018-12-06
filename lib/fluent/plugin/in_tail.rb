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

if Fluent.windows?
  require_relative 'file_wrapper'
else
  Fluent::FileWrapper = File
end

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('tail', self)

    helpers :timer, :event_loop, :parser, :compat_parameters

    class WatcherSetupError < StandardError
      def initialize(msg)
        @message = msg
      end

      def to_s
        @message
      end
    end

    FILE_PERMISSION = 0644

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
    desc 'The tag of the event.'
    config_param :tag, :string
    desc 'The paths to exclude the files from watcher list.'
    config_param :exclude_path, :array, default: []
    desc 'Specify interval to keep reference to old file when rotate a file.'
    config_param :rotate_wait, :time, default: 5
    desc 'Fluentd will record the position it last read into this file.'
    config_param :pos_file, :string, default: nil
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

    attr_reader :paths

    @@pos_file_paths = {}

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      parser_config = conf.elements('parse').first
      unless parser_config
        raise Fluent::ConfigError, "<parse> section is required."
      end
      unless parser_config["@type"]
        raise Fluent::ConfigError, "parse/@type is required."
      end

      (1..Fluent::Plugin::MultilineParser::FORMAT_MAX_NUM).each do |n|
        parser_config["format#{n}"] = conf["format#{n}"] if conf["format#{n}"]
      end

      super

      if !@enable_watch_timer && !@enable_stat_watcher
        raise Fluent::ConfigError, "either of enable_watch_timer or enable_stat_watcher must be true"
      end

      @paths = @path.split(',').map {|path| path.strip }
      if @paths.empty?
        raise Fluent::ConfigError, "tail: 'path' parameter is required on tail input"
      end

      # TODO: Use plugin_root_dir and storage plugin to store positions if available
      if @pos_file
        if @@pos_file_paths.has_key?(@pos_file) && !called_in_test?
          plugin_id_using_this_path = @@pos_file_paths[@pos_file]
          raise Fluent::ConfigError, "Other 'in_tail' plugin already use same pos_file path: plugin_id = #{plugin_id_using_this_path}, pos_file path = #{@pos_file}"
        end
        @@pos_file_paths[@pos_file] = self.plugin_id
      else
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
      @file_perm = system_config.file_permission || FILE_PERMISSION
      @parser = parser_create(conf: parser_config)
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
        FileUtils.mkdir_p(pos_file_dir) unless Dir.exist?(pos_file_dir)
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT|File::BINARY, @file_perm)
        @pf_file.sync = true
        @pf = PositionFile.parse(@pf_file)
      end

      refresh_watchers unless @skip_refresh_on_startup
      timer_execute(:in_tail_refresh_watchers, @refresh_interval, &method(:refresh_watchers))
    end

    def shutdown
      # during shutdown phase, don't close io. It should be done in close after all threads are stopped. See close.
      stop_watchers(@tails.keys, immediate: true, remove_watcher: false)
      @pf_file.close if @pf_file

      super
    end

    def close
      super
      # close file handles after all threads stopped (in #close of thread plugin helper)
      close_watcher_handles
    end

    def expand_paths
      date = Time.now
      paths = []

      @paths.each { |path|
        path = date.strftime(path)
        if path.include?('*')
          paths += Dir.glob(path).select { |p|
            begin
              is_file = !File.directory?(p)
              if File.readable?(p) && is_file
                if @limit_recently_modified && File.mtime(p) < (date - @limit_recently_modified)
                  false
                else
                  true
                end
              else
                if is_file
                  unless @ignore_list.include?(path)
                    log.warn "#{p} unreadable. It is excluded and would be examined next time."
                    @ignore_list << path if @ignore_repeated_permission_error
                  end
                end
                false
              end
            rescue Errno::ENOENT
              false
            end
          }
        else
          # When file is not created yet, Dir.glob returns an empty array. So just add when path is static.
          paths << path
        end
      }
      excluded = @exclude_path.map { |path| path = date.strftime(path); path.include?('*') ? Dir.glob(path) : path }.flatten.uniq
      paths - excluded
    end

    # in_tail with '*' path doesn't check rotation file equality at refresh phase.
    # So you should not use '*' path when your logs will be rotated by another tool.
    # It will cause log duplication after updated watch files.
    # In such case, you should separate log directory and specify two paths in path parameter.
    # e.g. path /path/to/dir/*,/path/to/rotated_logs/target_file
    def refresh_watchers
      target_paths = expand_paths
      existence_paths = @tails.keys

      unwatched = existence_paths - target_paths
      added = target_paths - existence_paths

      stop_watchers(unwatched, immediate: false, unwatched: true) unless unwatched.empty?
      start_watchers(added) unless added.empty?
    end

    def setup_watcher(path, pe)
      line_buffer_timer_flusher = (@multiline_mode && @multiline_flush_interval) ? TailWatcher::LineBufferTimerFlusher.new(log, @multiline_flush_interval, &method(:flush_buffer)) : nil
      tw = TailWatcher.new(path, @rotate_wait, pe, log, @read_from_head, @enable_watch_timer, @enable_stat_watcher, @read_lines_limit, method(:update_watcher), line_buffer_timer_flusher, @from_encoding, @encoding, open_on_every_update, &method(:receive_lines))
      tw.attach do |watcher|
        event_loop_attach(watcher.timer_trigger) if watcher.timer_trigger
        event_loop_attach(watcher.stat_trigger) if watcher.stat_trigger
      end
      tw
    rescue => e
      if tw
        tw.detach { |watcher|
          event_loop_detach(watcher.timer_trigger) if watcher.timer_trigger
          event_loop_detach(watcher.stat_trigger) if watcher.stat_trigger
        }
        tw.close
      end
      raise e
    end

    def start_watchers(paths)
      paths.each { |path|
        pe = nil
        if @pf
          pe = @pf[path]
          if @read_from_head && pe.read_inode.zero?
            begin
              pe.update(Fluent::FileWrapper.stat(path).ino, 0)
            rescue Errno::ENOENT
              $log.warn "#{path} not found. Continuing without tailing it."
            end
          end
        end

        begin
          tw = setup_watcher(path, pe)
        rescue WatcherSetupError => e
          log.warn "Skip #{path} because unexpected setup error happens: #{e}"
          next
        end
        @tails[path] = tw
      }
    end

    def stop_watchers(paths, immediate: false, unwatched: false, remove_watcher: true)
      paths.each { |path|
        tw = remove_watcher ? @tails.delete(path) : @tails[path]
        if tw
          tw.unwatched = unwatched
          if immediate
            detach_watcher(tw, false)
          else
            detach_watcher_after_rotate_wait(tw)
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
    end

    # refresh_watchers calls @tails.keys so we don't use stop_watcher -> start_watcher sequence for safety.
    def update_watcher(path, pe)
      if @pf
        unless pe.read_inode == @pf[path].read_inode
          log.trace "Skip update_watcher because watcher has been already updated by other inotify event"
          return
        end
      end
      rotated_tw = @tails[path]
      @tails[path] = setup_watcher(path, pe)
      detach_watcher_after_rotate_wait(rotated_tw) if rotated_tw
    end

    # TailWatcher#close is called by another thread at shutdown phase.
    # It causes 'can't modify string; temporarily locked' error in IOHandler
    # so adding close_io argument to avoid this problem.
    # At shutdown, IOHandler's io will be released automatically after detached the event loop
    def detach_watcher(tw, close_io = true)
      tw.detach { |watcher|
        event_loop_detach(watcher.timer_trigger) if watcher.timer_trigger
        event_loop_detach(watcher.stat_trigger) if watcher.stat_trigger
      }
      tw.close if close_io
      flush_buffer(tw)
      if tw.unwatched && @pf
        @pf[tw.path].update_pos(PositionFile::UNWATCHED_POSITION)
      end
    end

    def detach_watcher_after_rotate_wait(tw)
      # Call event_loop_attach/event_loop_detach is high-cost for short-live object.
      # If this has a problem with large number of files, use @_event_loop directly instead of timer_execute.
      timer_execute(:in_tail_close_watcher, @rotate_wait, repeat: false) do
        detach_watcher(tw)
      end
    end

    def flush_buffer(tw)
      if lb = tw.line_buffer
        lb.chomp!
        @parser.parse(lb) { |time, record|
          if time && record
            tag = if @tag_prefix || @tag_suffix
                    @tag_prefix + tw.tag + @tag_suffix
                  else
                    @tag
                  end
            record[@path_key] ||= tw.path unless @path_key.nil?
            router.emit(tag, time, record)
          else
            log.warn "got incomplete line at shutdown from #{tw.path}: #{lb.inspect}"
          end
        }
      end
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
            log.warn "pattern not match: #{line.inspect}"
          end
        }
      rescue => e
        log.warn line.dump, error: e.to_s
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

    def parse_multilines(lines, tail_watcher)
      lb = tail_watcher.line_buffer
      es = Fluent::MultiEventStream.new
      if @parser.has_firstline?
        tail_watcher.line_buffer_timer_flusher.reset_timer if tail_watcher.line_buffer_timer_flusher
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
      tail_watcher.line_buffer = lb
      es
    end

    class TailWatcher
      def initialize(path, rotate_wait, pe, log, read_from_head, enable_watch_timer, enable_stat_watcher, read_lines_limit, update_watcher, line_buffer_timer_flusher, from_encoding, encoding, open_on_every_update, &receive_lines)
        @path = path
        @rotate_wait = rotate_wait
        @pe = pe || MemoryPositionEntry.new
        @read_from_head = read_from_head
        @enable_watch_timer = enable_watch_timer
        @enable_stat_watcher = enable_stat_watcher
        @read_lines_limit = read_lines_limit
        @receive_lines = receive_lines
        @update_watcher = update_watcher

        @stat_trigger = @enable_stat_watcher ? StatWatcher.new(self, &method(:on_notify)) : nil
        @timer_trigger = @enable_watch_timer ? TimerTrigger.new(1, log, &method(:on_notify)) : nil

        @rotate_handler = RotateHandler.new(self, &method(:on_rotate))
        @io_handler = nil
        @log = log

        @line_buffer_timer_flusher = line_buffer_timer_flusher
        @from_encoding = from_encoding
        @encoding = encoding
        @open_on_every_update = open_on_every_update
      end

      attr_reader :path
      attr_reader :log, :pe, :read_lines_limit, :open_on_every_update
      attr_reader :from_encoding, :encoding
      attr_reader :stat_trigger, :enable_watch_timer, :enable_stat_watcher
      attr_accessor :timer_trigger
      attr_accessor :line_buffer, :line_buffer_timer_flusher
      attr_accessor :unwatched  # This is used for removing position entry from PositionFile

      def tag
        @parsed_tag ||= @path.tr('/', '.').gsub(/\.+/, '.').gsub(/^\./, '')
      end

      def wrap_receive_lines(lines)
        @receive_lines.call(lines, self)
      end

      def attach
        on_notify
        yield self
      end

      def detach
        yield self
        @io_handler.on_notify if @io_handler
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
            @io_handler = IOHandler.new(self, &method(:wrap_receive_lines))
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

          log_msg = "detected rotation of #{@path}"
          log_msg << "; waiting #{@rotate_wait} seconds" if watcher_needs_update # wait rotate_time if previous file exists
          @log.info log_msg

          if watcher_needs_update
            @update_watcher.call(@path, swap_state(@pe))
          else
            @io_handler = IOHandler.new(self, &method(:wrap_receive_lines))
          end
        end
      end

      def swap_state(pe)
        # Use MemoryPositionEntry for rotated file temporary
        mpe = MemoryPositionEntry.new
        mpe.update(pe.read_inode, pe.read_pos)
        @pe = mpe
        pe # This pe will be updated in on_rotate after TailWatcher is initialized
      end

      class TimerTrigger < Coolio::TimerWatcher
        def initialize(interval, log, &callback)
          @callback = callback
          @log = log
          super(interval, true)
        end

        def on_timer
          @callback.call
        rescue => e
          @log.error e.to_s
          @log.error_backtrace
        end
      end

      class StatWatcher < Coolio::StatWatcher
        def initialize(watcher, &callback)
          @watcher = watcher
          @callback = callback
          super(watcher.path)
        end

        def on_change(prev, cur)
          @callback.call
        rescue
          # TODO log?
          @watcher.log.error $!.to_s
          @watcher.log.error_backtrace
        end
      end

      class FIFO
        def initialize(from_encoding, encoding)
          @from_encoding = from_encoding
          @encoding = encoding
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
          if @from_encoding == @encoding
            s
          else
            s.encode(@encoding, @from_encoding)
          end
        end

        def next_line
          idx = @buffer.index(@eol)
          convert(@buffer.slice!(0, idx + 1)) unless idx.nil?
        end

        def bytesize
          @buffer.bytesize
        end
      end

      class IOHandler
        def initialize(watcher, &receive_lines)
          @watcher = watcher
          @receive_lines = receive_lines
          @fifo = FIFO.new(@watcher.from_encoding || Encoding::ASCII_8BIT, @watcher.encoding || Encoding::ASCII_8BIT)
          @iobuf = ''.force_encoding('ASCII-8BIT')
          @lines = []
          @io = nil
          @notify_mutex = Mutex.new
          @watcher.log.info "following tail of #{@watcher.path}"
        end

        def on_notify
          @notify_mutex.synchronize { handle_notify }
        end

        def handle_notify
          with_io do |io|
            begin
              read_more = false

              if !io.nil? && @lines.empty?
                begin
                  while true
                    @fifo << io.readpartial(2048, @iobuf)
                    while (line = @fifo.next_line)
                      @lines << line
                    end
                    if @lines.size >= @watcher.read_lines_limit
                      # not to use too much memory in case the file is very large
                      read_more = true
                      break
                    end
                  end
                rescue EOFError
                end
              end

              unless @lines.empty?
                if @receive_lines.call(@lines)
                  @watcher.pe.update_pos(io.pos - @fifo.bytesize)
                  @lines.clear
                else
                  read_more = false
                end
              end
            end while read_more
          end
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

        def open
          io = Fluent::FileWrapper.open(@watcher.path)
          io.seek(@watcher.pe.read_pos + @fifo.bytesize)
          io
        rescue RangeError
          io.close if io
          raise WatcherSetupError, "seek error with #{@watcher.path}: file position = #{@watcher.pe.read_pos.to_s(16)}, reading bytesize = #{@fifo.bytesize.to_s(16)}"
        rescue Errno::ENOENT
          nil
        end

        def with_io
          begin
            if @watcher.open_on_every_update
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
            @watcher.log.error $!.to_s
            @watcher.log.error_backtrace
            close
          end
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
        def initialize(watcher, &on_rotate)
          @watcher = watcher
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

          begin
            if @inode != inode || fsize < @fsize
              @on_rotate.call(stat)
            end
            @inode = inode
            @fsize = fsize
          end

        rescue
          @watcher.log.error $!.to_s
          @watcher.log.error_backtrace
        end
      end

      class LineBufferTimerFlusher
        def initialize(log, flush_interval, &flush_method)
          @log = log
          @flush_interval = flush_interval
          @flush_method = flush_method
          @start = nil
        end

        def on_notify(tw)
          if @start && @flush_interval
            if Time.now - @start >= @flush_interval
              @flush_method.call(tw)
              tw.line_buffer = nil
              @start = nil
            end
          end
        end

        def reset_timer
          @start = Time.now
        end
      end
    end

    class PositionFile
      UNWATCHED_POSITION = 0xffffffffffffffff

      def initialize(file, file_mutex, map, last_pos)
        @file = file
        @file_mutex = file_mutex
        @map = map
        @last_pos = last_pos
      end

      def [](path)
        if m = @map[path]
          return m
        end

        @file_mutex.synchronize {
          @file.pos = @last_pos
          @file.write "#{path}\t0000000000000000\t0000000000000000\n"
          seek = @last_pos + path.bytesize + 1
          @last_pos = @file.pos
          @map[path] = FilePositionEntry.new(@file, @file_mutex, seek, 0, 0)
        }
      end

      def self.parse(file)
        compact(file)

        file_mutex = Mutex.new
        map = {}
        file.pos = 0
        file.each_line {|line|
          m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
          unless m
            $log.warn "Unparsable line in pos_file: #{line}"
            next
          end
          path = m[1]
          pos = m[2].to_i(16)
          ino = m[3].to_i(16)
          seek = file.pos - line.bytesize + path.bytesize + 1
          map[path] = FilePositionEntry.new(file, file_mutex, seek, pos, ino)
        }
        new(file, file_mutex, map, file.pos)
      end

      # Clean up unwatched file entries
      def self.compact(file)
        file.pos = 0
        existent_entries = file.each_line.map { |line|
          m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
          unless m
            $log.warn "Unparsable line in pos_file: #{line}"
            next
          end
          path = m[1]
          pos = m[2].to_i(16)
          ino = m[3].to_i(16)
          # 32bit inode converted to 64bit at this phase
          pos == UNWATCHED_POSITION ? nil : ("%s\t%016x\t%016x\n" % [path, pos, ino])
        }.compact

        file.pos = 0
        file.truncate(0)
        file.write(existent_entries.join)
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
  end
end
