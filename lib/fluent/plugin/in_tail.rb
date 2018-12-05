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

    FILE_PERMISSION = 0644

    def initialize
      super
      @paths = []
      @tails = {}
      @pf_file = nil
      @pf = nil
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
    desc 'Enable the additional watch timer.'
    config_param :enable_watch_timer, :bool, default: true
    desc 'The encoding after conversion of the input.'
    config_param :encoding, :string, default: nil
    desc 'The encoding of the input.'
    config_param :from_encoding, :string, default: nil
    desc 'Add the log path being tailed to records. Specify the field name to be used.'
    config_param :path_key, :string, default: nil

    attr_reader :paths

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

      @paths = @path.split(',').map {|path| path.strip }
      if @paths.empty?
        raise Fluent::ConfigError, "tail: 'path' parameter is required on tail input"
      end

      unless @pos_file
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
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT|File::BINARY, @file_perm)
        @pf_file.sync = true
        @pf = PositionFile.parse(@pf_file)
      end

      refresh_watchers
      timer_execute(:in_tail_refresh_watchers, @refresh_interval, &method(:refresh_watchers))
    end

    def shutdown
      stop_watchers(@tails.keys, true)
      @pf_file.close if @pf_file

      super
    end

    def expand_paths
      date = Time.now
      paths = []

      excluded = @exclude_path.map { |path| path = date.strftime(path); path.include?('*') ? Dir.glob(path) : path }.flatten.uniq
      @paths.each { |path|
        path = date.strftime(path)
        if path.include?('*')
          paths += Dir.glob(path).select { |p|
            if File.readable?(p)
              true
            else
              log.warn "#{p} unreadable. It is excluded and would be examined next time."
              false
            end
          }
        else
          # When file is not created yet, Dir.glob returns an empty array. So just add when path is static.
          paths << path
        end
      }
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

      stop_watchers(unwatched, false, true) unless unwatched.empty?
      start_watchers(added) unless added.empty?
    end

    def setup_watcher(path, pe)
      line_buffer_timer_flusher = (@multiline_mode && @multiline_flush_interval) ? TailWatcher::LineBufferTimerFlusher.new(log, @multiline_flush_interval, &method(:flush_buffer)) : nil
      tw = TailWatcher.new(path, @rotate_wait, pe, log, @read_from_head, @enable_watch_timer, @read_lines_limit, method(:update_watcher), line_buffer_timer_flusher, &method(:receive_lines))
      tw.attach do |watcher|
        watcher.timer_trigger = timer_execute(:in_tail_timer_trigger, 1, &watcher.method(:on_notify)) if watcher.enable_watch_timer
        event_loop_attach(watcher.stat_trigger)
      end
      tw
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

        @tails[path] = setup_watcher(path, pe)
      }
    end

    def stop_watchers(paths, immediate = false, unwatched = false)
      paths.each { |path|
        tw = @tails.delete(path)
        if tw
          tw.unwatched = unwatched
          if immediate
            close_watcher(tw, false)
          else
            close_watcher_after_rotate_wait(tw)
          end
        end
      }
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
      close_watcher_after_rotate_wait(rotated_tw) if rotated_tw
    end

    # TailWatcher#close is called by another thread at shutdown phase.
    # It causes 'can't modify string; temporarily locked' error in IOHandler
    # so adding close_io argument to avoid this problem.
    # At shutdown, IOHandler's io will be released automatically after detached the event loop
    def close_watcher(tw, close_io = true)
      tw.close(close_io)
      flush_buffer(tw)
      if tw.unwatched && @pf
        @pf[tw.path].update_pos(PositionFile::UNWATCHED_POSITION)
      end
    end

    def close_watcher_after_rotate_wait(tw)
      timer_execute(:in_tail_close_watcher, @rotate_wait, repeat: false) do
        close_watcher(tw)
      end
    end

    def flush_buffer(tw)
      if lb = tw.line_buffer
        lb.chomp!
        if @encoding
          if @from_encoding
            lb.encode!(@encoding, @from_encoding)
          else
            lb.force_encoding(@encoding)
          end
        end
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
        if @encoding
          if @from_encoding
            line.encode!(@encoding, @from_encoding)
          else
            line.force_encoding(@encoding)
          end
        end
        @parser.parse(line) { |time, record|
          if time && record
            record[@path_key] ||= tail_watcher.path unless @path_key.nil?
            es.add(time, record)
          else
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
      def initialize(path, rotate_wait, pe, log, read_from_head, enable_watch_timer, read_lines_limit, update_watcher, line_buffer_timer_flusher, &receive_lines)
        @path = path
        @rotate_wait = rotate_wait
        @pe = pe || MemoryPositionEntry.new
        @read_from_head = read_from_head
        @enable_watch_timer = enable_watch_timer
        @read_lines_limit = read_lines_limit
        @receive_lines = receive_lines
        @update_watcher = update_watcher

        @stat_trigger = StatWatcher.new(path, log, &method(:on_notify))
        @timer_trigger = nil

        @rotate_handler = RotateHandler.new(path, log, &method(:on_rotate))
        @io_handler = nil
        @log = log

        @line_buffer_timer_flusher = line_buffer_timer_flusher
      end

      attr_reader :path
      attr_reader :stat_trigger, :enable_watch_timer
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
        @timer_trigger.detach if @enable_watch_timer && @timer_trigger.attached?
        @stat_trigger.detach if @stat_trigger.attached?
      end

      def close(close_io = true)
        if close_io && @io_handler
          @io_handler.on_notify
          @io_handler.close
        end
        detach
      end

      def on_notify
        @rotate_handler.on_notify if @rotate_handler
        @line_buffer_timer_flusher.on_notify(self) if @line_buffer_timer_flusher
        return unless @io_handler
        @io_handler.on_notify
      end

      def on_rotate(io)
        if @io_handler == nil
          if io
            # first time
            stat = io.stat
            fsize = stat.size
            inode = stat.ino

            last_inode = @pe.read_inode
            if inode == last_inode
              # rotated file has the same inode number with the last file.
              # assuming following situation:
              #   a) file was once renamed and backed, or
              #   b) symlink or hardlink to the same file is recreated
              # in either case, seek to the saved position
              pos = @pe.read_pos
            elsif last_inode != 0
              # this is FilePositionEntry and fluentd once started.
              # read data from the head of the rotated file.
              # logs never duplicate because this file is a rotated new file.
              pos = 0
              @pe.update(inode, pos)
            else
              # this is MemoryPositionEntry or this is the first time fluentd started.
              # seek to the end of the any files.
              # logs may duplicate without this seek because it's not sure the file is
              # existent file or rotated new file.
              pos = @read_from_head ? 0 : fsize
              @pe.update(inode, pos)
            end
            io.seek(pos)

            @io_handler = IOHandler.new(io, @pe, @log, @read_lines_limit, &method(:wrap_receive_lines))
          else
            @io_handler = NullIOHandler.new
          end
        else
          log_msg = "detected rotation of #{@path}"
          log_msg << "; waiting #{@rotate_wait} seconds" if @io_handler.io  # wait rotate_time if previous file is exist
          @log.info log_msg

          if io
            stat = io.stat
            inode = stat.ino
            if inode == @pe.read_inode # truncated
              @pe.update_pos(stat.size)
              io_handler = IOHandler.new(io, @pe, @log, @read_lines_limit, &method(:wrap_receive_lines))
              @io_handler.close
              @io_handler = io_handler
            elsif @io_handler.io.nil? # There is no previous file. Reuse TailWatcher
              @pe.update(inode, io.pos)
              io_handler = IOHandler.new(io, @pe, @log, @read_lines_limit, &method(:wrap_receive_lines))
              @io_handler = io_handler
            else # file is rotated and new file found
              @update_watcher.call(@path, swap_state(@pe))
            end
          else # file is rotated and new file not found
            # Clear RotateHandler to avoid duplicated file watch in same path.
            @rotate_handler = nil
            @update_watcher.call(@path, swap_state(@pe))
          end
        end
      end

      def swap_state(pe)
        # Use MemoryPositionEntry for rotated file temporary
        mpe = MemoryPositionEntry.new
        mpe.update(pe.read_inode, pe.read_pos)
        @pe = mpe
        @io_handler.pe = mpe # Don't re-create IOHandler because IOHandler has an internal buffer.

        pe # This pe will be updated in on_rotate after TailWatcher is initialized
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
          # TODO log?
          @log.error $!.to_s
          @log.error_backtrace
        end
      end

      class IOHandler
        def initialize(io, pe, log, read_lines_limit, first = true, &receive_lines)
          @log = log
          @log.info "following tail of #{io.path}" if first
          @io = io
          @pe = pe
          @read_lines_limit = read_lines_limit
          @receive_lines = receive_lines
          @buffer = ''.force_encoding('ASCII-8BIT')
          @iobuf = ''.force_encoding('ASCII-8BIT')
          @lines = []
        end

        attr_reader :io
        attr_accessor :pe

        def on_notify
          begin
            read_more = false

            if @lines.empty?
              begin
                while true
                  if @buffer.empty?
                    @io.readpartial(2048, @buffer)
                  else
                    @buffer << @io.readpartial(2048, @iobuf)
                  end
                  while line = @buffer.slice!(/.*?\n/m)
                    @lines << line
                  end
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
              if @receive_lines.call(@lines)
                @pe.update_pos(@io.pos - @buffer.bytesize)
                @lines.clear
              else
                read_more = false
              end
            end
          end while read_more

        rescue
          @log.error $!.to_s
          @log.error_backtrace
          close
        end

        def close
          @io.close unless @io.closed?
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
      end

      class RotateHandler
        def initialize(path, log, &on_rotate)
          @path = path
          @inode = nil
          @fsize = -1  # first
          @on_rotate = on_rotate
          @log = log
        end

        def on_notify
          begin
            stat = Fluent::FileWrapper.stat(@path)
            inode = stat.ino
            fsize = stat.size
          rescue Errno::ENOENT
            # moved or deleted
            inode = nil
            fsize = 0
          end

          begin
            if @inode != inode || fsize < @fsize
              # rotated or truncated
              begin
                io = Fluent::FileWrapper.open(@path)
              rescue Errno::ENOENT
              end
              @on_rotate.call(io)
            end
            @inode = inode
            @fsize = fsize
          end

        rescue
          @log.error $!.to_s
          @log.error_backtrace
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

      def initialize(file, map, last_pos)
        @file = file
        @map = map
        @last_pos = last_pos
      end

      def [](path)
        if m = @map[path]
          return m
        end

        @file.pos = @last_pos
        @file.write path
        @file.write "\t"
        seek = @file.pos
        @file.write "0000000000000000\t0000000000000000\n"
        @last_pos = @file.pos

        @map[path] = FilePositionEntry.new(@file, seek)
      end

      def self.parse(file)
        compact(file)

        map = {}
        file.pos = 0
        file.each_line {|line|
          m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
          next unless m
          path = m[1]
          seek = file.pos - line.bytesize + path.bytesize + 1
          map[path] = FilePositionEntry.new(file, seek)
        }
        new(file, map, file.pos)
      end

      # Clean up unwatched file entries
      def self.compact(file)
        file.pos = 0
        existent_entries = file.each_line.map { |line|
          m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
          next unless m
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

      def initialize(file, seek)
        @file = file
        @seek = seek
      end

      def update(ino, pos)
        @file.pos = @seek
        @file.write "%016x\t%016x" % [pos, ino]
      end

      def update_pos(pos)
        @file.pos = @seek
        @file.write "%016x" % pos
      end

      def read_inode
        @file.pos = @seek + INO_OFFSET
        raw = @file.read(16)
        raw ? raw.to_i(16) : 0
      end

      def read_pos
        @file.pos = @seek
        raw = @file.read(16)
        raw ? raw.to_i(16) : 0
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
