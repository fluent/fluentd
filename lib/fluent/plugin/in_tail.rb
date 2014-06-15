#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent
  class NewTailInput < Input
    Plugin.register_input('tail', self)

    def initialize
      super
      @paths = []
      @tails = {}
    end

    config_param :path, :string
    config_param :tag, :string
    config_param :rotate_wait, :time, :default => 5
    config_param :pos_file, :string, :default => nil
    config_param :read_from_head, :bool, :default => false
    config_param :refresh_interval, :time, :default => 60

    attr_reader :paths

    def configure(conf)
      super

      @paths = @path.split(',').map {|path| path.strip }
      if @paths.empty?
        raise ConfigError, "tail: 'path' parameter is required on tail input"
      end

      unless @pos_file
        $log.warn "'pos_file PATH' parameter is not set to a 'tail' source."
        $log.warn "this parameter is highly recommended to save the position to resume tailing."
      end

      configure_parser(conf)
      configure_tag

      @multiline_mode = conf['format'] == 'multiline'
      @receive_handler = if @multiline_mode
                           method(:parse_multilines)
                         else
                           method(:parse_singleline)
                         end
    end

    def configure_parser(conf)
      @parser = TextParser.new
      @parser.configure(conf)
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

    def start
      if @pos_file
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT|File::BINARY, DEFAULT_FILE_PERMISSION)
        @pf_file.sync = true
        @pf = PositionFile.parse(@pf_file)
      end

      @loop = Coolio::Loop.new
      refresh_watchers

      @refresh_trigger = TailWatcher::TimerWatcher.new(@refresh_interval, true, log, &method(:refresh_watchers))
      @refresh_trigger.attach(@loop)
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @refresh_trigger.detach if @refresh_trigger && @refresh_trigger.attached?

      stop_watchers(@tails.keys, true)
      @loop.stop rescue nil # when all watchers are detached, `stop` raises RuntimeError. We can ignore this exception.
      @thread.join
      @pf_file.close if @pf_file
    end

    def expand_paths
      date = Time.now
      paths = []
      @paths.each { |path|
        path = date.strftime(path)
        if path.include?('*')
          paths += Dir.glob(path)
        else
          # When file is not created yet, Dir.glob returns an empty array. So just add when path is static.
          paths << path
        end
      }
      paths
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
      tw = TailWatcher.new(path, @rotate_wait, pe, log, method(:update_watcher), &method(:receive_lines))
      tw.attach(@loop)
      tw
    end

    def start_watchers(paths)
      paths.each { |path|
        pe = nil
        if @pf
          pe = @pf[path]
          if @read_from_head && pe.read_inode.zero?
            pe.update(File::Stat.new(path).ino, 0)
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
            close_watcher(tw)
          else
            close_watcher_after_rotate_wait(tw)
          end
        end
      }
    end

    # refresh_watchers calls @tails.keys so we don't use stop_watcher -> start_watcher sequence for safety.
    def update_watcher(path, pe)
      rotated_tw = @tails[path]
      @tails[path] = setup_watcher(path, pe)
      close_watcher_after_rotate_wait(rotated_tw) if rotated_tw
    end

    def close_watcher(tw)
      tw.close
      flush_buffer(tw)
      if tw.unwatched && @pf
        @pf[tw.path].update_pos(PositionFile::UNWATCHED_POSITION)
      end
    end

    def close_watcher_after_rotate_wait(tw)
      closer = TailWatcher::Closer.new(@rotate_wait, tw, log, &method(:close_watcher))
      closer.attach(@loop)
    end

    def flush_buffer(tw)
      if lb = tw.line_buffer
        lb.chomp!
        time, record = parse_line(lb)
        if time && record
          tag = if @tag_prefix || @tag_suffix
                  @tag_prefix + tail_watcher.tag + @tag_suffix
                else
                  @tag
                end
          Engine.emit(tag, time, record)
        else
          log.warn "got incomplete line at shutdown from #{tw.path}: #{lb.inspect}"
        end
      end
    end

    def run
      @loop.run
    rescue
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
    end

    def receive_lines(lines, tail_watcher)
      es = @receive_handler.call(lines, tail_watcher)
      unless es.empty?
        tag = if @tag_prefix || @tag_suffix
                @tag_prefix + tail_watcher.tag + @tag_suffix
              else
                @tag
              end
        begin
          Engine.emit_stream(tag, es)
        rescue
          # ignore errors. Engine shows logs and backtraces.
        end
      end
    end

    def parse_line(line)
      return @parser.parse(line)
    end

    def convert_line_to_event(line, es)
      begin
        line.chomp!  # remove \n
        time, record = parse_line(line)
        if time && record
          es.add(time, record)
        else
          log.warn "pattern not match: #{line.inspect}"
        end
      rescue => e
        log.warn line.dump, :error => e.to_s
        log.debug_backtrace(e.backtrace)
      end
    end

    def parse_singleline(lines, tail_watcher)
      es = MultiEventStream.new
      lines.each { |line|
        convert_line_to_event(line, es)
      }
      es
    end

    def parse_multilines(lines, tail_watcher)
      lb = tail_watcher.line_buffer
      es = MultiEventStream.new
      lines.each { |line|
        if @parser.parser.firstline?(line)
          if lb
            convert_line_to_event(lb, es)
          end
          lb = line
        else
          if lb.nil?
            log.warn "got incomplete line before first line from #{tail_watcher.path}: #{lb.inspect}"
          else
            lb << line
          end
        end
      }
      tail_watcher.line_buffer = lb
      es
    end

    class TailWatcher
      def initialize(path, rotate_wait, pe, log, update_watcher, &receive_lines)
        @path = path
        @rotate_wait = rotate_wait
        @pe = pe || MemoryPositionEntry.new
        @receive_lines = receive_lines
        @update_watcher = update_watcher

        @timer_trigger = TimerWatcher.new(1, true, log, &method(:on_notify))
        @stat_trigger = StatWatcher.new(path, log, &method(:on_notify))

        @rotate_handler = RotateHandler.new(path, log, &method(:on_rotate))
        @io_handler = nil
        @log = log
      end

      attr_reader :path
      attr_accessor :line_buffer
      attr_accessor :unwatched  # This is used for removing position entry from PositionFile

      def tag
        @parsed_tag ||= @path.tr('/', '.').gsub(/\.+/, '.').gsub(/^\./, '')
      end

      def wrap_receive_lines(lines)
        @receive_lines.call(lines, self)
      end

      def attach(loop)
        @timer_trigger.attach(loop)
        @stat_trigger.attach(loop)
        on_notify
      end

      def detach
        @timer_trigger.detach if @timer_trigger.attached?
        @stat_trigger.detach if @stat_trigger.attached?
      end

      def close
        if @io_handler
          @io_handler.on_notify
          @io_handler.close
        end
        detach
      end

      def on_notify
        @rotate_handler.on_notify
        return unless @io_handler
        @io_handler.on_notify
      end

      def on_rotate(io)
        if @io_handler == nil
          if io
            # first time
            unless $platformwin
              stat = io.stat
              fsize = stat.size
              inode = stat.ino
            else
              fsize = io.size
              inode = io.ino
            end

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
              pos = fsize
              @pe.update(inode, pos)
            end
            io.seek(pos)

            @io_handler = IOHandler.new(io, @pe, @log, &method(:wrap_receive_lines))
          else
            @io_handler = NullIOHandler.new
          end
        else
          log_msg = "detected rotation of #{@path}"
          log_msg << "; waiting #{@rotate_wait} seconds" if @io_handler.io  # wait rotate_time if previous file is exist
          @log.info log_msg

          if io
            unless $platformwin
              stat = io.stat
              fsize = stat.size
              inode = stat.ino
            else
              fsize = io.size
              inode = io.ino
            end
            if inode == @pe.read_inode # truncated
              @pe.update_pos(fsize)
              io_handler = IOHandler.new(io, @pe, @log, &method(:wrap_receive_lines))
              @io_handler.close
              @io_handler = io_handler
            elsif @io_handler.io.nil? # There is no previous file. Reuse TailWatcher
              @pe.update(inode, io.pos)
              io_handler = IOHandler.new(io, @pe, @log, &method(:wrap_receive_lines))
              @io_handler = io_handler
            else
              @update_watcher.call(@path, swap_state(@pe))
            end
          else
            @io_handler.close
            @io_handler = NullIOHandler.new
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
      end

      class TimerWatcher < Coolio::TimerWatcher
        def initialize(interval, repeat, log, &callback)
          @callback = callback
          @log = log
          super(interval, repeat)
        end

        def on_timer
          @callback.call
        rescue
          # TODO log?
          @log.error $!.to_s
          @log.error_backtrace
        end
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

      class Closer < Coolio::TimerWatcher
        def initialize(interval, tw, log, &callback)
          @callback = callback
          @tw = tw
          @log = log
          super(interval, false)
        end

        def on_timer
          @callback.call(@tw)
        rescue => e
          @log.error e.to_s
          @log.error_backtrace(e.backtrace)
        end
      end

      MAX_LINES_AT_ONCE = 1000

      class IOHandler
        def initialize(io, pe, log, first = true, &receive_lines)
          @log = log
          @log.info "following tail of #{io.path}" if first
          @io = io
          @pe = pe
          @receive_lines = receive_lines
          @buffer = ''.force_encoding('ASCII-8BIT')
          @iobuf = ''.force_encoding('ASCII-8BIT')
        end

        attr_reader :io
        attr_accessor :pe

        def on_notify
          begin
            lines = []
            read_more = false

            begin
              while true
                unless $platformwin
                  if @buffer.empty?
                    @io.read_nonblock(2048, @buffer)
                  else
                    @buffer << @io.read_nonblock(2048, @iobuf)
                  end
                else
                  if @buffer.empty?
                    @io.read(2048,@buffer)
                  else
                    @buffer << @io.read(2048, @iobuf)                    
                  end
                end
                while line = @buffer.slice!(/.*?\n/m)
                  lines << line
                end
                if lines.size >= MAX_LINES_AT_ONCE
                  # not to use too much memory in case the file is very large
                  read_more = true
                  break
                end
              end
            rescue EOFError
            end

            unless lines.empty?
              @receive_lines.call(lines)
              @pe.update_pos(@io.pos - @buffer.bytesize)
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
            unless $platformwin
              io = File.open(@path)
              stat = io.stat
              inode = stat.ino
              fsize = stat.size
            else
              io = Win32File.open(@path)
              inode = io.ino
              fsize = io.size
            end
          rescue Errno::ENOENT
            # moved or deleted
            inode = nil
            fsize = 0
          end

          begin
            if @inode != inode || fsize < @fsize
              # rotated or truncated
              @on_rotate.call(io)
              io = nil
            end

            @inode = inode
            @fsize = fsize
          ensure
            io.close if io
          end

        rescue
          @log.error $!.to_s
          @log.error_backtrace
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
        unless $platformwin
          @file.write "0000000000000000\t00000000\n"
        else
          @file.write "0000000000000000\t000000000000000000000000\n"
        end
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
          pos = m[2].to_i(16)
          ino = m[3].to_i(16)
          seek = file.pos - line.bytesize + path.bytesize + 1
          map[path] = FilePositionEntry.new(file, seek)
        }
        new(file, map, file.pos)
      end

      # Clean up unwatched file entries
      def self.compact(file)
        file.pos = 0
        existent_entries = file.each_line.select { |line|
          m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
          next unless m
          pos = m[2].to_i(16)
          pos == UNWATCHED_POSITION ? nil : line
        }

        file.pos = 0
        file.truncate(0)
        file.write(existent_entries.join)
      end
    end

    # pos               inode
    # ffffffffffffffff\tffffffff\n
    class FilePositionEntry
      unless $platformwin
        POS_SIZE = 16
        INO_OFFSET = 17
        INO_SIZE = 8
        LN_OFFSET = 25
        SIZE = 26
      else
        POS_SIZE = 16
        INO_OFFSET = 17
        INO_SIZE = 24
        LN_OFFSET = 31
        SIZE = 32
      end

      def initialize(file, seek)
        @file = file
        @seek = seek
      end

      def update(ino, pos)
        @file.pos = @seek
        unless $platformwin
          @file.write "%016x\t%08x" % [pos, ino]
        else
          @file.write "%016x\t%024x" % [pos, ino]
        end
      end

      def update_pos(pos)
        @file.pos = @seek
        @file.write "%016x" % pos
      end

      def read_inode
        @file.pos = @seek + INO_OFFSET
        raw = @file.read(INO_SIZE)
        raw ? raw.to_i(16) : 0
      end

      def read_pos
        @file.pos = @seek
        raw = @file.read(POS_SIZE)
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

  # This TailInput is for existence plugins which extends old in_tail
  # This class will be removed after release v1.
  class TailInput < Input
    def initialize
      super
      @paths = []
    end

    config_param :path, :string
    config_param :tag, :string
    config_param :rotate_wait, :time, :default => 5
    config_param :pos_file, :string, :default => nil

    attr_reader :paths

    def configure(conf)
      super

      @paths = @path.split(',').map {|path| path.strip }
      if @paths.empty?
        raise ConfigError, "tail: 'path' parameter is required on tail input"
      end

      unless @pos_file
        $log.warn "'pos_file PATH' parameter is not set to a 'tail' source."
        $log.warn "this parameter is highly recommended to save the position to resume tailing."
      end

      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = TextParser.new
      @parser.configure(conf)
    end

    def start
      if @pos_file
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT|File::BINARY, DEFAULT_FILE_PERMISSION)
        @pf_file.sync = true
        @pf = PositionFile.parse(@pf_file)
      end

      @loop = Coolio::Loop.new
      @tails = @paths.map {|path|
        pe = @pf ? @pf[path] : MemoryPositionEntry.new
        tw = TailWatcher.new(path, @rotate_wait, pe, &method(:receive_lines))
        tw.log = log
        tw
      }
      @tails.each {|tail|
        tail.attach(@loop)
      }
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @tails.each {|tail|
        tail.close
      }
      @loop.stop
      @thread.join
      @pf_file.close if @pf_file
    end

    def run
      @loop.run
    rescue
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
    end

    def receive_lines(lines)
      es = MultiEventStream.new
      lines.each {|line|
        begin
          line.chomp!  # remove \n
          time, record = parse_line(line)
          if time && record
            es.add(time, record)
          else
            log.warn "pattern not match: #{line.inspect}"
          end
        rescue
          log.warn line.dump, :error=>$!.to_s
          log.debug_backtrace
        end
      }

      unless es.empty?
        begin
          Engine.emit_stream(@tag, es)
        rescue
          # ignore errors. Engine shows logs and backtraces.
        end
      end
    end

    def parse_line(line)
      return @parser.parse(line)
    end

    class TailWatcher
      def initialize(path, rotate_wait, pe, &receive_lines)
        @path = path
        @rotate_wait = rotate_wait
        @pe = pe || MemoryPositionEntry.new
        @receive_lines = receive_lines

        @rotate_queue = []

        @timer_trigger = TimerWatcher.new(1, true, &method(:on_notify))
        @stat_trigger = StatWatcher.new(path, &method(:on_notify))

        @rotate_handler = RotateHandler.new(path, &method(:on_rotate))
        @io_handler = nil
        @log = $log
      end

      # We use accessor approach to assign each logger, not passing log object at initialization,
      # because several plugins depend on these internal classes.
      # This approach avoids breaking plugins with new log_level option.
      attr_accessor :log

      def log=(logger)
        @log = logger
        @timer_trigger.log = logger
        @stat_trigger.log = logger
        @rotate_handler.log = logger
      end

      def attach(loop)
        @timer_trigger.attach(loop)
        @stat_trigger.attach(loop)
        on_notify
      end

      def detach
        @timer_trigger.detach if @timer_trigger.attached?
        @stat_trigger.detach if @stat_trigger.attached?
      end

      def close
        @rotate_queue.reject! {|req|
          req.io.close
          true
        }
        detach
      end

      def on_notify
        @rotate_handler.on_notify
        return unless @io_handler
        @io_handler.on_notify

        # proceeds rotate queue
        return if @rotate_queue.empty?
        @rotate_queue.first.tick

        while @rotate_queue.first.ready?
          if io = @rotate_queue.first.io
            unless $platformwin
              stat = io.stat
              inode = stat.ino
            else
              inode = io.ino
            end
            
            if inode == @pe.read_inode
              # rotated file has the same inode number with the last file.
              # assuming following situation:
              #   a) file was once renamed and backed, or
              #   b) symlink or hardlink to the same file is recreated
              # in either case, seek to the saved position
              pos = @pe.read_pos
            else
              pos = io.pos
            end
            @pe.update(inode, pos)
            io_handler = IOHandler.new(io, @pe, log, &@receive_lines)
          else
            io_handler = NullIOHandler.new
          end
          @io_handler.close
          @io_handler = io_handler
          @rotate_queue.shift
          break if @rotate_queue.empty?
        end
      end

      def on_rotate(io)
        if @io_handler == nil
          if io
            # first time
            unless $platformwin
              stat = io.stat
              fsize = stat.size
              inode = stat.ino
            else
              fsize = io.size
              inode = io.ino
            end
            last_inode = @pe.read_inode
            if inode == last_inode
              # seek to the saved position
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
              pos = fsize
              @pe.update(inode, pos)
            end
            io.seek(pos)

            @io_handler = IOHandler.new(io, @pe, log, &@receive_lines)
          else
            @io_handler = NullIOHandler.new
          end

        else
          if io && @rotate_queue.find {|req| req.io == io }
            return
          end
          last_io = @rotate_queue.empty? ? @io_handler.io : @rotate_queue.last.io
          if last_io == nil
            log.info "detected rotation of #{@path}"
            # rotate imeediately if previous file is nil
            wait = 0
          else
            log.info "detected rotation of #{@path}; waiting #{@rotate_wait} seconds"
            wait = @rotate_wait
            wait -= @rotate_queue.first.wait unless @rotate_queue.empty?
          end
          @rotate_queue << RotationRequest.new(io, wait)
        end
      end

      class TimerWatcher < Coolio::TimerWatcher
        def initialize(interval, repeat, &callback)
          @callback = callback
          @log = $log
          super(interval, repeat)
        end

        attr_accessor :log

        def on_timer
          @callback.call
        rescue
          # TODO log?
          @log.error $!.to_s
          @log.error_backtrace
        end
      end

      class StatWatcher < Coolio::StatWatcher
        def initialize(path, &callback)
          @callback = callback
          @log = $log
          super(path)
        end

        attr_accessor :log

        def on_change(prev, cur)
          @callback.call
        rescue
          # TODO log?
          @log.error $!.to_s
          @log.error_backtrace
        end
      end

      class RotationRequest
        def initialize(io, wait)
          @io = io
          @wait = wait
        end

        attr_reader :io, :wait

        def tick
          @wait -= 1
        end

        def ready?
          @wait <= 0
        end
      end

      MAX_LINES_AT_ONCE = 1000

      class IOHandler
        def initialize(io, pe, log, &receive_lines)
          @log = log
          @log.info "following tail of #{io.path}"
          @io = io
          @pe = pe
          @receive_lines = receive_lines
          @buffer = ''.force_encoding('ASCII-8BIT')
          @iobuf = ''.force_encoding('ASCII-8BIT')
        end

        attr_reader :io

        def on_notify
          begin
            lines = []
            read_more = false

            begin
              while true
                unless $platformwin
                  if @buffer.empty?
                    @io.read_nonblock(2048, @buffer)
                  else
                    @buffer << @io.read_nonblock(2048, @iobuf)
                  end
                else
                  if @buffer.empty?
                    @io.read(2048,@buffer)
                  else
                    @buffer << @io.read(2048, @iobuf)                    
                  end
                end
                while line = @buffer.slice!(/.*?\n/m)
                  lines << line
                end
                if lines.size >= MAX_LINES_AT_ONCE
                  # not to use too much memory in case the file is very large
                  read_more = true
                  break
                end
              end
            rescue EOFError
            end

            unless lines.empty?
              @receive_lines.call(lines)
              @pe.update_pos(@io.pos - @buffer.bytesize)
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
        def initialize(path, &on_rotate)
          @path = path
          @inode = nil
          @fsize = -1  # first
          @on_rotate = on_rotate
          @log = $log
        end

        attr_accessor :log

        def on_notify
          begin
            unless $platformwin
              io = File.open(@path)
              stat = io.stat
              inode = stat.ino
              fsize = stat.size
            else
              io = Win32File.open(@path)
              inode = io.ino
              fsize = io.size
            end
          rescue Errno::ENOENT
            # moved or deleted
            inode = nil
            fsize = 0
          end

          begin
            if @inode != inode || fsize < @fsize
              # rotated or truncated
              @on_rotate.call(io)
              io = nil
            end

            @inode = inode
            @fsize = fsize
          ensure
            io.close if io
          end

        rescue
          @log.error $!.to_s
          @log.error_backtrace
        end
      end
    end


    class PositionFile
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
        unless $platformwin
          @file.write "0000000000000000\t00000000\n"
        else
          @file.write "0000000000000000\t000000000000000000000000\n"
        end
        @last_pos = @file.pos

        @map[path] = FilePositionEntry.new(@file, seek)
      end

      def self.parse(file)
        map = {}
        file.pos = 0
        file.each_line {|line|
          m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
          next unless m
          path = m[1]
          pos = m[2].to_i(16)
          ino = m[3].to_i(16)
          seek = file.pos - line.bytesize + path.bytesize + 1
          map[path] = FilePositionEntry.new(file, seek)
        }
        new(file, map, file.pos)
      end
    end

    # pos               inode
    # ffffffffffffffff\tffffffff\n
    class FilePositionEntry
    
      unless $platformwin
        POS_SIZE = 16
        INO_OFFSET = 17
        INO_SIZE = 8
        LN_OFFSET = 25
        SIZE = 26
      else
        POS_SIZE = 16
        INO_OFFSET = 17
        INO_SIZE = 24
        LN_OFFSET = 31
        SIZE = 32
      end

      def initialize(file, seek)
        @file = file
        @seek = seek
      end

      def update(ino, pos)
        @file.pos = @seek
        unless $platformwin
          @file.write "%016x\t%08x" % [pos, ino]
        else
          @file.write "%016x\t%024x" % [pos, ino]
        end
        @inode = ino
      end

      def update_pos(pos)
        @file.pos = @seek
        @file.write "%016x" % pos
      end

      def read_inode
        @file.pos = @seek + INO_OFFSET
        raw = @file.read(INO_SIZE)
        raw ? raw.to_i(16) : 0
      end

      def read_pos
        @file.pos = @seek
        raw = @file.read(POS_SIZE)
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
  
  #temporary code for win32 platform
  require 'Win32API'
  require 'fluent/win32api_constants.rb'
  class Win32File
    @@api_getlasterror = nil
    def initialize
      super
    end 

    def Win32File.open(path, *mode)
      @@api_getlasterror = Win32API.new('kernel32','GetLastError','v','i') unless @@api_getlasterror
      access = GENERIC_READ
      sharemode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
      creationdisposition = OPEN_EXISTING
      seektoend = false
      if mode.size > 0
         if mode[0] == "r"
           access = GENERIC_READ
           creationdisposition = OPEN_EXISTING
         elsif mode[0] == "r+"
           access = GENERIC_READ | GENERIC_WRITE
           creationdisposition = OPEN_ALWAYS
         elsif mode[0] == "w"
           access = GENERIC_WRITE
           creationdisposition = CREATE_ALWAYS
         elsif mode[0] == "w+"
           access = GENERIC_READ | GENERIC_WRITE
           creationdisposition = CREATE_ALWAYS
         elsif mode[0] == "a"
           access = GENERIC_WRITE
           creationdisposition = OPEN_ALWAYS
           seektoend = true
         elsif mode[0] == "a+"
           access = GENERIC_READ | GENERIC_WRITE
           creationdisposition = OPEN_ALWAYS
           seektoend = true
         else
           access = GENERIC_READ
           creationdisposition = OPEN_EXISTING
         end
         if mode.size > 1
           sharemode = mode[1]
         end
      end
      w32io = Win32Io.new
      hFile = w32io.createfile(path, access, sharemode, creationdisposition, FILE_ATTRIBUTE_NORMAL)
      dwerr = @@api_getlasterror.call
      if hFile == INVALID_HANDLE_VALUE
        if dwerr == ERROR_FILE_NOT_FOUND || dwerr == ERROR_ACCESS_DENIED
          raise SystemCallError.new(2)
        end
        return nil
      end
      if seektoend
        w32io.seek(0, IO::SEEK_END)
      end
      return w32io
    end
  end

  class Win32Io
    def initialize
      super
      @path = nil
      @file_handle = INVALID_HANDLE_VALUE
      @api_createfile = nil
      @api_closehandle = nil
      @api_setfilepointer = nil
      @api_getfilesize = nil
      @api_getlasterror = nil
      @api_readfile = nil
      @api_getfileinformationbyhandle = nil
      @currentPos = 0
      @file_size = 0
    end
    
    attr_reader :path, :file_handle
    
    def createfile(file_path, file_access, file_sharemode, file_creationdisposition, file_flagsandattrs)
      @path = file_path
      @api_createfile = Win32API.new('kernel32', 'CreateFile', %w(p i i i i i i), 'i') unless @api_createfile
      @file_handle = @api_createfile.call(file_path, file_access, file_sharemode, 0, file_creationdisposition, file_flagsandattrs, 0 )
    end
    
    def close
      unless @api_closehandle
        @api_closehandle = Win32API.new('kernel32', 'CloseHandle', 'i', 'i')
      end
      @api_closehandle.call(@file_handle)
      @file_handle = INVALID_HANDLE_VALUE
    end

    def closed?
      if @file_handle == INVALID_HANDLE_VALUE
         return false
      end
      return true
    end

    def seek(offset, whence = IO::SEEK_SET)
      @api_setfilepointer = Win32API.new('kernel32', 'SetFilePointer', %w(i i p i), 'i') unless @api_setfilepointer
      @api_getlasterror = Win32API.new('kernel32','GetLastError','v','i') unless @api_getlasterror
      case whence
      when IO::SEEK_CUR
        win32seek = FILE_CURRENT
      when IO::SEEK_END
        win32seek = FILE_END
      else
        win32seek = FILE_BEGIN
      end
      
      offsetlow = 0
      offsethi = 0
      if (offset > 0xFFFFFFFF)
        offsetlow = offset & 0x00000000FFFFFFFF
        offsethi = offset >> 32
      else
        offsetlow = offset
      end
      offsethi_p = [offsethi].pack("I")
      pos = @api_setfilepointer.call(@file_handle, offsetlow, offsethi_p, win32seek)
      err = @api_getlasterror.call
      if pos == -1 && err != 0
        return @currentPos
      end
      
      pos = [pos].pack("i").unpack("I")[0]
      offsethi = offsethi_p.unpack("I")[0]
      @currentPos = pos
      if offsethi > 0
        @currentPos = @currentPos + (offsethi << 32)
      end
      return @currentPos
    end

    def pos
      seek(0,IO::SEEK_CUR)
      return @currentPos
    end
    
    def size
      sizelow = 0
      sizehi_p = "\0" * 4
      @api_getfilesize = Win32API.new('kernel32', 'GetFileSize', %w(i p), 'i') unless @api_getfilesize
      @api_getlasterror = Win32API.new('kernel32','GetLastError','v','i') unless @api_getlasterror
      sizelow = @api_getfilesize.call(@file_handle, sizehi_p)
      err = @api_getlasterror.call
      if sizelow == -1 && err != 0
        return @file_size
      end
      sizelow = [sizelow].pack("i").unpack("I")[0]
      sizehi = sizehi_p.unpack("I")[0]
      @file_size = sizelow
      if sizehi > 0
        @file_size = @file_size + sizehi
      end
      return @file_size
    end
    
    def read(maxlen, outbuf = "")
      raise ArgumentError if maxlen < 0
      buf = "\0" * maxlen
      readbytes_p = "\0" * 4
      @api_readfile = Win32API.new('kernel32', 'ReadFile', %w(i p i p i), 'i') unless @api_readfile
      @api_getlasterror = Win32API.new('kernel32','GetLastError','v','i') unless @api_getlasterror
      ret = @api_readfile.call(@file_handle, buf, maxlen, readbytes_p, 0)
      err = @api_getlasterror.call
      if ret == 0
        raise IOError
      end
      readbytes = readbytes_p.unpack("I")[0]
      if readbytes == 0
        raise EOFError
      end
      buf_sliced = buf.slice(0, readbytes)
      outbuf << buf_sliced
      return buf_sliced
    end

    def ino
      @api_getfileinformationbyhandle = Win32API.new('kernel32', 'GetFileInformationByHandle', %w(i p), 'i') unless @api_getfileinformationbyhandle
      by_handle_file_information = '\0'*(4+8+8+8+4+4+4+4+4+4)   #72bytes
      ret = @api_getfileinformationbyhandle.call(@file_handle, by_handle_file_information)
      unless ret
        return 0
      end
      volumeserial = by_handle_file_information.unpack("I11Q1")[7]
      fileindex = by_handle_file_information.unpack("I11Q1")[11]
      return (volumeserial << 64) | fileindex
    end
  end
end
