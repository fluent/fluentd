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

      @multiline_mode = conf['format'] =~ /multiline/
      @receive_handler = if @multiline_mode
                           method(:parse_multilines)
                         else
                           method(:parse_singleline)
                         end
    end

    def configure_parser(conf)
      @parser = Plugin.new_parser(conf['format'])
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
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT, DEFAULT_FILE_PERMISSION)
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
      tw = TailWatcher.new(path, @rotate_wait, pe, log, @read_from_head, method(:update_watcher), &method(:receive_lines))
      tw.attach(@loop)
      tw
    end

    def start_watchers(paths)
      paths.each { |path|
        pe = nil
        if @pf
          pe = @pf[path]
          if @read_from_head && pe.read_inode.zero?
            begin
              pe.update(File::Stat.new(path).ino, 0)
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
      closer = TailWatcher::Closer.new(@rotate_wait, tw, log, &method(:close_watcher))
      closer.attach(@loop)
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
            router.emit(tag, time, record)
          else
            log.warn "got incomplete line at shutdown from #{tw.path}: #{lb.inspect}"
          end
        }
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
          router.emit_stream(tag, es)
        rescue
          # ignore errors. Engine shows logs and backtraces.
        end
      end
    end

    def convert_line_to_event(line, es)
      begin
        line.chomp!  # remove \n
        @parser.parse(line) { |time, record|
          if time && record
            es.add(time, record)
          else
            log.warn "pattern not match: #{line.inspect}"
          end
        }
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
      if @parser.has_firstline?
        lines.each { |line|
          if @parser.firstline?(line)
            if lb
              convert_line_to_event(lb, es)
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
              convert_line_to_event(lb, es)
              lb = ''
            end
          }
        end
      end
      tail_watcher.line_buffer = lb
      es
    end

    class TailWatcher
      def initialize(path, rotate_wait, pe, log, read_from_head, update_watcher, &receive_lines)
        @path = path
        @rotate_wait = rotate_wait
        @pe = pe || MemoryPositionEntry.new
        @read_from_head = read_from_head
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

      def close(close_io = true)
        if close_io && @io_handler
          @io_handler.on_notify
          @io_handler.close
        end
        detach
      end

      def on_notify
        @rotate_handler.on_notify if @rotate_handler
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

            @io_handler = IOHandler.new(io, @pe, @log, &method(:wrap_receive_lines))
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
              io_handler = IOHandler.new(io, @pe, @log, &method(:wrap_receive_lines))
              @io_handler.close
              @io_handler = io_handler
            elsif @io_handler.io.nil? # There is no previous file. Reuse TailWatcher
              @pe.update(inode, io.pos)
              io_handler = IOHandler.new(io, @pe, @log, &method(:wrap_receive_lines))
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
        ensure
          detach
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
                if @buffer.empty?
                  @io.read_nonblock(2048, @buffer)
                else
                  @buffer << @io.read_nonblock(2048, @iobuf)
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
            io = File.open(@path)
            stat = io.stat
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
        @pf_file = File.open(@pos_file, File::RDWR|File::CREAT, DEFAULT_FILE_PERMISSION)
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
          router.emit_stream(@tag, es)
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
            stat = io.stat
            inode = stat.ino
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
            stat = io.stat
            fsize = stat.size
            inode = stat.ino

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
                if @buffer.empty?
                  @io.read_nonblock(2048, @buffer)
                else
                  @buffer << @io.read_nonblock(2048, @iobuf)
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
            io = File.open(@path)
            stat = io.stat
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
        @file.write "0000000000000000\t00000000\n"
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
      POS_SIZE = 16
      INO_OFFSET = 17
      INO_SIZE = 8
      LN_OFFSET = 25
      SIZE = 26

      def initialize(file, seek)
        @file = file
        @seek = seek
      end

      def update(ino, pos)
        @file.pos = @seek
        @file.write "%016x\t%08x" % [pos, ino]
        @inode = ino
      end

      def update_pos(pos)
        @file.pos = @seek
        @file.write "%016x" % pos
      end

      def read_inode
        @file.pos = @seek + INO_OFFSET
        raw = @file.read(8)
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
