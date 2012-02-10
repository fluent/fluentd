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


class TailInput < Input
  Plugin.register_input('tail', self)

  def initialize
    super
    @paths = []
  end

  config_param :path, :string
  config_param :tag, :string
  config_param :rotate_wait, :time, :default => 5
  config_param :pos_file, :string, :default => nil

  def configure(conf)
    super

    @paths = @path.split(',').map {|path| path.strip }
    if @paths.empty?
      raise ConfigError, "tail: 'path' parameter is required on tail input"
    end

    if @pos_file
      @pf_file = File.open(@pos_file, File::RDWR|File::CREAT)
      @pf_file.sync = true
      @pf = PositionFile.parse(@pf_file)
    else
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
    @loop = Coolio::Loop.new
    @tailers = @paths.map {|path|
      pe = @pf ? @pf[path] : NullPositionEntry.instance
      Tailer.new(@loop, path, @rotate_wait, pe, method(:receive_lines))
    }
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @thread.join
    @pf_file.close if @pf_file
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  def receive_lines(lines)
    es = MultiEventStream.new
    lines.each {|line|
      begin
        line.rstrip!  # remove \n
        time, record = parse_line(line)
        if time && record
          es.add(time, record)
        end
      rescue
        $log.warn line.dump, :error=>$!.to_s
        $log.debug_backtrace
      end
    }

    unless es.empty?
      Engine.emit_stream(@tag, es)
    end
  end

  def parse_line(line)
    return @parser.parse(line)
  end

  class Tailer
    def initialize(loop, path, rotate_wait, pe, receive_lines)
      @loop = loop
      @path = path
      @rotate_wait = rotate_wait
      @pe = pe
      @receive_lines = receive_lines

      @rotate_queue = []
      @rotate_timer = nil
      @io_handler = nil

      @rh = RotateHandler.new(path, method(:rotate))
      @rh.check  # invoke rotate
      @rh.on_rotate = method(:on_rotate)
      @rh.attach(@loop)
    end

    def on_rotate(io)
      return if @rotate_queue.include?(io)
      $log.info "detected rotation of #{@path}; waiting #{@rotate_wait} seconds"
      @rotate_queue.push(io)

      # start rotate_timer
      unless @rotate_timer
        @rotate_timer = RotateTimer.new(@rotate_wait, method(:on_rotate_timer))
        @rotate_timer.attach(@loop)
      end
    end

    def on_rotate_timer
      io = @rotate_queue.first
      rotate(io, 0)
    end

    def rotate(io, start_pos=nil)
      # start_pos is nil if first
      io_handler = IOHandler.new(io, start_pos, @pe, @receive_lines)

      if @io_handler
        @io_handler.close
        @io_handler = nil
      end
      io_handler.attach(@loop)
      @io_handler = io_handler
      @rotate_queue.shift

      if @rotate_queue.empty?
        @rotate_timer.detach if @rotate_timer
        @rotate_timer = nil
      end
    end

    def shutdown
      @rotate_queue.reject! {|io|
        io.close
        true
      }
      if @io_handler
        @io_handler.close
        @io_handler = nil
      end
      if @rotate_timer
        @rotate_timer.detach
        @rotate_timer = nil
      end
    end
  end

  class RotateHandler
    def initialize(path, on_rotate)
      @path = path
      @inode = nil
      @fsize = 0
      @on_rotate = on_rotate
      @path = path
      @stat_watcher = Stat.new(self, @path)
      @timer_watcher = Timer.new(self, 1)
    end

    attr_accessor :on_rotate

    def check
      begin
        io = File.open(@path)
      rescue Errno::ENOENT
        # moved or deleted
        @inode = nil
        @fsize = 0
        return
      end

      begin
        stat = io.stat
        inode = stat.ino
        fsize = stat.size

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
      $log.error $!.to_s
      $log.error_backtrace
    end

    def attach(loop)
      @stat_watcher.attach(loop)
      @timer_watcher.attach(loop)
    end

    def detach
      @stat_watcher.detach
      @timer_watcher.detach
    end

    def attached?
      @stat_watcher.attached?
    end

    class Stat < Coolio::StatWatcher
      def initialize(h, path)
        @h = h
        super(path)
      end

      def on_change(prev, cur)
        @h.call
      end
    end

    class Timer < Coolio::TimerWatcher
      def initialize(h, interval)
        @h = h
        super(interval, true)
      end

      def on_timer
        @h.check
      end
    end
  end

  class RotateTimer < Coolio::TimerWatcher
    def initialize(interval, callback)
      super(interval, true)
      @callback = callback
    end

    def on_timer
      @callback.call
    rescue
      # TODO log?
    end
  end

  class IOHandler < Coolio::IOWatcher
    def initialize(io, start_pos, pe, receive_lines)
      $log.info "following tail of #{io.path}"
      @io = io
      @pe = pe
      @receive_lines = receive_lines

      if start_pos
        # rotated
        @pos = start_pos

      else
        # first time
        stat = io.stat
        fsize = stat.size
        inode = stat.ino
        if inode == @pe.read_inode
          # seek to the saved position
          @pos = @pe.read_pos
        else
          # seek to the end of the file.
          # logs never duplicate but may be lost if fluentd is down.
          @pos = fsize
          @pe.update(inode, @pos)
        end
      end

      io.seek(@pos)

      @buffer = ''
      super(io)
    end

    def on_readable
      lines = []

      while line = @io.gets
        @buffer << line
        @pos = @io.pos
        unless @buffer[@buffer.length-1] == ?\n
          break
        end
        lines << line
      end

      @pe.update_pos(@pos)
      @receive_lines.call(lines) unless lines.empty?
    rescue
      $log.error $!.to_s
      $log.error_backtrace
      close
    end

    def close
      detach if attached?
      @io.close unless @io.closed?
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

      @map[path] = PositionEntry.new(@file, seek)
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
        map[path] = PositionEntry.new(file, seek)
      }
      new(file, map, file.pos)
    end
  end

  # pos               inode
  # ffffffffffffffff\tffffffff\n
  class PositionEntry
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
      @file.read(8).to_i(16)
    end

    def read_pos
      @file.pos = @seek
      @file.read(16).to_i(16)
    end
  end

  class NullPositionEntry
    require 'singleton'
    include Singleton
    def update(ino, pos)
    end
    def update_pos(pos)
    end
    def read_pos
      0
    end
    def read_inode
      0
    end
  end
end


end

