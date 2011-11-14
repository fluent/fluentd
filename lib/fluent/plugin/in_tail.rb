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
    end

    configure_parser(conf)
  end

  def configure_parser(conf)
    @parser = TextParser.new
    @parser.configure(conf)
  end

  def start
    @loop = Coolio::Loop.new
    handlers = @paths.map {|path|
      $log.debug "following tail of #{path}"
      pe = @pf ? @pf[path] : NullPositionEntry.instance
      h = Handler.new(path, pe, method(:receive_lines))
      @loop.attach h
    }
    handlers.each {|h|
      h.on_change  # initialize call
    }
    @thread = Thread.new(&method(:run))
  end

  def shutdown
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

  class Handler < Coolio::StatWatcher
    def initialize(path, pe, callback)
      stat = File.lstat(path)
      @pe = pe
      @inode = stat.ino
      if @inode == @pe.read_inode
        # seek to the saved position
        @pos = @pe.read_pos
      else
        # seek to the end of file first.
        # logs never duplicate but may be lost if fluent is down.
        @pos = stat.size
        @pe.update(@inode, stat.size)
      end
      @buffer = ''
      @callback = callback
      super(path)
    end

    def on_change
      lines = []
      inode = nil

      File.open(path) {|f|
        stat = f.lstat
        inode = stat.ino

        if @inode != inode || stat.size < @pos
          # moved or deleted
          @pos = 0
        else
          f.seek(@pos)
        end

        line = f.gets
        unless line
          break
        end

        @buffer << line
        unless line[line.length-1] == ?\n
          @pos = f.pos
          break
        end

        lines << @buffer
        @buffer = ''

        while line = f.gets
          unless line[line.length-1] == ?\n
            @buffer = line
            break
          end
          lines << line
        end

        @pos = f.pos
      }

      if @inode != inode
        @pe.update(inode, @pos)
        @inode = inode
      else
        @pe.update_pos(@pos)
      end

      @callback.call(lines)

    rescue Errno::ENOENT
      # moved or deleted
      @pos = 0
    # TODO rescue
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

