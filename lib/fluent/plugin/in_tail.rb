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
    @paths = []
    @parser = TextParser.new
  end

  def configure(conf)
    if path = conf['path']
      @paths = path.split(',').map {|path| path.strip }
    end

    if @paths.empty?
      raise ConfigError, "tail: 'path' parameter is required on tail input"
    end

    @parser.configure(conf)

    if tag = conf['tag']
      @tag = tag
    else
      raise ConfigError, "tail: 'tag' parameter is required on tail input"
    end
  end

  def start
    @loop = Coolio::Loop.new
    @paths.each {|path|
      $log.debug "following tail of #{path}"
      @loop.attach Handler.new(path, method(:receive_lines))
    }
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.stop
    @thread.join
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  def receive_lines(lines)
    array = []
    lines.each {|line|
      begin
        time, record = @parser.parse(line)
        if time && record
          array << Event.new(time, record)
        end
      rescue
        $log.warn line.dump, :error=>$!.to_s
        $log.debug_backtrace
      end
    }

    unless array.empty?
      Engine.emit_stream(@tag, ArrayEventStream.new(array))
    end
  end

  # seek to the end of file first.
  # logs never duplicate but may be lost if fluent is down.
  class Handler < Coolio::StatWatcher
    def initialize(path, callback)
      @pos = File.stat(path).size
      @buffer = ''
      @callback = callback
      super(path)
    end

    def on_change
      lines = []

      File.open(path) {|f|
        if f.lstat.size < @pos
          # moved or deleted
          @pos = 0
        else
          f.seek(@pos)
        end

        line = f.gets
        unless line
          return
        end

        @buffer << line
        unless line[line.length-1] == ?\n
          @pos = f.pos
          return
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

      @callback.call(lines)

    rescue Errno::ENOENT
      # moved or deleted
      @pos = 0
    end
  end
end


end

