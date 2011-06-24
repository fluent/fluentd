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

  require 'eventmachine-tail'

  def initialize
    @paths = []
    @parser = TextParser.new

    # TODO
    #@read_all = false
    #@last_record_file = nil
  end

  def configure(conf)
    if path = conf['path']
      @paths = path.split(',').map {|path| path.strip }
    else
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
    @paths.each {|path|
      # -1 = seek to the end of file.
      # logs never duplicate but may be lost if fluent is down
      Tailer.new(path, -1, &method(:receive_lines))
    }
  end

  def shutdown
  end

  def receive_lines(path, lines)
    array = []
    lines.each {|line|
      begin
        time, record = @parser.parse(line)
        if time && record
          array << Event.new(time, record)
        end
      rescue
        $log.warn "#{line.dump}: #{$!}"
        $log.debug_backtrace
      end
    }

    unless array.empty?
      Engine.emit_stream(@tag, ArrayEventStream.new(array))
    end
  end

  class Tailer < EventMachine::FileTail
    def initialize(path, startpos=-1, &callback)
      super(path, startpos)
      @callback = callback
      @buffer = BufferedTokenizer.new
    end

    def receive_data(data)
      lines = []
      @buffer.extract(data).each do |line|
        lines << line
      end
      EventMachine.defer {
        @callback.call(path, lines)
      }
    end

    # FIXME for event-machine-tail-0.6.1
    def read
      super
    rescue
      if $!.to_s == "closed stream"
        @file = nil
        schedule_reopen
      else
        raise
      end
    end

    def on_exception(e)
      if e.class == Errno::ENOENT
        $log.error "tail: #{path} does not exist"
      end
      # raise error
      super(e)
    end
  end
end


end

