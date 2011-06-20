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

  TEMPLATES = {
    'apache' => [/^(?<host>.*?) .*? (?<user>.*?) \[(?<time>.*?)\] "(?<method>\S+?)(?: +(?<path>.*?) +\S*?)?" (?<code>.*?) (?<size>.*?)(?: "(?<referer>.*?)" "(?<agent>.*?)")?/, "%d/%b/%Y:%H:%M:%S %z"],
    'syslog' => [/^(?<time>.*? .*? .*?) (?<host>.*?) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?[^\:]*\: *(?<message>.*)/, "%b %d %H:%M:%S"],
  }

  def self.register_tempalte(name, regexp)
    TEMPLATES[name] = regexp
  end

  def self.get_template(name)
    TEMPLATES[name]
  end

  require 'eventmachine-tail'

  def initialize
    require 'time'
    @paths = []
    @regexp = nil
    @time_format = nil

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

    if format = conf['format']
      if format[0] == ?/ && format[format.length-1] == ?/
        # regexp
        begin
          @regexp = Regexp.new(format[1..-2])
          if @regexp.named_captures.empty?
            raise "No named captures"
          end
        rescue
          raise ConfigError, "tail: Invalid regexp '#{format[1..-2]}': #{$!}"
        end

      else
        # template
        @regexp, @time_format = TailInput.get_template(format)
        unless @regexp
          raise ConfigError, "tail: Unknown format template '#{format}'"
        end
      end
    else
      raise ConfigError, "tail: 'format' parameter is required on tail input"
    end

    if time_format = conf['time_format']
      unless @regexp.names.include?('time')
        raise ConfigError, "tail: 'time_format' parameter is invalid when format doesn't have 'time' capture"
      end
      @time_format = time_format
    end

    if tag = conf['tag']
      @tag = tag
    elsif @regexp.names.include?('tag')
      @tag = ""
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
    array = lines.map {|line|
      process_line(line)
    }
    array.compact!

    unless array.empty?
      Engine.emit_stream(@tag, ArrayEventStream.new(array))
    end
  end

  private
  def process_line(line)
    m = @regexp.match(line)
    unless m
      $log.debug "tail: pattern not match: #{line}"
      # TODO?
      return nil
    end

    tag = nil
    time = nil
    record = {}

    m.names.each {|name|
      if value = m[name]
        case name
        when "time"
          if @time_format
            time = Time.strptime(value, @time_format).to_i
          else
            time = Time.parse(value).to_i
          end
        when "tag"
          tag = value
        else
          record[name] = value
        end
      end
    }

    time ||= Engine.now

    if tag
      Engine.emit(@tag+tag, Event.new(time, record))
      return nil
    end

    Event.new(time, record)

  rescue
    $log.warn "#{line.dump}: #{$!}"
    $log.debug_backtrace
    nil
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

