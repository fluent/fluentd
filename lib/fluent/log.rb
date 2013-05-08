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


class Log
  module TTYColor
    RESET   = "\033]R"
    CRE     = "\033[K"
    CLEAR   = "\033c"
    NORMAL  = "\033[0;39m"
    RED     = "\033[1;31m"
    GREEN   = "\033[1;32m"
    YELLOW  = "\033[1;33m"
    BLUE    = "\033[1;34m"
    MAGENTA = "\033[1;35m"
    CYAN    = "\033[1;36m"
    WHITE   = "\033[1;37m"
  end

  LEVEL_TRACE = 0
  LEVEL_DEBUG = 1
  LEVEL_INFO  = 2
  LEVEL_WARN  = 3
  LEVEL_ERROR = 4
  LEVEL_FATAL = 5

  LEVEL_TEXT = %w(trace debug info warn error fatal)

  def initialize(out=STDERR, level=LEVEL_TRACE)
    @out = out
    @level = level
    @debug_mode = false
    @self_event = false
    @tag = 'fluent'
    @time_format = '%Y-%m-%d %H:%M:%S %z '
    enable_color out.tty?
  end

  attr_accessor :out
  attr_accessor :level
  attr_accessor :tag
  attr_accessor :time_format

  def enable_debug(b=true)
    @debug_mode = b
    self
  end

  def enable_event(b=true)
    @self_event = b
    self
  end

  def enable_color(b=true)
    if b
      @color_trace = TTYColor::BLUE
      @color_debug = TTYColor::WHITE
      @color_info  = TTYColor::GREEN
      @color_warn  = TTYColor::YELLOW
      @color_error = TTYColor::MAGENTA
      @color_fatal = TTYColor::RED
      @color_reset = TTYColor::NORMAL
    else
      @color_trace = ''
      @color_debug = ''
      @color_info  = ''
      @color_warn  = ''
      @color_error = ''
      @color_fatal = ''
      @color_reset = ''
    end
    self
  end

  def on_trace(&block)
    return if @level > LEVEL_TRACE
    block.call if block
  end

  def trace(*args, &block)
    return if @level > LEVEL_TRACE
    args << block.call if block
    time, msg = event(:trace, args)
    puts [@color_trace, caller_line(time, 1, LEVEL_TRACE), msg, @color_reset].join
  end
  alias TRACE trace

  def trace_backtrace(backtrace=$!.backtrace)
    return if @level > LEVEL_TRACE
    time = Time.now
    backtrace.each {|msg|
      puts ["  ", caller_line(time, 4, LEVEL_TRACE), msg].join
    }
    nil
  end

  def on_debug(&block)
    return if @level > LEVEL_DEBUG
    block.call if block
  end

  def debug(*args, &block)
    return if @level > LEVEL_DEBUG
    args << block.call if block
    time, msg = event(:debug, args)
    puts [@color_debug, caller_line(time, 1, LEVEL_DEBUG), msg, @color_reset].join
  end
  alias DEBUG debug

  def debug_backtrace(backtrace=$!.backtrace)
    return if @level > LEVEL_DEBUG
    time = Time.now
    backtrace.each {|msg|
      puts ["  ", caller_line(time, 4, LEVEL_DEBUG), msg].join
    }
    nil
  end

  def on_info(&block)
    return if @level > LEVEL_INFO
    block.call if block
  end

  def info(*args, &block)
    return if @level > LEVEL_INFO
    args << block.call if block
    time, msg = event(:info, args)
    puts [@color_info, caller_line(time, 1, LEVEL_INFO), msg, @color_reset].join
  end
  alias INFO info

  def info_backtrace(backtrace=$!.backtrace)
    return if @level > LEVEL_INFO
    time = Time.now
    backtrace.each {|msg|
      puts ["  ", caller_line(time, 4, LEVEL_INFO), msg].join
    }
    nil
  end

  def on_warn(&block)
    return if @level > LEVEL_WARN
    block.call if block
  end

  def warn(*args, &block)
    return if @level > LEVEL_WARN
    args << block.call if block
    time, msg = event(:warn, args)
    puts [@color_warn, caller_line(time, 1, LEVEL_WARN), msg, @color_reset].join
  end
  alias WARN warn

  def warn_backtrace(backtrace=$!.backtrace)
    return if @level > LEVEL_WARN
    time = Time.now
    backtrace.each {|msg|
      puts ["  ", caller_line(time, 4, LEVEL_WARN), msg].join
    }
    nil
  end

  def on_error(&block)
    return if @level > LEVEL_ERROR
    block.call if block
  end

  def error(*args, &block)
    return if @level > LEVEL_ERROR
    args << block.call if block
    time, msg = event(:error, args)
    puts [@color_error, caller_line(time, 1, LEVEL_ERROR), msg, @color_reset].join
  end
  alias ERROR error

  def error_backtrace(backtrace=$!.backtrace)
    return if @level > LEVEL_ERROR
    time = Time.now
    backtrace.each {|msg|
      puts ["  ", caller_line(time, 4, LEVEL_ERROR), msg].join
    }
    nil
  end

  def on_fatal(&block)
    return if @level > LEVEL_FATAL
    block.call if block
  end

  def fatal(*args, &block)
    return if @level > LEVEL_FATAL
    args << block.call if block
    time, msg = event(:fatal, args)
    puts [@color_fatal, caller_line(time, 1, LEVEL_FATAL), msg, @color_reset].join
  end
  alias FATAL fatal

  def fatal_backtrace(backtrace=$!.backtrace)
    return if @level > LEVEL_FATAL
    time = Time.now
    backtrace.each {|msg|
      puts ["  ", caller_line(time, 4, LEVEL_FATAL), msg].join
    }
    nil
  end

  def puts(msg)
    @out.puts(msg)
    @out.flush
    msg
  rescue
    # FIXME
    nil
  end

  def write(data)
    @out.write(data)
  end

  def flush
    @out.flush
  end

  private
  def event(level, args)
    time = Time.now
    message = ''
    map = {}
    args.each {|a|
      if a.is_a?(Hash)
        a.each_pair {|k,v|
          map[k.to_s] = v
        }
      else
        message << a.to_s
      end
    }

    if @self_event
      c = caller
      if c.count(c.shift) <= 0
        record = map.dup
        record['message'] = message.dup
        ### In signal trap context, 'Engine.emit' cannot get Monitor lock (Thread level lock),
        ### and ThreadError will be raised
        begin
          Engine.emit("#{@tag}.#{level}", time.to_i, record)
        rescue ThreadError # message: can't be called from trap context
          # ignore
        end
        # TODO: We should check 'GET_THREAD()->interrupt_mask & TRAP_INTERRUPT_MASK'
        #       for replacement of begin-rescue if any methods exists.
      end
    end

    map.each_pair {|k,v|
      message << " #{k}=#{v.inspect}"
    }

    return time, message
  end

  def caller_line(time, depth, level)
    line = caller(depth+1)[0]
    log_msg = "#{time.strftime(@time_format)}[#{LEVEL_TEXT[level]}]: "
    if @debug_mode
      if match = /^(.+?):(\d+)(?::in `(.*)')?/.match(line)
        file = match[1].split('/')[-2,2].join('/')
        line = match[2]
        method = match[3]
        return "#{log_msg}#{file}:#{line}:#{method}: "
      end
    end
    return log_msg
  end
end


end

