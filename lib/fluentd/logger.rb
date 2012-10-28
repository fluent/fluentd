#
# Fluentd
#
# Copyright (C) 2012 FURUHASHI Sadayuki
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
module Fluentd

  require 'logger'

  class Logger
    def initialize(dev, shift_age=0, shift_size=1048576)
      if dev.is_a?(String)
        @path = dev
        @io = File.open(@path, File::WRONLY|File::APPEND|File::CREAT)
      else
        @io = dev
      end
      @io.sync = true

      @time_format = '%Y-%m-%d %H:%M:%S %z: '
      @level = LEVEL_TRACE
      @caller_trace = true

      @hook = []
      @hook_stdout = false
      @hook_stderr = false

      @logdev = ::Logger::LogDevice.new(@io, :shift_age=>shift_age, :shift_size=>shift_size)

      enable_color(@io.tty?)
    end

    attr_accessor :level
    attr_accessor :time_format
    attr_accessor :caller_trace

    def hook_io(target)
      return nil if @io == target || @hook.include?(target)
      target.reopen(@io)
      @hook << target
      true
    end

    def reopen!
      if @path
        @io.reopen(@path)
        @hook.each {|target|
          target.reopen(@io)
        }
      end
      nil
    end

    def reopen
      begin
        reopen!
        return true
      rescue
        return false  # TODO log?
      end
    end

    def close
      if @path
        @io.close unless @io.closed?
      end
      nil
    end

    LEVELS = {
      :trace => 0,
      :debug => 1,
      :info  => 2,
      :warn  => 3,
      :error => 4,
      :fatal => 5,
    }

    LEVELS.each_pair do |name,lv|
      eval <<-MODULE_EVAL
        LEVEL_#{name.to_s.upcase} = #{lv}

        def on_#{name}(&block)
          return if @level > #{lv}
          block.call if block
        end

        def #{name}(*args, &block)
          return if @level > #{lv}
          args << block.call if block
          time = Time.now
          msg = format_message(:#{name}, args)
          tmsg = time.strftime(@time_format)
          at = caller_trace(1) if @caller_trace
          write [@color_#{name}, tmsg, at, msg, @eol].join
        end

        def #{name}_backtrace(backtrace=$!.backtrace)
          return if @level > #{lv}
          at = caller_trace(1) if @caller_trace
          backtrace.each {|msg|
            write ["  ", at, msg, "\n"].join
          }
        end
      MODULE_EVAL
    end

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
      @eol = "#{@color_reset}\n"
      self
    end

    def puts(line)
      write "#{line}\n"
    end

    def write(data)
      @logdev.write(data)
    end

    private

    def format_message(level_name, args)
      msg = ''

      args.each {|a|
        case a
        when Hash
          a.each_pair {|k,v|
            msg << "#{k}=#{v.inspect}"
          }
        else
          msg << a.to_s
        end
      }

      return msg
    end

    def caller_trace(depth)
      line = caller(depth + 1)[0]
      if match = /^(.+?):(\d+)(?::in `(.*)')?/.match(line)
        file = match[1].split('/')[-2,2].join('/')
        line = match[2]
        method = match[3]
        return "#{file}:#{line}:#{method}: "
      end
      return nil
    end
  end

end

