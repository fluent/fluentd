#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
  require 'serverengine'
  require 'forwardable'

  class Logger
    def initialize(dev, config={})
      @dl = ServerEngine::DaemonLogger.new(dev, config)

      io = @dl.instance_eval { @io }
      if io.tty?
        enable_color
      end

      @debug_mode = true
      @time_format = '%Y-%m-%d %H:%M:%S %z '
      @tag = 'fluent'
      @level = LEVEL_DEBUG
    end

    def path=(path)
      @dl.path = path
    end

    attr_accessor :tag
    attr_accessor :time_format

    extend Forwardable

    def_delegators :@dl, :hook_stdout!, :hook_stderr!, :reopen!, :reopen, :close, :<<, :path=

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

    LEVEL_FATAL = ::Logger::FATAL
    LEVEL_ERROR = ::Logger::ERROR
    LEVEL_WARN  = ::Logger::WARN
    LEVEL_INFO  = ::Logger::INFO
    LEVEL_DEBUG = ::Logger::DEBUG
    LEVEL_TRACE = -1

    def level=(expr)
      case expr.to_s
      when 'fatal', LEVEL_FATAL.to_s
        @level = LEVEL_FATAL
      when 'error', LEVEL_ERROR.to_s
        @level = LEVEL_ERROR
      when 'warn', LEVEL_WARN.to_s
        @level = LEVEL_WARN
      when 'info', LEVEL_INFO.to_s
        @level = LEVEL_INFO
      when 'debug', LEVEL_DEBUG.to_s
        @level = LEVEL_DEBUG
      when 'trace', LEVEL_TRACE.to_s
        @level = LEVEL_TRACE
      else
        raise ArgumentError, "invalid log level: #{expr}"
      end

      @level
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

    %w[fatal error warn info debug trace].each do |level|
      eval <<-CODE
      def #{level}?
        @level <= LEVEL_#{level.upcase}
      end

      def on_#{level}(&block)
        return if @level > LEVEL_#{level.upcase}
        block.call if block
      end

      def #{level}(*args, &block)
        return if @level > LEVEL_#{level.upcase}
        add_event('#{level}', @color_#{level}, args, block)
      end

      def #{level}_backtrace(backtrace=$!.backtrace)
        return if @level > LEVEL_#{level.upcase}
        add_backtrace('#{level}', backtrace)
      end
      CODE
    end

    private
    def add_event(level, color, args, block)
      message = ''

      record = {}
      args.each {|a|
        if a.is_a?(Hash)
          a.each_pair {|k,v|
            record[k.to_s] = v
          }
        else
          message << a.to_s
        end
      }

      message << block.call if block

      record.each_pair {|k,v|
        message << " #{k}=#{v.inspect}"
      }

      time = Time.now
      self << format(2, time, level, color, message, @color_reset)

      collect_stats(level, message, record, time)

      nil
    end

    def add_backtrace(level, backtrace)
      time = Time.now

      backtrace.each {|message|
        self << format(5, time, level, '  ', message, nil)
      }

      nil
    end

    def format(stack_depth, time, level, prefix, message, suffix)
      time_str = time.strftime(@time_format)

      if @debug_mode
        at_line = caller(stack_depth+1)[0]
        if match = /^(.+?):(\d+)(?::in `(.*)')?/.match(at_line)
          file = match[1].split('/')[-2,2].join('/')
          line = match[2]
          method = match[3]
          return "#{prefix}#{time_str}[#{level}]: #{file}:#{line}:#{method}: #{message}#{suffix}\n"
        end
      end

      return "#{prefix}#{time_str}[#{level}]: #{message}#{suffix}\n"
    end

    def collect_stats(level, message, record, time)
      # See RootAgent::StatsCollectLoggerMixin
    end
  end

end
