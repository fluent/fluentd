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

require 'forwardable'
require 'logger'

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

    def self.str_to_level(log_level_str)
      case log_level_str.downcase
      when "trace" then LEVEL_TRACE
      when "debug" then LEVEL_DEBUG
      when "info"  then LEVEL_INFO
      when "warn"  then LEVEL_WARN
      when "error" then LEVEL_ERROR
      when "fatal" then LEVEL_FATAL
      else raise "Unknown log level: level = #{log_level_str}"
      end
    end

    def initialize(logger, opts={})
      # overwrites logger.level= so that config reloading resets level of Fluentd::Log
      orig_logger_level_setter = logger.class.public_instance_method(:level=).bind(logger)
      me = self
      # The original ruby logger sets the number as each log level like below.
      # DEBUG = 0
      # INFO  = 1
      # WARN  = 2
      # ERROR = 3
      # FATAL = 4
      # Serverengine use this original log number. In addition to this, serverengine sets -1 as TRACE level.
      # TRACE = -1
      #
      # On the other hand, in fluentd side, it sets the number like below.
      # TRACE = 0
      # DEBUG = 1
      # INFO  = 2
      # WARN  = 3
      # ERROR = 4
      # FATAL = 5
      #
      # Then fluentd's level is set as serverengine's level + 1.
      # So if serverengine's logger level is changed, fluentd's log level will be changed to that + 1.
      logger.define_singleton_method(:level=) {|level| orig_logger_level_setter.call(level); me.level = self.level + 1 }

      @logger = logger
      @out = logger.instance_variable_get(:@logdev)
      @level = logger.level + 1
      @debug_mode = false
      @self_event = false
      @tag = 'fluent'
      @time_format = '%Y-%m-%d %H:%M:%S %z '
      @depth_offset = 1
      enable_color out.tty?
      # TODO: This variable name is unclear so we should change to better name.
      @threads_exclude_events = []

      # Fluent::Engine requires Fluent::Log, so we must take that object lazily
      @engine = Fluent.const_get('Engine')
      @optional_header = nil
      @optional_attrs = nil

      @suppress_repeated_stacktrace = opts[:suppress_repeated_stacktrace]
    end

    def dup
      dl_opts = {}
      dl_opts[:log_level] = @level - 1
      logger = ServerEngine::DaemonLogger.new(@out, dl_opts)
      clone = self.class.new(logger, suppress_repeated_stacktrace: @suppress_repeated_stacktrace)
      clone.tag = @tag
      clone.time_format = @time_format
      # optional headers/attrs are not copied, because new PluginLogger should have another one of it
      clone
    end

    attr_accessor :out
    attr_accessor :level
    attr_accessor :tag
    attr_accessor :time_format
    attr_accessor :optional_header, :optional_attrs

    def logdev=(logdev)
      @out = logdev
      @logger.instance_variable_set(:@logdev, logdev)
      nil
    end

    def reopen!
      # do nothing in @logger.reopen! because it's already reopened in Supervisor.load_config
      @logger.reopen! if @logger
      nil
    end

    def enable_debug(b=true)
      @debug_mode = b
      self
    end

    def enable_event(b=true)
      @self_event = b
      self
    end

    def enable_color?
      !@color_reset.empty?
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

    # If you want to suppress event emitting in specific thread, please use this method.
    # Events in passed thread are never emitted.
    def disable_events(thread)
      @threads_exclude_events.push(thread) unless @threads_exclude_events.include?(thread)
    end

    def on_trace(&block)
      return if @level > LEVEL_TRACE
      block.call if block
    end

    def trace(*args, &block)
      return if @level > LEVEL_TRACE
      args << block.call if block
      time, msg = event(:trace, args)
      puts [@color_trace, caller_line(time, @depth_offset, LEVEL_TRACE), msg, @color_reset].join
    rescue
      # logger should not raise an exception. This rescue prevents unexpected behaviour.
    end
    alias TRACE trace

    def trace_backtrace(backtrace=$!.backtrace)
      dump_stacktrace(backtrace, LEVEL_TRACE)
    end

    def on_debug(&block)
      return if @level > LEVEL_DEBUG
      block.call if block
    end

    def debug(*args, &block)
      return if @level > LEVEL_DEBUG
      args << block.call if block
      time, msg = event(:debug, args)
      puts [@color_debug, caller_line(time, @depth_offset, LEVEL_DEBUG), msg, @color_reset].join
    rescue
    end
    alias DEBUG debug

    def debug_backtrace(backtrace=$!.backtrace)
      dump_stacktrace(backtrace, LEVEL_DEBUG)
    end

    def on_info(&block)
      return if @level > LEVEL_INFO
      block.call if block
    end

    def info(*args, &block)
      return if @level > LEVEL_INFO
      args << block.call if block
      time, msg = event(:info, args)
      puts [@color_info, caller_line(time, @depth_offset, LEVEL_INFO), msg, @color_reset].join
    rescue
    end
    alias INFO info

    def info_backtrace(backtrace=$!.backtrace)
      dump_stacktrace(backtrace, LEVEL_INFO)
    end

    def on_warn(&block)
      return if @level > LEVEL_WARN
      block.call if block
    end

    def warn(*args, &block)
      return if @level > LEVEL_WARN
      args << block.call if block
      time, msg = event(:warn, args)
      puts [@color_warn, caller_line(time, @depth_offset, LEVEL_WARN), msg, @color_reset].join
    rescue
    end
    alias WARN warn

    def warn_backtrace(backtrace=$!.backtrace)
      dump_stacktrace(backtrace, LEVEL_WARN)
    end

    def on_error(&block)
      return if @level > LEVEL_ERROR
      block.call if block
    end

    def error(*args, &block)
      return if @level > LEVEL_ERROR
      args << block.call if block
      time, msg = event(:error, args)
      puts [@color_error, caller_line(time, @depth_offset, LEVEL_ERROR), msg, @color_reset].join
    rescue
    end
    alias ERROR error

    def error_backtrace(backtrace=$!.backtrace)
      dump_stacktrace(backtrace, LEVEL_ERROR)
    end

    def on_fatal(&block)
      return if @level > LEVEL_FATAL
      block.call if block
    end

    def fatal(*args, &block)
      return if @level > LEVEL_FATAL
      args << block.call if block
      time, msg = event(:fatal, args)
      puts [@color_fatal, caller_line(time, @depth_offset, LEVEL_FATAL), msg, @color_reset].join
    rescue
    end
    alias FATAL fatal

    def fatal_backtrace(backtrace=$!.backtrace)
      dump_stacktrace(backtrace, LEVEL_FATAL)
    end

    def puts(msg)
      @logger << msg + "\n"
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

    def reset
      @out.reset if @out.respond_to?(:reset)
    end

    private

    def dump_stacktrace(backtrace, level)
      return if @level > level

      time = Time.now
      line = caller_line(time, 5, level)
      if @suppress_repeated_stacktrace && (Thread.current[:last_repeated_stacktrace] == backtrace)
        puts ["  ", line, 'suppressed same stacktrace'].join
      else
        backtrace.each { |msg|
          puts ["  ", line, msg].join
        }
        Thread.current[:last_repeated_stacktrace] = backtrace if @suppress_repeated_stacktrace
      end

      nil
    end

    def event(level, args)
      time = Time.now
      message = @optional_header ? @optional_header.dup : ''
      map = @optional_attrs ? @optional_attrs.dup : {}
      args.each {|a|
        if a.is_a?(Hash)
          a.each_pair {|k,v|
            map[k.to_s] = v
          }
        else
          message << a.to_s
        end
      }

      map.each_pair {|k,v|
        if k == "error".freeze && v.is_a?(Exception) && !map.has_key?("error_class")
          message << " error_class=#{v.class.to_s} error=#{v.to_s.inspect}"
        else
          message << " #{k}=#{v.inspect}"
        end
      }

      unless @threads_exclude_events.include?(Thread.current)
        record = map.dup
        record.keys.each {|key|
          record[key] = record[key].inspect unless record[key].respond_to?(:to_msgpack)
        }
        record['message'] = message.dup
        @engine.push_log_event("#{@tag}.#{level}", time.to_i, record)
      end

      return time, message
    end

    def caller_line(time, depth, level)
      log_msg = "#{time.strftime(@time_format)}[#{LEVEL_TEXT[level]}]: "
      if @debug_mode
        line = caller(depth+1)[0]
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


  # PluginLogger has own log level separated from global $log object.
  # This class enables log_level option in each plugin.
  #
  # PluginLogger has same functionality as Log but some methods are forwarded to internal logger
  # for keeping logging action consistency in the process, e.g. color, tag, event, etc.
  class PluginLogger < Log
    def initialize(logger)
      @logger = logger
      @level = @logger.level
      @depth_offset = 2
      if logger.instance_variable_defined?(:@suppress_repeated_stacktrace)
        @suppress_repeated_stacktrace = logger.instance_variable_get(:@suppress_repeated_stacktrace)
      end

      enable_color @logger.enable_color?
    end

    def level=(log_level_str)
      @level = Log.str_to_level(log_level_str)
    end

    alias orig_enable_color enable_color

    def enable_color(b = true)
      orig_enable_color b
      @logger.enable_color b
    end

    extend Forwardable
    def_delegators '@logger', :enable_color?, :enable_debug, :enable_event,
      :disable_events, :tag, :tag=, :time_format, :time_format=,
      :event, :caller_line, :puts, :write, :flush, :reset, :out, :out=,
      :optional_header, :optional_header=, :optional_attrs, :optional_attrs=
  end


  module PluginLoggerMixin
    def self.included(klass)
      klass.instance_eval {
        desc 'Allows the user to set different levels of logging for each plugin.'
        config_param :@log_level, :string, default: nil, alias: :log_level # 'log_level' will be warned as deprecated
      }
    end

    def initialize
      super

      @log = $log # Use $log object directly by default
    end

    attr_accessor :log

    def configure(conf)
      super

      if level = conf['@log_level']
        unless @log.is_a?(PluginLogger)
          @log = PluginLogger.new($log.dup)
        end
        @log.level = level
        @log.optional_header = "[#{self.class.name}#{plugin_id_configured? ? "(" + @id + ")" : ""}] "
        @log.optional_attrs = {}
      end
    end

    def start
      @log.reset
      super
    end

    def terminate
      super
      @log.reset
    end
  end

  # This class delegetes some methods which are used in `Fluent::Logger` to a instance variable(`dev`) in `Logger::LogDevice` class
  # https://github.com/ruby/ruby/blob/7b2d47132ff8ee950b0f978ab772dee868d9f1b0/lib/logger.rb#L661
  class LogDeviceIO < ::Logger::LogDevice
    def flush
      if @dev.respond_to?(:flush)
        @dev.flush
      else
        super
      end
    end

    def tty?
      if @dev.respond_to?(:tty?)
        @dev.tty?
      else
        super
      end
    end

    def sync=(v)
      if @dev.respond_to?(:sync=)
        @dev.sync = v
      else
        super
      end
    end
  end
end
