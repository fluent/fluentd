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

    LOG_EVENT_TAG_PREFIX = 'fluent'
    LOG_EVENT_LABEL = '@FLUENT_LOG'
    LOG_TYPE_SUPERVISOR = :supervisor # only in supervisor, or a worker with --no-supervisor
    LOG_TYPE_WORKER0 = :worker0 # only in a worker with worker_id=0 (without showing worker id)
    LOG_TYPE_DEFAULT = :default # show logs in all supervisor/workers, with worker id in workers (default)

    LOG_TYPES = [LOG_TYPE_SUPERVISOR, LOG_TYPE_WORKER0, LOG_TYPE_DEFAULT].freeze

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

    def self.event_tags
      LEVEL_TEXT.map{|t| "#{LOG_EVENT_TAG_PREFIX}.#{t}" }
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
      @log_event_enabled = false
      @depth_offset = 1
      @format = nil
      @time_format = nil
      @formatter = nil

      self.format = :text
      enable_color out.tty?
      # TODO: This variable name is unclear so we should change to better name.
      @threads_exclude_events = []

      # Fluent::Engine requires Fluent::Log, so we must take that object lazily
      @engine = Fluent.const_get('Engine')
      @optional_header = nil
      @optional_attrs = nil

      @suppress_repeated_stacktrace = opts[:suppress_repeated_stacktrace]

      @process_type = opts[:process_type] # :supervisor, :worker0, :workers Or :standalone
      @process_type ||= :standalone # to keep behavior of existing code
      case @process_type
      when :supervisor
        @show_supervisor_log = true
        @show_worker0_log = false
      when :worker0
        @show_supervisor_log = false
        @show_worker0_log = true
      when :workers
        @show_supervisor_log = false
        @show_worker0_log = false
      when :standalone
        @show_supervisor_log = true
        @show_worker0_log = true
      else
        raise "BUG: unknown process type for logger:#{@process_type}"
      end
      @worker_id = opts[:worker_id]
      @worker_id_part = "##{@worker_id} " # used only for :default log type in workers
    end

    def dup
      dl_opts = {}
      dl_opts[:log_level] = @level - 1
      logger = ServerEngine::DaemonLogger.new(@out, dl_opts)
      clone = self.class.new(logger, suppress_repeated_stacktrace: @suppress_repeated_stacktrace, process_type: @process_type, worker_id: @worker_id)
      clone.format = @format
      clone.time_format = @time_format
      clone.log_event_enabled = @log_event_enabled
      # optional headers/attrs are not copied, because new PluginLogger should have another one of it
      clone
    end

    attr_reader :format
    attr_accessor :log_event_enabled
    attr_accessor :out
    attr_accessor :level
    attr_accessor :time_format
    attr_accessor :optional_header, :optional_attrs

    def logdev=(logdev)
      @out = logdev
      @logger.instance_variable_set(:@logdev, logdev)
      nil
    end

    def format=(fmt)
      return if @format == fmt

      @time_format = '%Y-%m-%d %H:%M:%S %z'
      @time_formatter = Strftime.new(@time_format) rescue nil

      case fmt
      when :text
        @format = :text
        @formatter = Proc.new { |type, time, level, msg|
          r = caller_line(type, time, @depth_offset, level)
          r << msg
          r
        }
      when :json
        @format = :json
        @formatter = Proc.new { |type, time, level, msg|
          r = {
            'time' => format_time(time),
            'level' => LEVEL_TEXT[level],
            'message' => msg
          }
          if wid = get_worker_id(type)
            r['worker_id'] = wid
          end
          Yajl.dump(r)
        }
      end

      nil
    end

    def time_format=(time_fmt)
      @time_format = time_fmt
      @time_formatter = Strftime.new(@time_format) rescue nil
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
      @log_event_enabled = b
      self
    end

    # If you want to suppress event emitting in specific thread, please use this method.
    # Events in passed thread are never emitted.
    def disable_events(thread)
      # this method is not symmetric with #enable_event.
      @threads_exclude_events.push(thread) unless @threads_exclude_events.include?(thread)
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

    def log_type(args)
      if LOG_TYPES.include?(args.first)
        args.shift
      else
        LOG_TYPE_DEFAULT
      end
    end

    # TODO: skip :worker0 logs when Fluentd gracefully restarted
    def skipped_type?(type)
      case type
      when LOG_TYPE_DEFAULT
        false
      when LOG_TYPE_WORKER0
        !@show_worker0_log
      when LOG_TYPE_SUPERVISOR
        !@show_supervisor_log
      else
        raise "BUG: unknown log type:#{type}"
      end
    end

    def on_trace
      return if @level > LEVEL_TRACE
      yield
    end

    def trace(*args, &block)
      return if @level > LEVEL_TRACE
      type = log_type(args)
      return if skipped_type?(type)
      args << block.call if block
      time, msg = event(:trace, args)
      puts [@color_trace, @formatter.call(type, time, LEVEL_TRACE, msg), @color_reset].join
    rescue
      # logger should not raise an exception. This rescue prevents unexpected behaviour.
    end
    alias TRACE trace

    def trace_backtrace(backtrace=$!.backtrace, type: :default)
      dump_stacktrace(type, backtrace, LEVEL_TRACE)
    end

    def on_debug
      return if @level > LEVEL_DEBUG
      yield
    end

    def debug(*args, &block)
      return if @level > LEVEL_DEBUG
      type = log_type(args)
      return if skipped_type?(type)
      args << block.call if block
      time, msg = event(:debug, args)
      puts [@color_debug, @formatter.call(type, time, LEVEL_DEBUG, msg), @color_reset].join
    rescue
    end
    alias DEBUG debug

    def debug_backtrace(backtrace=$!.backtrace, type: :default)
      dump_stacktrace(type, backtrace, LEVEL_DEBUG)
    end

    def on_info
      return if @level > LEVEL_INFO
      yield
    end

    def info(*args, &block)
      return if @level > LEVEL_INFO
      type = log_type(args)
      return if skipped_type?(type)
      args << block.call if block
      time, msg = event(:info, args)
      puts [@color_info, @formatter.call(type, time, LEVEL_INFO, msg), @color_reset].join
    rescue
    end
    alias INFO info

    def info_backtrace(backtrace=$!.backtrace, type: :default)
      dump_stacktrace(type, backtrace, LEVEL_INFO)
    end

    def on_warn
      return if @level > LEVEL_WARN
      yield
    end

    def warn(*args, &block)
      return if @level > LEVEL_WARN
      type = log_type(args)
      return if skipped_type?(type)
      args << block.call if block
      time, msg = event(:warn, args)
      puts [@color_warn, @formatter.call(type, time, LEVEL_WARN, msg), @color_reset].join
    rescue
    end
    alias WARN warn

    def warn_backtrace(backtrace=$!.backtrace, type: :default)
      dump_stacktrace(type, backtrace, LEVEL_WARN)
    end

    def on_error
      return if @level > LEVEL_ERROR
      yield
    end

    def error(*args, &block)
      return if @level > LEVEL_ERROR
      type = log_type(args)
      return if skipped_type?(type)
      args << block.call if block
      time, msg = event(:error, args)
      puts [@color_error, @formatter.call(type, time, LEVEL_ERROR, msg), @color_reset].join
    rescue
    end
    alias ERROR error

    def error_backtrace(backtrace=$!.backtrace, type: :default)
      dump_stacktrace(type, backtrace, LEVEL_ERROR)
    end

    def on_fatal
      return if @level > LEVEL_FATAL
      yield
    end

    def fatal(*args, &block)
      return if @level > LEVEL_FATAL
      type = log_type(args)
      return if skipped_type?(type)
      args << block.call if block
      time, msg = event(:fatal, args)
      puts [@color_fatal, @formatter.call(type, time, LEVEL_FATAL, msg), @color_reset].join
    rescue
    end
    alias FATAL fatal

    def fatal_backtrace(backtrace=$!.backtrace, type: :default)
      dump_stacktrace(type, backtrace, LEVEL_FATAL)
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
    # We need `#<<` method to use this logger class with other
    # libraries such as aws-sdk
    alias << write

    def flush
      @out.flush
    end

    def reset
      @out.reset if @out.respond_to?(:reset)
    end

    def dump_stacktrace(type, backtrace, level)
      return if @level > level

      time = Time.now

      if @format == :text
        line = caller_line(type, time, 5, level)
        if @suppress_repeated_stacktrace && (Thread.current[:last_repeated_stacktrace] == backtrace)
          puts ["  ", line, 'suppressed same stacktrace'].join
        else
          backtrace.each { |msg|
            puts ["  ", line, msg].join
          }
          Thread.current[:last_repeated_stacktrace] = backtrace if @suppress_repeated_stacktrace
        end
      else
        r = {
          'time' => format_time(time),
          'level' => LEVEL_TEXT[level],
        }
        if wid = get_worker_id(type)
          r['worker_id'] = wid
        end

        if @suppress_repeated_stacktrace && (Thread.current[:last_repeated_stacktrace] == backtrace)
          r['message'] = 'suppressed same stacktrace'
        else
          r['message'] = backtrace.join("\n")
          Thread.current[:last_repeated_stacktrace] = backtrace if @suppress_repeated_stacktrace
        end

        puts Yajl.dump(r)
      end

      nil
    end

    def get_worker_id(type)
      if type == :default && (@process_type == :worker0 || @process_type == :workers)
        @worker_id
      else
        nil
      end
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

      if @log_event_enabled && !@threads_exclude_events.include?(Thread.current)
        record = map.dup
        record.keys.each {|key|
          record[key] = record[key].inspect unless record[key].respond_to?(:to_msgpack)
        }
        record['message'] = message.dup
        @engine.push_log_event("#{LOG_EVENT_TAG_PREFIX}.#{level}", Fluent::EventTime.from_time(time), record)
      end

      return time, message
    end

    def caller_line(type, time, depth, level)
      worker_id_part = if type == :default && (@process_type == :worker0 || @process_type == :workers)
                         @worker_id_part
                       else
                         "".freeze
                       end
      log_msg = "#{format_time(time)} [#{LEVEL_TEXT[level]}]: #{worker_id_part}"
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

    def format_time(time)
      @time_formatter ? @time_formatter.exec(time) : time.strftime(@time_format)
    end
  end


  # PluginLogger has own log level separated from global $log object.
  # This class enables log_level option in each plugin.
  #
  # PluginLogger has same functionality as Log but some methods are forwarded to internal logger
  # for keeping logging action consistency in the process, e.g. color, event, etc.
  class PluginLogger < Log
    def initialize(logger)
      @logger = logger
      @level = @logger.level
      @format = nil
      @depth_offset = 2
      if logger.instance_variable_defined?(:@suppress_repeated_stacktrace)
        @suppress_repeated_stacktrace = logger.instance_variable_get(:@suppress_repeated_stacktrace)
      end

      self.format = @logger.format
      enable_color @logger.enable_color?
    end

    def level=(log_level_str)
      @level = Log.str_to_level(log_level_str)
    end

    alias orig_format= format=
    alias orig_enable_color enable_color

    def format=(fmt)
      self.orig_format = fmt
      @logger.format = fmt
    end

    def enable_color(b = true)
      orig_enable_color b
      @logger.enable_color b
    end

    extend Forwardable
    def_delegators '@logger', :get_worker_id, :enable_color?, :enable_debug, :enable_event,
      :disable_events, :log_event_enabled, :log_event_enabled=, :time_format, :time_format=,
      :time_formatter, :time_formatter=, :event, :caller_line, :puts, :write, :<<, :flush,
      :reset, :out, :out=, :optional_header, :optional_header=, :optional_attrs, :optional_attrs=
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

      if plugin_id_configured? || conf['@log_level']
        @log = PluginLogger.new($log.dup) unless @log.is_a?(PluginLogger)
        @log.optional_attrs = {}

        if level = conf['@log_level']
          @log.level = level
        end

        if plugin_id_configured?
          @log.optional_header = "[#{@id}] "
        end
      end
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

    def reopen(path, mode)
      if mode != 'a'
        raise "Unsupported mode: #{mode}"
      end
      super(path)
    end
  end
end
