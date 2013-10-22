require 'logger'

require 'serverengine'

require 'fluentd/worker'
require 'fluentd/config/parser'
require 'fluentd/plugin/input'
require 'fluentd/plugin/output'
require 'fluentd/plugin/filter'

::Fluentd.plugin = Fluentd::PluginRegistry.new

module Fluentd
  module PluginSpecHelper
    class PluginDriver
      attr_reader :instance

      def initialize(plugin_klass, conf)
        plugin = plugin_klass.new
        unless plugin.is_a?(Fluentd::Plugin::Input) || plugin.is_a?(Fluentd::Plugin::Filter) || plugin.is_a?(Fluentd::Plugin::Output)
          raise ArgumentError, "unknown class as plugin #{plugin.class}"
        end

        testlogger = TestLogger.new(nil)
        plugin.log = testlogger

        conf = Fluentd::Config::Parser.parse(conf, '[test config]')

        plugin.configure(conf)

        dummy_router = TestRouter.new
        plugin.instance_eval{ @event_router = dummy_router }

        @dummy_router = dummy_router
        @instance = plugin
      end

      def pitch(tag, time, record)
        @instance.emit(tag, time, record)
      end

      def pitches(tag, es)
        @instance.emits(tag, es)
      end

      class BatchPitcher
        def initialize(parent, tag, time=Time.now.to_i)
          @parent = parent
          @tag = tag
          @time = time
        end
        def pitch(record, time=nil)
          @parent.pitch(@tag, time ? time : @time, record)
        end

        def pitches(es)
          @parent.pitches(@tag, es)
        end
      end

      def with(tag, time=Time.now.to_i)
        raise ArgumentError unless block_given?
        yield BatchPitcher.new(self, tag, time)
      end

      def run
        raise ArgumentError unless block_given?
        @instance.start
        yield self
        @instance.stop
      end

      def events
        @dummy_router.events
      end
    end

    def generate_driver(plugin, conf)
      PluginDriver.new(plugin, conf)
    end

    class Event
      def initialize(time, record)
        @time = time
        @record = record
      end
      attr_reader :time, :record
      alias_method :t, :time
      alias_method :r, :record
    end

    class TestRouter
      attr_accessor :events

      def initialize
        @events = {}
      end

      def add_collector(pattern, collector)
        nil
      end

      def current_offset
        raise NotImplementedError # not used?
        Fluentd::EventRouter::Offset.new(self, 0)
      end

      def emit(tag, time, record)
        @events[tag] ||= []
        @events[tag].push(Event.new(time, record))
      end

      def emits(tag, es)
        @events[tag] ||= []
        es.each do |time,record|
          @events[tag].push(Event.new(time, record))
        end
      end
    end

    class DummyLogDevice
      attr_reader :logs

      def initialize
        @logs = []
      end

      def write(message)
        @logs.push message
      end

      def close
        true
      end
    end

    class TestLogger < ::Logger
      def initialize(dev, config={})
        super
        @logdev = DummyLogDevice.new
        self.sev_threshold = LEVEL_DEBUG
      end

      LEVEL_FATAL = ::Logger::FATAL
      LEVEL_ERROR = ::Logger::ERROR
      LEVEL_WARN  = ::Logger::WARN
      LEVEL_INFO  = ::Logger::INFO
      LEVEL_DEBUG = ::Logger::DEBUG
      LEVEL_TRACE = -1

      # Need the same thing with Fluentd::Loggger. Let me override ::Logger
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

        self.sev_threshold = @level
      end

      def logs
        @logdev.logs
      end
    end
  end
end
