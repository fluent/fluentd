# This file MUST NOT be required from any fluentd modules except for 'PLUGIN_spec.rb'!

require 'fluent/log'
require 'fluent/engine'
require 'fluent/plugin'
require 'fluent/config'
require 'fluent/input'
require 'fluent/output'

$log ||= Fluent::Log.new

# Fluentd::Engine.setup_test_environment!

module Fluent
  module PluginEmitStub
    def self.register_target(klass)
      @@current_target = klass
      @@store ||= {}
      @@store[Thread.current] ||= {}
      @@store[Thread.current][klass] = {} # tag => [events]
    end

    def self.emit_event(tag, time, record)
      @@store[Thread.current][@@current_target][tag] ||= []
      @@store[Thread.current][@@current_target][tag].push( PluginSpecHelper::Event.new(time, record) )
    end

    def self.stored(tag)
      @@store[Thread.current][@@current_target] || {}
    end
  end

  module PluginSpecUser
    refine Fluent::EngineClass do
      def emit(tag, time, record)
        Fluent::PluginEmitStub.emit_event(tag, time, record)
      end
      def emits(tag, es)
        es.each do |time, record|
          Fluent::PluginEmitStub.emit_event(tag, time, record)
        end
      end
    end
  end

  module PluginSpecHelper
    class PluginDriver
      attr_reader :instance

      def initialize(plugin_klass, conf)
        @plugin_klass = plugin_klass
        Fluent::PluginEmitStub.register_target(plugin_klass)

        plugin = plugin_klass.new
        unless plugin.is_a?(Fluent::Input) || plugin.is_a?(Fluent::Output)
          raise ArgumentError, "unknown class as plugin #{plugin.class}"
        end

        logger = TestLogger.new

        conf = Fluent::Config.parse(conf, '[test config]')
        plugin.configure(conf)

        plugin_loglevel = plugin.instance_variable_get(:@log_level)
        if plugin_loglevel
          logger.log_level = plugin_loglevel
        end
        plugin.log = logger

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
        @instance.shutdown
      end

      def events
        Fluent::PluginEmitStub.stored(@plugin_klass)
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

    class DummyLogDevice
      attr_reader :logs

      def initialize
        @logs = []
      end

      def tty?
        false
      end

      def puts(*args)
        args.each do |arg|
          write(arg + "\n")
        end
      end

      def write(message)
        @logs.push message
      end

      def flush
        true
      end

      def close
        true
      end
    end

    class TestLogger < PluginLogger
      def initialize
        @logdev = DummyLogDevice.new
        super(Log.new(@logdev))
      end

      def logs
        @logdev.logs
      end
    end
  end
end
