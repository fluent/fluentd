require_relative 'helper'
require 'fluent/plugin/input'
require 'fluent/plugin/output'
require 'fluent/plugin/bare_output'
require 'fluent/plugin/filter'

module FluentTest
  class FluentTestInput < ::Fluent::Plugin::Input
    ::Fluent::Plugin.register_input('test_in', self)

    attr_reader :started

    def start
      super
      @started = true
    end

    def shutdown
      @started = false
      super
    end
  end

  class FluentTestGenInput < ::Fluent::Plugin::Input
    ::Fluent::Plugin.register_input('test_in_gen', self)

    attr_reader :started

    config_param :num, :integer, default: 10000

    def start
      super
      @started = true

      @num.times { |i|
        router.emit("test.evet", Fluent::EventTime.now, {'message' => 'Hello!', 'key' => "value#{i}", 'num' => i})
      }
    end

    def shutdown
      @started = false
      super
    end
  end

  class FluentTestOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output('test_out', self)

    def initialize
      super
      @events = Hash.new { |h, k| h[k] = [] }
    end

    attr_reader :events
    attr_reader :started

    def start
      super
      @started = true
    end

    def shutdown
      @started = false
      super
    end

    def process(tag, es)
      es.each do |time, record|
        @events[tag] << record
      end
    end
  end

  class FluentTestDynamicOutput < ::Fluent::Plugin::BareOutput
    ::Fluent::Plugin.register_output('test_dynamic_out', self)

    attr_reader :child
    attr_reader :started

    def start
      super
      @started = true
      @child = Fluent::Plugin.new_output('copy')
      conf = config_element('DYNAMIC', '', {}, [
          config_element('store', '', {'@type' => 'test_out', '@id' => 'dyn_out1'}),
          config_element('store', '', {'@type' => 'test_out', '@id' => 'dyn_out2'}),
      ])
      @child.configure(conf)
      @child.start
    end

    def after_start
      super
      @child.after_start
    end

    def stop
      super
      @child.stop
    end

    def before_shutdown
      super
      @child.before_shutdown
    end

    def shutdown
      @started = false
      super
      @child.shutdown
    end

    def after_shutdown
      super
      @child.after_shutdown
    end

    def close
      super
      @child.close
    end

    def terminate
      super
      @child.terminate
    end

    def process(tag, es)
      es.each do |time, record|
        @events[tag] << record
      end
    end
  end

  class FluentTestBufferedOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output('test_out_buffered', self)

    attr_reader :started

    def start
      super
      @started = true
    end

    def shutdown
      @started = false
      super
    end

    def write(chunk)
      # drop everything
    end
  end

  class FluentTestEmitOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output('test_out_emit', self)
    helpers :event_emitter
    def write(chunk)
      tag = chunk.metadata.tag || 'test'
      array = []
      chunk.each do |time, record|
        array << [time, record]
      end
      router.emit_array(tag, array)
    end
  end

  class FluentTestErrorOutput < ::Fluent::Plugin::Output
    ::Fluent::Plugin.register_output('test_out_error', self)

    def format(tag, time, record)
      raise "emit error!"
    end

    def write(chunk)
      raise "chunk error!"
    end
  end

  class FluentCompatTestFilter < ::Fluent::Filter
    ::Fluent::Plugin.register_filter('test_compat_filter', self)

    def initialize(field = '__test__')
      super()
      @num = 0
      @field = field
    end

    attr_reader :num
    attr_reader :started

    def start
      super
      @started = true
    end

    def shutdown
      @started = false
      super
    end

    def filter(tag, time, record)
      record[@field] = @num
      @num += 1
      record
    end
  end

  class FluentTestFilter < ::Fluent::Plugin::Filter
    ::Fluent::Plugin.register_filter('test_filter', self)

    def initialize(field = '__test__')
      super()
      @num = 0
      @field = field
    end

    attr_reader :num
    attr_reader :started

    def start
      super
      @started = true
    end

    def shutdown
      @started = false
      super
    end

    def filter(tag, time, record)
      record[@field] = @num
      @num += 1
      record
    end
  end

  class FluentTestBuffer < Fluent::Plugin::Buffer
    ::Fluent::Plugin.register_buffer('test_buffer', self)

    def resume
      return {}, []
    end

    def generate_chunk(metadata)
    end

    def multi_workers_ready?
      false
    end
  end

  class TestEmitErrorHandler
    def initialize
      @events = Hash.new { |h, k| h[k] = [] }
    end

    attr_reader :events

    def handle_emit_error(tag, time, record, error)
      @events[tag] << record
    end

    def handle_emits_error(tag, es, error)
      es.each { |time,record| handle_emit_error(tag, time, record, error) }
    end
  end
end
