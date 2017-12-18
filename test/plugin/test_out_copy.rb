require_relative '../helper'
require 'fluent/test/driver/multi_output'
require 'fluent/plugin/out_copy'
require 'fluent/event'

class CopyOutputTest < Test::Unit::TestCase
  class << self
    def startup
      $LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts'))
      require 'fluent/plugin/out_test'
      require 'fluent/plugin/out_test2'
    end

    def shutdown
      $LOAD_PATH.shift
    end
  end

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    <store>
      @type test
      name c0
    </store>
    <store>
      @type test2
      name c1
    </store>
    <store>
      @type test
      name c2
    </store>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::MultiOutput.new(Fluent::Plugin::CopyOutput).configure(conf)
  end

  def test_configure
    d = create_driver

    outputs = d.instance.outputs
    assert_equal 3, outputs.size
    assert_equal Fluent::Plugin::TestOutput, outputs[0].class
    assert_equal Fluent::Plugin::Test2Output, outputs[1].class
    assert_equal Fluent::Plugin::TestOutput, outputs[2].class
    assert_equal "c0", outputs[0].name
    assert_equal "c1", outputs[1].name
    assert_equal "c2", outputs[2].name
  end

  def test_feed_events
    d = create_driver

    assert !d.instance.outputs[0].has_router?
    assert_not_nil d.instance.outputs[1].router
    assert !d.instance.outputs[2].has_router?

    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: 'test') do
      d.feed(time, {"a" => 1})
      d.feed(time, {"a" => 2})
    end

    d.instance.outputs.each {|o|
      assert_equal [ [time, {"a"=>1}], [time, {"a"=>2}] ], o.events
    }
  end

  def test_msgpack_unpacker_cache_bug_for_msgpack_event_stream
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    source = Fluent::ArrayEventStream.new([ [time, {"a" => 1}], [time, {"a" => 2}] ])
    es = Fluent::MessagePackEventStream.new(source.to_msgpack_stream)

    d.run(default_tag: 'test') do
      d.feed(es)
    end

    d.instance.outputs.each { |o|
      assert_equal [ [time, {"a"=>1}], [time, {"a"=>2}] ], o.events
    }
  end

  def create_event_test_driver(does_deep_copy = false)
    config = %[
      deep_copy #{does_deep_copy}
      <store>
        @type test
        name output1
      </store>
      <store>
        @type test
        name output2
      </store>
    ]

    d = Fluent::Test::Driver::MultiOutput.new(Fluent::Plugin::CopyOutput).configure(config)
    d.instance.outputs[0].define_singleton_method(:process) do |tag, es|
      es.each do |time, record|
        record['foo'] = 'bar'
      end
      super(tag, es)
    end
    d
  end

  time = event_time("2013-05-26 06:37:22 UTC")
  mes0 = Fluent::MultiEventStream.new
  mes0.add(time, {"a" => 1})
  mes0.add(time, {"b" => 1})
  mes1 = Fluent::MultiEventStream.new
  mes1.add(time, {"a" => 1})
  mes1.add(time, {"b" => 1})

  data(
    "OneEventStream without deep_copy"   => [false, Fluent::OneEventStream.new(time, {"a" => 1})],
    "OneEventStream with deep_copy"      => [true,  Fluent::OneEventStream.new(time, {"a" => 1})],
    "ArrayEventStream without deep_copy" => [false, Fluent::ArrayEventStream.new([ [time, {"a" => 1}], [time, {"b" => 2}] ])],
    "ArrayEventStream with deep_copy"    => [true,  Fluent::ArrayEventStream.new([ [time, {"a" => 1}], [time, {"b" => 2}] ])],
    "MultiEventStream without deep_copy" => [false, mes0],
    "MultiEventStream with deep_copy"    => [true,  mes1],
  )
  def test_deep_copy_controls_shallow_or_deep_copied(data)
    does_deep_copy, es = data

    d = create_event_test_driver(does_deep_copy)

    d.run(default_tag: 'test') do
      d.feed(es)
    end

    events = d.instance.outputs.map(&:events)

    if does_deep_copy
      events[0].each_with_index do |entry0, i|
        record0 = entry0.last
        record1 = events[1][i].last

        assert{ record0.object_id != record1.object_id }
        assert_equal "bar", record0["foo"]
        assert !record1.has_key?("foo")
      end
    else
      events[0].each_with_index do |entry0, i|
        record0 = entry0.last
        record1 = events[1][i].last

        assert{ record0.object_id == record1.object_id }
        assert_equal "bar", record0["foo"]
        assert_equal "bar", record1["foo"]
      end
    end
  end

  IGNORE_ERROR_CONFIG = %[
    <store ignore_error>
      @type test
      name c0
    </store>
    <store ignore_error>
      @type test
      name c1
    </store>
    <store>
      @type test
      name c2
    </store>
  ]

  def test_ignore_error
    d = create_driver(IGNORE_ERROR_CONFIG)

    # override to raise an error
    d.instance.outputs[0].define_singleton_method(:process) do |tag, es|
      raise ArgumentError, 'Failed'
    end

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    assert_nothing_raised do
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
    end
  end
end

