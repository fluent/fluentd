require_relative '../helper'
require 'fluent/test/driver/multi_output'
require 'fluent/plugin/out_roundrobin'

class RoundRobinOutputTest < Test::Unit::TestCase
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
  CONFIG_WITH_WEIGHT = %[
    <store>
      @type test
      name c0
      weight 3
    </store>
    <store>
      @type test2
      name c1
      weight 3
    </store>
    <store>
      @type test
      name c2
    </store>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::MultiOutput.new(Fluent::Plugin::RoundRobinOutput).configure(conf)
  end

  def test_configure
    d = create_driver

    outputs = d.instance.outputs
    assert_equal 3, outputs.size

    assert_equal Fluent::Plugin::TestOutput, outputs[0].class
    assert_equal Fluent::Plugin::Test2Output, outputs[1].class
    assert_equal Fluent::Plugin::TestOutput, outputs[2].class

    assert !outputs[0].has_router?
    assert outputs[1].has_router?
    assert outputs[1].router
    assert !outputs[2].has_router?

    assert_equal "c0", outputs[0].name
    assert_equal "c1", outputs[1].name
    assert_equal "c2", outputs[2].name

    weights = d.instance.weights
    assert_equal 3, weights.size
    assert_equal 1, weights[0]
    assert_equal 1, weights[1]
    assert_equal 1, weights[2]

    d = create_driver(CONFIG_WITH_WEIGHT)

    outputs = d.instance.outputs
    assert_equal 3, outputs.size

    assert_equal Fluent::Plugin::TestOutput, outputs[0].class
    assert_equal Fluent::Plugin::Test2Output, outputs[1].class
    assert_equal Fluent::Plugin::TestOutput, outputs[2].class

    assert_equal "c0", outputs[0].name
    assert_equal "c1", outputs[1].name
    assert_equal "c2", outputs[2].name

    weights = d.instance.weights
    assert_equal 3, weights.size
    assert_equal 3, weights[0]
    assert_equal 3, weights[1]
    assert_equal 1, weights[2]
  end

  def test_events_feeded_to_plugins_by_roundrobin
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: 'test') do
      d.feed(time, {"a" => 1})
      d.feed(time, {"a" => 2})
      d.feed(time, {"a" => 3})
      d.feed(time, {"a" => 4})
    end

    os = d.instance.outputs

    assert_equal [
        [time, {"a"=>1}],
        [time, {"a"=>4}],
      ], os[0].events

    assert_equal [
        [time, {"a"=>2}],
      ], os[1].events

    assert_equal [
        [time, {"a"=>3}],
      ], os[2].events
  end

  def test_events_feeded_with_specified_weights
    d = create_driver(CONFIG_WITH_WEIGHT)

    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: 'test') do
      14.times do |i|
        d.feed(time, {"a" => i})
      end
    end

    os = d.instance.outputs

    assert_equal 6, os[0].events.size  # weight=3
    assert_equal 6, os[1].events.size  # weight=3
    assert_equal 2, os[2].events.size  # weight=1
  end
end

