require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_roundrobin'

class RoundRobinOutputTest < Test::Unit::TestCase
  class << self
    def startup
      $LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts'))
      require 'fluent/plugin/out_test'
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
      type test
      name c0
    </store>
    <store>
      type test
      name c1
    </store>
    <store>
      type test
      name c2
    </store>
  ]
  CONFIG_WITH_WEIGHT = %[
    <store>
      type test
      name c0
      weight 3
    </store>
    <store>
      type test
      name c1
      weight 3
    </store>
    <store>
      type test
      name c2
    </store>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::RoundRobinOutput).configure(conf)
  end

  def test_configure
    d = create_driver

    outputs = d.instance.outputs
    assert_equal 3, outputs.size
    assert_equal Fluent::TestOutput, outputs[0].class
    assert_equal Fluent::TestOutput, outputs[1].class
    assert_equal Fluent::TestOutput, outputs[2].class
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
    assert_equal Fluent::TestOutput, outputs[0].class
    assert_equal Fluent::TestOutput, outputs[1].class
    assert_equal Fluent::TestOutput, outputs[2].class
    assert_equal "c0", outputs[0].name
    assert_equal "c1", outputs[1].name
    assert_equal "c2", outputs[2].name

    weights = d.instance.weights
    assert_equal 3, weights.size
    assert_equal 3, weights[0]
    assert_equal 3, weights[1]
    assert_equal 1, weights[2]
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.run do
      d.emit({"a"=>1}, time)
      d.emit({"a"=>2}, time)
      d.emit({"a"=>3}, time)
      d.emit({"a"=>4}, time)
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

    d.instance.outputs.each {|o|
      assert_not_nil o.router
    }
  end

  def test_emit_weighted
    d = create_driver(CONFIG_WITH_WEIGHT)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.run do
      14.times do |i|
        d.emit({"a"=>i}, time)
      end
    end

    os = d.instance.outputs

    assert_equal 6, os[0].events.size  # weight=3
    assert_equal 6, os[1].events.size  # weight=3
    assert_equal 2, os[2].events.size  # weight=1
  end
end

