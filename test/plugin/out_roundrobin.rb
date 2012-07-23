require 'fluent/test'

class RoundRobinOutputTest < Test::Unit::TestCase
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
  CONFIG2 = %[
<store weight:3>
  type test
  name c0
</store>
<store weight:3>
  type test
  name c1
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

    d = create_driver(CONFIG2)

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
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.emit({"a"=>3}, time)
    d.emit({"a"=>4}, time)

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

  def test_emit_weighted
    d = create_driver(CONFIG2)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    (1..12).each do |i|
      d.emit({"a"=>i}, time)
    end

    os = d.instance.outputs

    assert_equal [
      [time, {"a"=>1}],
      [time, {"a"=>4}],
      [time, {"a"=>6}],
      [time, {"a"=>8}],
      [time, {"a"=>11}],
    ], os[0].events

    assert_equal [
      [time, {"a"=>2}],
      [time, {"a"=>5}],
      [time, {"a"=>7}],
      [time, {"a"=>9}],
      [time, {"a"=>12}],
    ], os[1].events

    assert_equal [
      [time, {"a"=>3}],
      [time, {"a"=>10}],
    ], os[2].events
  end
end

