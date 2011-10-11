require 'fluent/test'

class CopyOutputTest < Test::Unit::TestCase
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

  def create_driver(conf = CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput).configure(conf)
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
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    d.instance.outputs.each {|o|
      assert_equal [
          [time, {"a"=>1}],
          [time, {"a"=>2}],
        ], o.events
    }
  end
end

