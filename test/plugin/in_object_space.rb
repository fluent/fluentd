require 'fluent/test'

class ObjectSpaceInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    emit_interval 1
    tag t1
    top 2
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ObjectSpaceInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 1, d.instance.emit_interval
    assert_equal "t1", d.instance.tag
    assert_equal 2, d.instance.top
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      sleep 2
    end

    emits = d.emits
    assert_equal true, emits.length > 0

    tag, time, record = emits[0]
    assert_equal 2, record.size
  end
end

