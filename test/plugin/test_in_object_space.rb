require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_object_space'

class ObjectSpaceInputTest < Test::Unit::TestCase
  class FailObject
    def self.class
      raise "error"
    end
  end

  def setup
    Fluent::Test.setup
  end

  TESTCONFIG = %[
    emit_interval 1
    tag t1
    top 2
  ]

  def create_driver(conf=TESTCONFIG)
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

    d.expected_emits_length = 2
    d.run

    emits = d.emits
    assert_equal true, emits.length > 0

    emits.each { |tag, time, record|
      assert_equal d.instance.tag, tag
      assert_equal d.instance.top, record.keys.size
    }
  end
end
