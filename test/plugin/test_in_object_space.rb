require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_object_space'

require 'timeout'

class ObjectSpaceInputTest < Test::Unit::TestCase
  def waiting(seconds, instance)
    begin
      Timeout.timeout(seconds) do
        yield
      end
    rescue Timeout::Error
      STDERR.print(*instance.log.out.logs)
      raise
    end
  end

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
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ObjectSpaceInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 1, d.instance.emit_interval
    assert_equal "t1", d.instance.tag
    assert_equal 2, d.instance.top
  end

  def test_emit
    d = create_driver

    d.run do
      waiting(10, d.instance) do
        sleep 0.5 until d.events.size > 3
      end
    end

    emits = d.events
    assert{ emits.length > 0 }

    emits.each { |tag, time, record|
      assert_equal d.instance.tag, tag
      assert_equal d.instance.top, record.keys.size
      assert(time.is_a?(Fluent::EventTime))
    }
  end
end
