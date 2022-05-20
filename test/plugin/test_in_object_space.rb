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
  end

  def setup
    Fluent::Test.setup
    # Overriding this behavior in the global scope will have an unexpected influence on other tests.
    # So this should be overridden here and be removed in `teardown`.
    def FailObject.class
      raise "FailObject error for tests in ObjectSpaceInputTest."
    end
  end

  def teardown
    FailObject.singleton_class.remove_method(:class)
  end

  TESTCONFIG = %[
    emit_interval 0.2
    tag t1
    top 2
  ]

  def create_driver(conf=TESTCONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ObjectSpaceInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 0.2, d.instance.emit_interval
    assert_equal "t1", d.instance.tag
    assert_equal 2, d.instance.top
  end

  def test_emit
    d = create_driver

    d.run(expect_emits: 3)

    emits = d.events
    assert{ emits.length > 0 }

    emits.each { |tag, time, record|
      assert_equal d.instance.tag, tag
      assert_equal d.instance.top, record.keys.size
      assert(time.is_a?(Fluent::EventTime))
    }
  end
end
