require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_monitor_agent'

class MonitorAgentInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = '')
    Fluent::Test::InputTestDriver.new(Fluent::MonitorAgentInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal("0.0.0.0", d.instance.bind)
    assert_equal(24220, d.instance.port)
    assert_equal(nil, d.instance.tag)
    assert_equal(60, d.instance.emit_interval)
  end
end
