require_relative 'helper'
require 'fluent/test'
require 'fluent/input'

class FluentInputTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = '')
    Fluent::Test::InputTestDriver.new(Fluent::Input).configure(conf)
  end

  # for v0.12 compatibility
  def test_router_emit
    d = create_driver
    assert_true d.instance.respond_to?(:router)
    assert_true d.instance.respond_to?(:router=)
    assert_true d.instance.router.respond_to?(:emit)
  end
end
