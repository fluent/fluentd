require_relative 'helper'
require 'fluent/input'

class FluentInputTest < ::Test::Unit::TestCase
  include Fluent

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = '')
    Fluent::Test::InputTestDriver.new(Fluent::Input).configure(conf, true)
  end

  def test_configure
    d = create_driver
    assert_equal Engine.root_agent.event_router, d.instance.router

    assert_raise(ArgumentError) {
      create_driver('@label @unknown')
    }

    Engine.root_agent.add_label('@known')
    d = nil
    assert_nothing_raised { 
      d = create_driver('@label @known')
    }
    assert d.instance.router
  end
end
