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

  def test_router
    d = create_driver
    assert_equal Engine.root_agent.event_router, d.instance.router

    d = nil
    assert_nothing_raised {
      d = create_driver('@label @known')
    }
    expected = Engine.root_agent.find_label('@known').event_router
    assert_equal expected, d.instance.router

    # TestDriver helps to create a label instance automatically, so directly test here
    assert_raise(ArgumentError) {
      Fluent::Input.new.configure(Config.parse('@label @unknown', '(test)', '(test_dir)', true))
    }
  end
end
