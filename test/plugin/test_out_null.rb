require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_null'

class NullOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Output.new(Fluent::Plugin::NullOutput).configure(conf)
  end

  def test_configure
    assert_nothing_raised do
      create_driver
    end
  end

  def test_process
    d = create_driver
    assert_nothing_raised do
      d.run do
        d.feed("test", Fluent::EventTime.now, {"test" => "null"})
      end
    end
    assert_equal([], d.events(tag: "test"))
  end
end
