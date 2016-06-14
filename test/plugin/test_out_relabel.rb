require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_relabel'

class RelabelOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def default_config
    config_element('ROOT', '', {"@type"=>"forward", "@label"=>"@FORWARD"})
  end

  def create_driver(conf = default_config)
    Fluent::Test::OutputTestDriver.new(Fluent::RelabelOutput).configure(conf)
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.run do
      d.emit({"a"=>1}, time)
      d.emit({"a"=>2}, time)
    end
    emits = d.emits
    assert_equal [["test", time, {"a"=>1}], ["test", time, {"a"=>2}]], emits
  end
end
