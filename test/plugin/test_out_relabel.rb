require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_relabel'

class RelabelOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def default_config
    config_element('ROOT', '', {"@type"=>"relabel", "@label"=>"@RELABELED"})
  end

  def create_driver(conf = default_config)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::RelabelOutput).configure(conf)
  end

  def test_process
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: 'test') do
      d.feed(time, {"a"=>1})
      d.feed(time, {"a"=>2})
    end
    assert_equal [["test", time, {"a"=>1}], ["test", time, {"a"=>2}]], d.events
  end
end
