require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_gc_stat'

class GCStatInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    emit_interval 1
    tag t1
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::GCStatInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal(1, d.instance.emit_interval)
    assert_equal("t1", d.instance.tag)
  end

  def test_emit
    stat = GC.stat
    stub(GC).stat { stat }

    d = create_driver
    d.end_if do
      d.emit_count >= 2
    end
    d.run do
      sleep(0.1) until d.stop?
    end

    events = d.events
    assert(events.length > 0)
    events.each_index {|i|
      assert_equal(stat, events[i][2])
      assert(events[i][1].is_a?(Fluent::EventTime))
    }
  end
end
