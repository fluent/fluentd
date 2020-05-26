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

  def setup_gc_stat
    stat = GC.stat
    stub(GC).stat { stat }
    stat
  end

  def test_emit
    stat = setup_gc_stat

    d = create_driver
    d.run(expect_emits: 2)

    events = d.events
    assert(events.length > 0)
    events.each_index {|i|
      assert_equal(stat, events[i][2])
      assert(events[i][1].is_a?(Fluent::EventTime))
    }
  end

  def test_emit_with_use_symbol_keys_false
    stat = setup_gc_stat
    result = {}
    stat.each_pair { |k, v|
      result[k.to_s] = v
    }

    d = create_driver(CONFIG + "use_symbol_keys false")
    d.run(expect_emits: 2)

    events = d.events
    assert(events.length > 0)
    events.each_index {|i|
      assert_equal(result, events[i][2])
      assert(events[i][1].is_a?(Fluent::EventTime))
    }
  end
end
