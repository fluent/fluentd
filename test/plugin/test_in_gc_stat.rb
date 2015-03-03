require_relative '../helper'
require 'fluent/test'
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
    Fluent::Test::InputTestDriver.new(Fluent::GCStatInput).configure(conf)
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
    d.run do
      sleep 2
    end

    emits = d.emits
    assert(emits.length > 0)
    assert_equal(stat, emits[0][2])
  end
end
