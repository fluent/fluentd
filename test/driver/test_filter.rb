require_relative '../helper'
require 'fluent/test/driver/filter'

class NullFilter < Fluent::Plugin::Filter
  def filter(tag, time, record)
  end
end

class TestDriverFilterTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Filter.new(NullFilter).configure(conf)
  end

  test 'run w/ block returns last evaluated value in block' do
    d = create_driver
    value = d.run do
      :value
    end
    assert_equal(value, :value)
  end

  test 'run w/o block returns nil' do
    d = create_driver
    assert_nil(d.run(timeout: 0.01))
  end
end
