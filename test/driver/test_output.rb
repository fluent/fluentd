require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_null'

class TestDriverOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Output.new(Fluent::Plugin::NullOutput).configure(conf)
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
