require_relative '../helper'
require 'fluent/test/driver/parser'

class NullParser < Fluent::Plugin::Parser
  def parse(text)
    yield nil, nil
  end
end

class TestDriverParserTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Parser.new(NullParser).configure(conf)
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
