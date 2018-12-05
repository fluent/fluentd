require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_msgpack'

class MessagePackFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::MessagePackFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome'}
  end

  def test_format
    d = create_driver({})
    formatted = d.instance.format(tag, @time, record)

    assert_equal(record.to_msgpack, formatted)
  end
end
