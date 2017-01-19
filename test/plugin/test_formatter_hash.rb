require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_hash'

class HashFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::HashFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome', 'greeting' => 'hello'}
  end

  def test_format
    d = create_driver({})
    formatted = d.instance.format(tag, @time, record)

    assert_equal(%Q!{"message"=>"awesome", "greeting"=>"hello"}\n!, formatted.encode(Encoding::UTF_8))
  end

  def test_format_without_newline
    d = create_driver('add_newline' => false)
    formatted = d.instance.format(tag, @time, record)

    assert_equal(%Q!{"message"=>"awesome", "greeting"=>"hello"}!, formatted.encode(Encoding::UTF_8))
  end
end
