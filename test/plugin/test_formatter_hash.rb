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

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format(data)
    newline_conf, newline = data
    d = create_driver({"newline" => newline_conf})
    formatted = d.instance.format(tag, @time, record)

    assert_equal(%Q!{"message"=>"awesome", "greeting"=>"hello"}#{newline}!, formatted.encode(Encoding::UTF_8))
  end

  def test_format_without_newline
    d = create_driver('add_newline' => false)
    formatted = d.instance.format(tag, @time, record)

    assert_equal(%Q!{"message"=>"awesome", "greeting"=>"hello"}!, formatted.encode(Encoding::UTF_8))
  end
end
