require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_tsv'

class TSVFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::TSVFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome', 'greeting' => 'hello'}
  end

  def test_config_params
    d = create_driver(
      'keys' => 'message,greeting',
    )
    assert_equal ["message", "greeting"], d.instance.keys
    assert_equal "\t", d.instance.delimiter
    assert_equal true, d.instance.add_newline

    d = create_driver(
      'keys' => 'message,greeting',
      'delimiter' => ',',
      'add_newline' => false,
    )
    assert_equal ["message", "greeting"], d.instance.keys
    assert_equal ",", d.instance.delimiter
    assert_equal false, d.instance.add_newline
  end

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format(data)
    newline_conf, newline = data
    d = create_driver(
      'keys' => 'message,greeting',
      'newline' => newline_conf
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("awesome\thello#{newline}", formatted)
  end

  def test_format_without_newline
    d = create_driver(
      'keys' => 'message,greeting',
      'add_newline' => false,
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("awesome\thello", formatted)
  end

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format_with_customized_delimiters(data)
    newline_conf, newline = data
    d = create_driver(
      'keys' => 'message,greeting',
      'delimiter' => ',',
      'newline' => newline_conf,
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("awesome,hello#{newline}", formatted)
  end
end
