# frozen_string_literal: true

require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_single_value'

class SingleValueFormatterTest < ::Test::Unit::TestCase
  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::SingleValueFormatter).configure(conf)
  end

  def test_config_params
    d = create_driver
    assert_equal "message", d.instance.message_key
  end

  def test_config_params_message_key
    d = create_driver('message_key' => 'foobar')
    assert_equal "foobar", d.instance.message_key
  end

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format(data)
    newline_conf, newline = data
    d = create_driver('newline' => newline_conf)
    formatted = d.instance.format('tag', event_time, {'message' => 'awesome'})
    assert_equal("awesome#{newline}", formatted)
  end

  def test_format_without_newline
    d = create_driver('add_newline' => 'false')
    formatted = d.instance.format('tag', event_time, {'message' => 'awesome'})
    assert_equal("awesome", formatted)
  end

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format_with_message_key(data)
    newline_conf, newline = data
    d = create_driver('message_key' => 'foobar', 'newline' => newline_conf)
    formatted = d.instance.format('tag', event_time, {'foobar' => 'foo'})

    assert_equal("foo#{newline}", formatted)
  end
end
