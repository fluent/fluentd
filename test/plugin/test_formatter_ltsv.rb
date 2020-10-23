require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_ltsv'

class LabeledTSVFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::LabeledTSVFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome', 'greeting' => 'hello'}
  end

  def test_config_params
    d = create_driver
    assert_equal "\t", d.instance.delimiter
    assert_equal  ":", d.instance.label_delimiter
    assert_equal  true, d.instance.add_newline

    d = create_driver(
      'delimiter'       => ',',
      'label_delimiter' => '=',
      'add_newline' => false,
    )

    assert_equal ",", d.instance.delimiter
    assert_equal "=", d.instance.label_delimiter
    assert_equal  false, d.instance.add_newline
  end

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format(data)
    newline_conf, newline = data
    d = create_driver({"newline" => newline_conf})
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\tgreeting:hello#{newline}", formatted)
  end

  def test_format_without_newline
    d = create_driver('add_newline' => false)
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\tgreeting:hello", formatted)
  end

  data("newline (LF)" => ["lf", "\n"],
       "newline (CRLF)" => ["crlf", "\r\n"])
  def test_format_with_customized_delimiters(data)
    newline_conf, newline = data

    d = create_driver(
      'delimiter'       => ',',
      'label_delimiter' => '=',
      'newline' => newline_conf,
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message=awesome,greeting=hello#{newline}", formatted)
  end
end
