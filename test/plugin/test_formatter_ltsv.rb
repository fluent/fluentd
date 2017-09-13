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

  def test_format
    d = create_driver({})
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\tgreeting:hello\n", formatted)
  end

  def test_format_without_newline
    d = create_driver('add_newline' => false)
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\tgreeting:hello", formatted)
  end

  def test_format_with_customized_delimiters
    d = create_driver(
      'delimiter'       => ',',
      'label_delimiter' => '=',
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message=awesome,greeting=hello\n", formatted)
  end
end
