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

    d = create_driver(
      'keys' => 'message,greeting',
      'delimiter'       => ',',
    )
    assert_equal ["message", "greeting"], d.instance.keys
    assert_equal ",", d.instance.delimiter
  end

  def test_format
    d = create_driver(
      'keys' => 'message,greeting',
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("awesome\thello\n", formatted)
  end

  def test_format_with_customized_delimiters
    d = create_driver(
      'keys' => 'message,greeting',
      'delimiter'       => ',',
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("awesome,hello\n", formatted)
  end
end
