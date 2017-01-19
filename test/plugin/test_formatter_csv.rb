require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_csv'

class CsvFormatterTest < ::Test::Unit::TestCase

  def setup
    @time = event_time
  end

  CONF = %[
    fields a,b,c
  ]

  def create_driver(conf = CONF)
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::CsvFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def test_config_params
    d = create_driver
    assert_equal(',', d.instance.delimiter)
    assert_equal(true, d.instance.force_quotes)
    assert_equal(['a', 'b', 'c'], d.instance.fields)
  end

  data('empty array' => [],
       'array including empty string' => ['', ''])
  def test_empty_fields(param)
    assert_raise Fluent::ConfigError do
      create_driver('fields' => param)
    end
  end

  data(
    'tab_char' => ["\t", '\t'],
    'tab_string' => ["\t", 'TAB'],
    'pipe' => ['|', '|'])
  def test_config_params_with_customized_delimiters(data)
    expected, target = data
    d = create_driver("delimiter" => target, 'fields' => 'a,b,c')
    assert_equal expected, d.instance.delimiter
  end

  def test_format
    d = create_driver("fields" => "message,message2")
    formatted = d.instance.format(tag, @time, {
                                    'message' => 'awesome',
                                    'message2' => 'awesome2'
                                  })
    assert_equal("\"awesome\",\"awesome2\"\n", formatted)
  end

  def test_format_without_newline
    d = create_driver("fields" => "message,message2", "add_newline" => false)
    formatted = d.instance.format(tag, @time, {
                                    'message' => 'awesome',
                                    'message2' => 'awesome2'
                                  })
    assert_equal("\"awesome\",\"awesome2\"", formatted)
  end

  def test_format_with_customized_delimiters
    d = create_driver("fields" => "message,message2",
                      "delimiter" => "\t")
    formatted = d.instance.format(tag, @time, {
                                    'message' => 'awesome',
                                    'message2' => 'awesome2'
                                  })
    assert_equal("\"awesome\"\t\"awesome2\"\n", formatted)
  end

  def test_format_with_non_quote
    d = create_driver("fields" => "message,message2",
                      "force_quotes" => false)
    formatted = d.instance.format(tag, @time, {
                                    'message' => 'awesome',
                                    'message2' => 'awesome2'
                                  })
    assert_equal("awesome,awesome2\n", formatted)
  end

  data(
    'nil' => {
      'message' => 'awesome',
      'message2' => nil,
      'message3' => 'awesome3'
    },
    'blank' => {
      'message' => 'awesome',
      'message2' => '',
      'message3' => 'awesome3'
    })
  def test_format_with_empty_fields(data)
    d = create_driver("fields" => "message,message2,message3")
    formatted = d.instance.format(tag, @time, data)
    assert_equal("\"awesome\",\"\",\"awesome3\"\n", formatted)
  end

  data(
    'normally' => 'one,two,three',
    'white_space' => 'one , two , three',
    'blank' => 'one,,two,three')
  def test_config_params_with_fields(data)
    d = create_driver('fields' => data)
    assert_equal %w(one two three), d.instance.fields
  end
end
