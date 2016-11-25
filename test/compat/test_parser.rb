require_relative '../helper'
require 'fluent/plugin/parser'

class TextParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  class MultiEventTestParser < ::Fluent::Parser
    include Fluent::Configurable

    def parse(text)
      2.times { |i|
        record = {}
        record['message'] = text
        record['number'] = i
        yield Fluent::Engine.now, record
      }
    end
  end

  Fluent::TextParser.register_template('multi_event_test', Proc.new { MultiEventTestParser.new })

  def test_lookup_unknown_format
    assert_raise Fluent::ConfigError do
      Fluent::Plugin.new_parser('unknown')
    end
  end

  data('register_formatter' => 'known', 'register_template' => 'known_old')
  def test_lookup_known_parser(data)
    $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(__FILE__)), '..', 'scripts'))
    assert_nothing_raised Fluent::ConfigError do
      Fluent::Plugin.new_parser(data)
    end
    $LOAD_PATH.shift
  end

  def test_parse_with_return
    parser = Fluent::TextParser.new
    parser.configure('format' => 'none')
    _time, record = parser.parse('log message!')
    assert_equal({'message' => 'log message!'}, record)
  end

  def test_parse_with_block
    parser = Fluent::TextParser.new
    parser.configure('format' => 'none')
    parser.parse('log message!') { |time, record|
      assert_equal({'message' => 'log message!'}, record)
    }
  end

  def test_multi_event_parser
    parser = Fluent::TextParser.new
    parser.configure('format' => 'multi_event_test')
    i = 0
    parser.parse('log message!') { |time, record|
      assert_equal('log message!', record['message'])
      assert_equal(i, record['number'])
      i += 1
    }
  end

  def test_setting_estimate_current_event_value
    p1 = Fluent::TextParser.new
    assert_nil p1.estimate_current_event
    assert_nil p1.parser

    p1.configure('format' => 'none')
    assert_equal true, p1.parser.estimate_current_event

    p2 = Fluent::TextParser.new
    assert_nil p2.estimate_current_event
    assert_nil p2.parser

    p2.estimate_current_event = false

    p2.configure('format' => 'none')
    assert_equal false, p2.parser.estimate_current_event
  end

  data(ignorecase: Regexp::IGNORECASE,
       multiline: Regexp::MULTILINE,
       both: Regexp::IGNORECASE & Regexp::MULTILINE)
  def test_regexp_parser_config(options)
    source = "a"
    parser = Fluent::TextParser::RegexpParser.new(Regexp.new(source, options), { "dummy" => "dummy" })
    regexp = parser.instance_variable_get("@regexp")
    assert_equal(options, regexp.options)
  end
end
