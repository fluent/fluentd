require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class NoneParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def test_config_params
    parser = Fluent::Test::Driver::Parser.new(Fluent::TextParser::NoneParser)
    parser.configure({})
    assert_equal "message", parser.instance.message_key

    parser.configure('message_key' => 'foobar')
    assert_equal "foobar", parser.instance.message_key
  end

  def test_parse
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin.new_parser('none'))
    parser.configure({})
    parser.instance.parse('log message!') { |time, record|
      assert_equal({'message' => 'log message!'}, record)
    }
  end

  def test_parse_with_message_key
    parser = Fluent::Test::Driver::Parser.new(Fluent::TextParser::NoneParser)
    parser.configure('message_key' => 'foobar')
    parser.instance.parse('log message!') { |time, record|
      assert_equal({'foobar' => 'log message!'}, record)
    }
  end

  def test_parse_without_default_time
    time_at_start = Time.now.to_i

    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin.new_parser('none'))
    parser.configure({})
    parser.instance.parse('log message!') { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal({'message' => 'log message!'}, record)
    }

    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin.new_parser('none'))
    parser.configure({'estimate_current_event' => 'false'})
    parser.instance.parse('log message!') { |time, record|
      assert_equal({'message' => 'log message!'}, record)
      assert_nil time, "parser returns nil w/o time if configured so"
    }
  end
end
