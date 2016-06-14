require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class JsonParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @parser = Fluent::TextParser::JSONParser.new
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse(data)
    @parser.configure('json_parser' => data)
    @parser.parse('{"time":1362020400,"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
      assert_equal(str2time('2013-02-28 12:00:00 +0900').to_i, time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'size'   => 777,
                     'method' => 'PUT',
                   }, record)
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_large_float(data)
    @parser.configure('json_parser' => data)
    @parser.parse('{"num":999999999999999999999999999999.99999}') { |time, record|
      assert_equal(Float, record['num'].class)
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_without_time(data)
    time_at_start = Time.now.to_i

    @parser.configure('json_parser' => data)
    @parser.parse('{"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal({
                     'host'   => '192.168.0.1',
                     'size'   => 777,
                     'method' => 'PUT',
                   }, record)
    }

    parser = Fluent::TextParser::JSONParser.new
    parser.estimate_current_event = false
    parser.configure('json_parser' => data)
    parser.parse('{"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
      assert_equal({
                     'host'   => '192.168.0.1',
                     'size'   => 777,
                     'method' => 'PUT',
                   }, record)
      assert_nil time, "parser return nil w/o time and if specified so"
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_invalid_time(data)
    @parser.configure('json_parser' => data)
    assert_raise Fluent::ParserError do
      @parser.parse('{"time":[],"k":"v"}') { |time, record| }
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_float_time(data)
    parser = Fluent::TextParser::JSONParser.new
    parser.configure('json_parser' => data)
    text = "100.1"
    parser.parse("{\"time\":\"#{text}\"}") do |time, record|
      assert_equal Time.at(text.to_f).to_i, time.sec
      assert_equal Time.at(text.to_f).nsec, time.nsec
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_keep_time_key(data)
    parser = Fluent::TextParser::JSONParser.new
    format = "%d/%b/%Y:%H:%M:%S %z"
    parser.configure(
                     'time_format' => format,
                     'keep_time_key' => 'true',
                     'json_parser' => data
                     )
    text = "28/Feb/2013:12:00:00 +0900"
    parser.parse("{\"time\":\"#{text}\"}") do |time, record|
      assert_equal Time.strptime(text, format).to_i, time.sec
      assert_equal text, record['time']
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_keep_time_key_without_time_format(data)
    parser = Fluent::TextParser::JSONParser.new
    parser.configure(
                     'keep_time_key' => 'true',
                     'json_parser' => data
                     )
    text = "100"
    parser.parse("{\"time\":\"#{text}\"}") do |time, record|
      assert_equal text.to_i, time.sec
      assert_equal text, record['time']
    end
  end
end
