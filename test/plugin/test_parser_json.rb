require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class JsonParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse(data)
    @parser.configure('json_parser' => data)
    @parser.instance.parse('{"time":1362020400,"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
      assert_equal(event_time('2013-02-28 12:00:00 +0900').to_i, time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'size'   => 777,
                     'method' => 'PUT',
                   }, record)
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_nested_json(data)
    @parser.configure(
                       'flatten_json' => 'true',
                       'json_parser' => data
                     )
    nested_json = '{"time": 1552109400, "nested_param": {
      "hash_hash": {"a": {"a": 0}, "b": {"b": 1, "c": 2}},
      "array_array": [[3], [4, 5], [6, 7, 8]],
      "hash_array": {"a": [[9], [10, 11], [12, 13, 14]]},
      "array_hash": [{"a": 15}, {"b": 16, "c": 17}]
    }}'
    @parser.instance.parse(nested_json) { |time, record|
      assert_equal(event_time('2019-03-09 14:30:00 +0900').to_i, time)
      assert_equal({
                     'nested_param.hash_hash.a.a'    =>   0,
                     'nested_param.hash_hash.b.b'    =>   1,
                     'nested_param.hash_hash.b.c'    =>   2,
                     'nested_param.array_array.0.0'  =>   3,
                     'nested_param.array_array.1.0'  =>   4,
                     'nested_param.array_array.1.1'  =>   5,
                     'nested_param.array_array.2.0'  =>   6,
                     'nested_param.array_array.2.1'  =>   7,
                     'nested_param.array_array.2.2'  =>   8,
                     'nested_param.hash_array.a.0.0' =>   9,
                     'nested_param.hash_array.a.1.0' =>  10,
                     'nested_param.hash_array.a.1.1' =>  11,
                     'nested_param.hash_array.a.2.0' =>  12,
                     'nested_param.hash_array.a.2.1' =>  13,
                     'nested_param.hash_array.a.2.2' =>  14,
                     'nested_param.array_hash.0.a'   =>  15,
                     'nested_param.array_hash.1.b'   =>  16,
                     'nested_param.array_hash.1.c'   =>  17
                   }, record)
    }

    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
    parser.configure('json_parser' => data)
    parser.instance.parse(nested_json) { |time, record|
      assert_equal(event_time('2019-03-09 14:30:00 +0900').to_i, time)
      assert_equal({
                     'nested_param' => {
                         'hash_hash'   => {'a' => {'a' => 0}, 'b' => {'b' => 1, 'c' => 2}},
                         'array_array' => [[3], [4, 5], [6, 7, 8]],
                         'hash_array'  => {'a' => [[9], [10, 11], [12, 13, 14]]},
                         'array_hash'  => [{'a' => 15}, {'b' => 16, 'c' => 17}]
                      }
                   }, record)
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_large_float(data)
    @parser.configure('json_parser' => data)
    @parser.instance.parse('{"num":999999999999999999999999999999.99999}') { |time, record|
      assert_equal(Float, record['num'].class)
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_without_time(data)
    time_at_start = Time.now.to_i

    @parser.configure('json_parser' => data)
    @parser.instance.parse('{"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal({
                     'host'   => '192.168.0.1',
                     'size'   => 777,
                     'method' => 'PUT',
                   }, record)
    }

    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
    parser.configure('json_parser' => data, 'estimate_current_event' => 'false')
    parser.instance.parse('{"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
      assert_equal({
                     'host'   => '192.168.0.1',
                     'size'   => 777,
                     'method' => 'PUT',
                   }, record)
      assert_nil time, "parser return nil w/o time and if specified so"
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_colon_string(data)
    @parser.configure('json_parser' => data)
    @parser.instance.parse('{"time":1362020400,"log":":message"}') { |time, record|
      assert_equal(record['log'], ':message')
    }
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_invalid_time(data)
    @parser.configure('json_parser' => data)
    assert_raise Fluent::ParserError do
      @parser.instance.parse('{"time":[],"k":"v"}') { |time, record| }
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_float_time(data)
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
    parser.configure('json_parser' => data)
    text = "100.1"
    parser.instance.parse("{\"time\":\"#{text}\"}") do |time, record|
      assert_equal 100, time.sec
      assert_equal 100_000_000, time.nsec
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_keep_time_key(data)
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
    format = "%d/%b/%Y:%H:%M:%S %z"
    parser.configure(
                     'time_format' => format,
                     'keep_time_key' => 'true',
                     'json_parser' => data
                     )
    text = "28/Feb/2013:12:00:00 +0900"
    parser.instance.parse("{\"time\":\"#{text}\"}") do |time, record|
      assert_equal Time.strptime(text, format).to_i, time.sec
      assert_equal text, record['time']
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_parse_with_keep_time_key_without_time_format(data)
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
    parser.configure(
                     'keep_time_key' => 'true',
                     'json_parser' => data
                     )
    text = "100"
    parser.instance.parse("{\"time\":\"#{text}\"}") do |time, record|
      assert_equal text.to_i, time.sec
      assert_equal text, record['time']
    end
  end

  def test_yajl_parse_io_with_buffer_smaller_than_input
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
    parser.configure(
                     'keep_time_key' => 'true',
                     'json_parser' => 'yajl',
                     'stream_buffer_size' => 1,
                     )
    text = "100"

    waiting(5) do
      rd, wr = IO.pipe
      wr.write "{\"time\":\"#{text}\"}"

      parser.instance.parse_io(rd) do |time, record|
        assert_equal text.to_i, time.sec
        assert_equal text, record['time']

        # Once a record has been received the 'write' end of the pipe must be
        # closed, otherwise the test will block waiting for more input.
        wr.close
      end
    end
  end
end
