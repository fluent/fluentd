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

  sub_test_case "various record pattern" do
    data("Only string", { record: '"message"', expected: [nil] }, keep: true)
    data("Only string without quotation", { record: "message", expected: [nil] }, keep: true)
    data("Only number", { record: "0", expected: [nil] }, keep: true)
    data(
      "Array of Hash",
      {
        record: '[{"k1": 1}, {"k2": 2}]',
        expected: [{"k1" => 1}, {"k2" => 2}]
      },
      keep: true,
    )
    data(
      "Array of both Hash and invalid",
      {
        record: '[{"k1": 1}, "string", {"k2": 2}, 0]',
        expected: [{"k1" => 1}, nil, {"k2" => 2}, nil]
      },
      keep: true,
    )
    data(
      "Array of all invalid",
      {
        record: '["string", 0, [{"k": 0}]]',
        expected: [nil, nil, nil]
      },
      keep: true,
    )
    def test_oj(data)
      i = 0
      @parser.configure('json_parser' => "oj")
      @parser.instance.parse(data[:record]) { |time, record|
        assert_equal(data[:expected][i], record)
        i += 1
      }
    end

    def test_yajl(data)
      i = 0
      @parser.configure('json_parser' => "yajl")
      @parser.instance.parse(data[:record]) { |time, record|
        assert_equal(data[:expected][i], record)
        i += 1
      }
    end

    def test_json(json)
      i = 0
      @parser.configure('json_parser' => "json")
      @parser.instance.parse(data[:record]) { |time, record|
        assert_equal(data[:expected][i], record)
        i += 1
      }
    end
  end
end
