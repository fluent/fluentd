require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class JsonParserTest < ::Test::Unit::TestCase
  # Captures warnings emitted via Warning.warn while a capture buffer is set.
  # `Module#prepend` cannot be undone, so the hook is installed once and
  # delegates to `super` whenever no capture is active, keeping other tests
  # unaffected.
  module WarningCapture
    class << self
      attr_accessor :buffer
    end

    def warn(message, category: nil)
      buffer = WarningCapture.buffer
      if buffer
        buffer << message
      else
        super
      end
    end
  end
  Warning.singleton_class.prepend(WarningCapture)

  def with_captured_warnings
    warnings = []
    original_deprecated = Warning[:deprecated]
    Warning[:deprecated] = true
    WarningCapture.buffer = warnings
    yield
    warnings
  ensure
    WarningCapture.buffer = nil
    Warning[:deprecated] = original_deprecated
  end

  def setup
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
  end

  sub_test_case "configure_json_parser" do
    data("oj", [:oj, [Oj.method(:load), Oj::ParseError]])
    data("json", [:json, [Fluent::Plugin::JSONParser::JSON_PARSE_PROC, JSON::ParserError]])
    data("yajl", [:yajl, [Fluent::Plugin::JSONParser::JSON_PARSE_PROC, JSON::ParserError]])
    def test_return_each_loader((input, expected_return))
      result = @parser.instance.configure_json_parser(input)
      assert_equal expected_return, result
    end

    def test_raise_exception_for_unknown_input
      assert_raise RuntimeError do
        @parser.instance.configure_json_parser(:unknown)
      end
    end

    def test_fall_back_oj_to_json_if_oj_not_available
      stub(Fluent::OjOptions).available? { false }

      result = @parser.instance.configure_json_parser(:oj)

      assert_equal [Fluent::Plugin::JSONParser::JSON_PARSE_PROC, JSON::ParserError], result
      logs = @parser.logs.collect do |log|
        log.gsub(/\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [-+]\d{4} /, "")
      end
      assert_equal(
        ["[info]: Oj is not installed, and failing back to JSON for json parser\n"],
        logs
      )
    end
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

  data('oj' => 'oj', 'json' => 'json', 'yajl' => 'yajl')
  def test_parse_with_duplicated_key(data)
    @parser.configure('json_parser' => data)
    assert_nothing_raised do
      @parser.instance.parse('{"k":"a","k":"b"}') { |time, record|
        assert_equal('b', record['k'])
      }
    end
  end

  data('oj' => 'oj', 'json' => 'json', 'yajl' => 'yajl')
  def test_parse_with_comments(data)
    @parser.configure('json_parser' => data)
    assert_nothing_raised do
      @parser.instance.parse('{
        "key1":"a", // for a,
        "key1":"b"  // for b
        }') { |time, record|
      }
    end
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

  # `rest` alone cannot detect a stream cut on a complete token boundary, so
  # these shapes exercise the `rest` / `partial_value` logical-OR classifier.
  # 'after-comma' is the critical case: without `partial_value` the whole
  # record was silently dropped with no warning.
  data('mid-token'   => ['{"a":1}{"b":2', [{"a" => 1}]],
       'after-colon' => ['{"a":1}{"b":',  [{"a" => 1}]],
       'after-brace' => ['{"a":1}{',      [{"a" => 1}]],
       'after-comma' => ['{"a":1,',       []])
  def test_parse_io_warns_on_truncated_stream(data)
    input, expected_records = data
    @parser.configure('json_parser' => 'json')
    io = StringIO.new(input)

    records = []
    assert_nothing_raised do
      @parser.instance.parse_io(io) do |time, record|
        records << record
      end
    end

    assert_equal expected_records, records

    logs = @parser.logs.collect do |log|
      log.gsub(/\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [-+]\d{4} /, "")
    end
    assert_equal 1, logs.size
    assert_match(/\[warn\]: JSON stream ended in the middle of a document/, logs.first)
    assert_match(/discarding incomplete data/, logs.first)
    # The incomplete bytes themselves must never be logged, since a record
    # fragment may contain sensitive data.
    assert_not_match(/"b"/, logs.first)
    assert_not_match(/"a"/, logs.first)
  end

  data('no-trailing-space' => ['{"a":1}{"b":2}', [{"a" => 1}, {"b" => 2}]],
       'trailing-space'    => ['{"a":1} ',       [{"a" => 1}]])
  def test_parse_io_does_not_warn_on_complete_stream(data)
    input, expected_records = data
    @parser.configure('json_parser' => 'json')
    io = StringIO.new(input)

    records = []
    assert_nothing_raised do
      @parser.instance.parse_io(io) do |time, record|
        records << record
      end
    end

    assert_equal expected_records, records
    assert_equal [], @parser.logs
  end

  # Yajl silently accepted duplicate keys (last value wins). The json gem
  # warns on duplicate keys today and will raise in json 3.0 unless
  # `allow_duplicate_key: true` is passed. These tests are two-fold:
  # the last-wins assertions catch the json 3.0 raise, while the
  # no-deprecation-warning assertions catch a missing option on current
  # json 2.x, where duplicate keys still succeed with only a warning.
  sub_test_case "duplicate keys are allowed for Yajl compatibility" do
    DUPLICATE_KEY_JSON = '{"k":"a","k":"b"}'

    def test_parse_io_last_value_wins_without_deprecation_warning
      parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
      parser.configure('json_parser' => 'json')

      records = []
      warnings = with_captured_warnings do
        parser.instance.parse_io(StringIO.new(DUPLICATE_KEY_JSON)) do |_time, record|
          records << record
        end
      end

      assert_equal([{"k" => "b"}], records)
      assert_equal([], warnings.grep(/duplicate key/))
    end

    data('oj' => 'oj', 'json' => 'json', 'yajl' => 'yajl')
    def test_parse_last_value_wins_without_deprecation_warning(data)
      # Use a fresh parser per test to avoid the json gem's per-parser
      # deprecation warning cap (MAX_DEPRECATIONS) masking later warnings.
      parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
      parser.configure('json_parser' => data)

      records = []
      warnings = with_captured_warnings do
        parser.instance.parse(DUPLICATE_KEY_JSON) do |_time, record|
          records << record
        end
      end

      assert_equal([{"k" => "b"}], records)
      assert_equal([], warnings.grep(/duplicate key/))
    end

    def test_parse_io_multiple_documents_with_duplicated_keys
      parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::JSONParser)
      parser.configure('json_parser' => 'json')

      io = StringIO.new('{"k":"a","k":"b"}{"k":"c","k":"d"}')

      records = []
      warnings = with_captured_warnings do
        parser.instance.parse_io(io) do |_time, record|
          records << record
        end
      end

      assert_equal([{"k" => "b"}, {"k" => "d"}], records)
      assert_equal([], warnings.grep(/duplicate key/))
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
      parsed_records = []
      @parser.configure("json_parser" => "oj")
      @parser.instance.parse(data[:record]) { |time, record|
        parsed_records.append(record)
      }
      assert_equal(data[:expected], parsed_records)
    end

    def test_yajl(data)
      parsed_records = []
      @parser.configure("json_parser" => "yajl")
      @parser.instance.parse(data[:record]) { |time, record|
        parsed_records.append(record)
      }
      assert_equal(data[:expected], parsed_records)
    end

    def test_json(json)
      parsed_records = []
      @parser.configure("json_parser" => "json")
      @parser.instance.parse(data[:record]) { |time, record|
        parsed_records.append(record)
      }
      assert_equal(data[:expected], parsed_records)
    end
  end

  # This becomes NoMethodError if a non-Hash object is passed to convert_values.
  # https://github.com/fluent/fluentd/issues/4100
  sub_test_case "execute_convert_values with null_empty_string" do
    data("Only string", { record: '"message"', expected: [nil] }, keep: true)
    data(
      "Hash",
      {
        record: '{"k1": 1, "k2": ""}',
        expected: [{"k1" => 1, "k2" => nil}]
      },
      keep: true,
    )
    data(
      "Array of Hash",
      {
        record: '[{"k1": 1}, {"k2": ""}]',
        expected: [{"k1" => 1}, {"k2" => nil}]
      },
      keep: true,
    )

    def test_oj(data)
      parsed_records = []
      @parser.configure("json_parser" => "oj", "null_empty_string" => true)
      @parser.instance.parse(data[:record]) { |time, record|
        parsed_records.append(record)
      }
      assert_equal(data[:expected], parsed_records)
    end

    def test_yajl(data)
      parsed_records = []
      @parser.configure("json_parser" => "yajl", "null_empty_string" => true)
      @parser.instance.parse(data[:record]) { |time, record|
        parsed_records.append(record)
      }
      assert_equal(data[:expected], parsed_records)
    end

    def test_json(json)
      parsed_records = []
      @parser.configure("json_parser" => "json", "null_empty_string" => true)
      @parser.instance.parse(data[:record]) { |time, record|
        parsed_records.append(record)
      }
      assert_equal(data[:expected], parsed_records)
    end
  end
end
