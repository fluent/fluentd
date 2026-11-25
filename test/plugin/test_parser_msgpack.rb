require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser_msgpack'

class MessagePackParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::MessagePackParser).configure(conf)
  end

  sub_test_case "simple setting" do
    data(
      "Normal Hash",
      {
        input: "\x82\xA7message\xADHello msgpack\xA3numd",
        expected: [{"message" => "Hello msgpack", "num" => 100}]
      },
      keep: true
    )
    data(
      "Array of multiple Hash",
      {
        input: "\x92\x81\xA7message\xA3foo\x81\xA7message\xA3bar",
        expected: [{"message"=>"foo"}, {"message"=>"bar"}]
      },
      keep: true
    )
    data(
      "String",
      {
        # "Hello msgpack".to_msgpack
        input: "\xADHello msgpack",
        expected: [nil]
      },
      keep: true
    )
    data(
      "Array of String",
      {
        # ["foo", "bar"].to_msgpack
        input: "\x92\xA3foo\xA3bar",
        expected: [nil, nil]
      },
      keep: true
    )
    data(
      "Array of String and Hash",
      {
        # ["foo", {message: "bar"}].to_msgpack
        input: "\x92\xA3foo\x81\xA7message\xA3bar",
        expected: [nil, {"message"=>"bar"}]
      },
      keep: true
    )

    def test_parse(data)
      parsed_records = []
      create_driver("").instance.parse(data[:input]) do |time, record|
        parsed_records.append(record)
      end
      assert_equal(data[:expected], parsed_records)
    end

    def test_parse_io(data)
      parsed_records = []
      StringIO.open(data[:input]) do |io|
        create_driver("").instance.parse_io(io) do |time, record|
          parsed_records.append(record)
        end
      end
      assert_equal(data[:expected], parsed_records)
    end
  end

  # This becomes NoMethodError if a non-Hash object is passed to convert_values.
  # https://github.com/fluent/fluentd/issues/4100
  sub_test_case "execute_convert_values with null_empty_string" do
    data(
      "Normal hash",
      {
        # {message: "foo", empty: ""}.to_msgpack
        input: "\x82\xA7message\xA3foo\xA5empty\xA0",
        expected: [{"message" => "foo", "empty" => nil}]
      },
      keep: true
    )
    data(
      "Array of multiple Hash",
      {
        # [{message: "foo", empty: ""}, {message: "bar", empty: ""}].to_msgpack
        input: "\x92\x82\xA7message\xA3foo\xA5empty\xA0\x82\xA7message\xA3bar\xA5empty\xA0",
        expected: [{"message"=>"foo", "empty" => nil}, {"message"=>"bar", "empty" => nil}]
      },
      keep: true
    )
    data(
      "String",
      {
        # "Hello msgpack".to_msgpack
        input: "\xADHello msgpack",
        expected: [nil]
      },
      keep: true
    )

    def test_parse(data)
      parsed_records = []
      create_driver("null_empty_string").instance.parse(data[:input]) do |time, record|
        parsed_records.append(record)
      end
      assert_equal(data[:expected], parsed_records)
    end

    def test_parse_io(data)
      parsed_records = []
      StringIO.open(data[:input]) do |io|
        create_driver("null_empty_string").instance.parse_io(io) do |time, record|
          parsed_records.append(record)
        end
      end
      assert_equal(data[:expected], parsed_records)
    end
  end

  sub_test_case "data left over from a previous call" do
    test "parse does not carry truncated data into the next call" do
      parser = create_driver("").instance
      parsed_records = []
      parser.parse("\x81\xA3ba") { |_time, record| parsed_records.append(record) }
      parser.parse("\x81\xA3bar\xA3bar") { |_time, record| parsed_records.append(record) }
      assert_equal([{"bar" => "bar"}], parsed_records)
    end

    test "parse does not carry objects left by an exception in the block into the next call" do
      parser = create_driver("").instance
      assert_raise(RuntimeError) do
        parser.parse("\x81\xA4from\xA1A\x81\xA4from\xA1B") { |_time, _record| raise "error in block" }
      end
      parsed_records = []
      parser.parse("\x81\xA4from\xA1C") { |_time, record| parsed_records.append(record) }
      assert_equal([{"from" => "C"}], parsed_records)
    end

    test "parse_partial_data continues decoding data split across calls" do
      parser = create_driver("").instance
      parsed_records = []
      parser.parse_partial_data("\x81\xA3ba") { |_time, record| parsed_records.append(record) }
      parser.parse_partial_data("r\xA3bar") { |_time, record| parsed_records.append(record) }
      assert_equal([{"bar" => "bar"}], parsed_records)
    end
  end

  test "parse decodes Fluent::EventTime extension type" do
    event_time = Fluent::EventTime.new(1700000000, 123)
    input = Fluent::MessagePackFactory.msgpack_packer.write({"message" => "foo", "time" => event_time}).to_s
    parsed_records = []
    create_driver("").instance.parse(input) { |_time, record| parsed_records.append(record) }
    assert_equal(1, parsed_records.size)
    assert_equal("foo", parsed_records[0]["message"])
    assert_equal_event_time(event_time, parsed_records[0]["time"])
  end

  # A bad time is skipped only while converting that record. The caller's block
  # stays outside the rescue, and parse itself still raises.
  sub_test_case "invalid time in one record" do
    def time_conf
      %[
        time_key t
        time_type string
        time_format %Y-%m-%d
      ]
    end

    def pack(*objects)
      packer = Fluent::MessagePackFactory.msgpack_packer
      objects.each { |object| packer.write(object) }
      packer.to_s
    end

    def warnings(driver)
      driver.logs.grep(/\[warn\]/)
    end

    def feed_stream(parser, bytes, method_name)
      parsed = []
      collect = ->(time, record) { parsed << [time, record] }
      case method_name
      when :parse_io
        StringIO.open(bytes) { |io| parser.parse_io(io, &collect) }
      when :parse_partial_data
        # Several calls, including splits in the middle of an object.
        offset = 0
        chunk_size = 4
        while offset < bytes.bytesize
          parser.parse_partial_data(bytes.byteslice(offset, chunk_size), &collect)
          offset += chunk_size
        end
      end
      parsed
    end

    data("parse_io" => :parse_io, "parse_partial_data" => :parse_partial_data)
    test "emits the records around a bad time and warns once" do |method_name|
      r1 = {"t" => "2020-01-01", "k" => "1"}
      bad = {"t" => "bad", "k" => "2"}
      r3 = {"t" => "2020-01-03", "k" => "3"}
      r4 = {"t" => "2020-01-04", "k" => "4"}
      bytes = pack([r1, bad, r3], r4)
      assert_operator bytes.bytesize, :>, 4 if method_name == :parse_partial_data

      driver = create_driver(time_conf)
      parsed = feed_stream(driver.instance, bytes, method_name)

      assert_equal([
        [event_time("2020-01-01", format: "%Y-%m-%d"), {"k" => "1"}],
        [event_time("2020-01-03", format: "%Y-%m-%d"), {"k" => "3"}],
        [event_time("2020-01-04", format: "%Y-%m-%d"), {"k" => "4"}],
      ], parsed)
      assert_equal(1, warnings(driver).size)
      assert_match(/\[warn\]: invalid record found error_class=\S+ error=/, warnings(driver).first)
    end

    data(
      "parse_io hash" => [:parse_io, :hash],
      "parse_io array" => [:parse_io, :array],
      "parse_partial_data hash" => [:parse_partial_data, :hash],
      "parse_partial_data array" => [:parse_partial_data, :array],
    )
    test "a ParserError from the caller escapes with no invalid-record warning" do |(method_name, shape)|
      record = {"t" => "2020-01-01", "k" => "1"}
      bytes = pack(shape == :array ? [record] : record)
      driver = create_driver(time_conf)
      parser = driver.instance
      raised = Fluent::Plugin::Parser::ParserError.new("from the caller")

      error = assert_raise(Fluent::Plugin::Parser::ParserError) do
        case method_name
        when :parse_io
          StringIO.open(bytes) { |io| parser.parse_io(io) { |_time, _record| raise raised } }
        when :parse_partial_data
          parser.parse_partial_data(bytes) { |_time, _record| raise raised }
        end
      end

      assert_same(raised, error)
      assert_equal([], warnings(driver).grep(/invalid record found/))
    end

    test "parse raises on the bad time after yielding the previous record" do
      r1 = {"t" => "2020-01-01", "k" => "1"}
      bad = {"t" => "bad", "k" => "2"}
      r3 = {"t" => "2020-01-03", "k" => "3"}
      driver = create_driver(time_conf)
      parsed = []

      error = assert_raise(Fluent::Plugin::Parser::ParserError) do
        driver.instance.parse(pack([r1, bad, r3])) do |_time, record|
          parsed << record
        end
      end

      assert_equal([{"k" => "1"}], parsed)
      assert_match(/invalid time format: value = bad/, error.message)
      assert_equal([], warnings(driver).grep(/invalid record found/))
    end
  end
end