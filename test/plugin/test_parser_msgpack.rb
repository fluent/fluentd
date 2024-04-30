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
end