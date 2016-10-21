require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'
require 'json'
require 'timecop'

class ParserTest < ::Test::Unit::TestCase
  class ExampleParser < Fluent::Plugin::Parser
    def parse(data)
      r = JSON.parse(data)
      yield convert_values(parse_time(r), r)
    end
  end

  def create_driver(conf={})
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::Parser).configure(conf)
  end

  def setup
    Fluent::Test.setup
  end

  sub_test_case 'base class works as plugin' do
    def test_init
      i = Fluent::Plugin::Parser.new
      assert_nil i.types
      assert_nil i.null_value_pattern
      assert !i.null_empty_string
      assert i.estimate_current_event
      assert !i.keep_time_key
    end

    def test_configure_against_string_literal
      d = create_driver('keep_time_key true')
      assert_true d.instance.keep_time_key
    end

    def test_parse
      d = create_driver
      assert_raise NotImplementedError do
        d.instance.parse('')
      end
    end
  end

  sub_test_case '#string_like_null' do
    setup do
      @i = ExampleParser.new
    end

    test 'returns false if null_empty_string is false and null_value_regexp is nil' do
      assert ! @i.string_like_null('a', false, nil)
      assert ! @i.string_like_null('', false, nil)
    end

    test 'returns true if null_empty_string is true and string value is empty' do
      assert ! @i.string_like_null('a', true, nil)
      assert @i.string_like_null('', true, nil)
    end

    test 'returns true if null_value_regexp has regexp and it matches string value' do
      assert ! @i.string_like_null('a', false, /null/i)
      assert @i.string_like_null('NULL', false, /null/i)
      assert @i.string_like_null('empty', false, /null|empty/i)
    end
  end

  sub_test_case '#build_type_converters converters' do
    setup do
      @i = ExampleParser.new
      types_config = {
        "s" => "string",
        "i" => "integer",
        "f" => "float",
        "b" => "bool",
        "t1" => "time",
        "t2" => "time:%Y-%m-%d %H:%M:%S.%N",
        "t3" => "time:+0100:%Y-%m-%d %H:%M:%S.%N",
        "t4" => "time:unixtime",
        "t5" => "time:float",
        "a1" => "array",
        "a2" => "array:|",
      }
      @hash = {
        'types' => JSON.dump(types_config),
      }
    end

    test 'to do #to_s by "string" type' do
      @i.configure(config_element('parse', '', @hash))
      c = @i.type_converters["s"]
      assert_equal "", c.call("")
      assert_equal "a", c.call("a")
      assert_equal "1", c.call(1)
      assert_equal "1.01", c.call(1.01)
      assert_equal "true", c.call(true)
      assert_equal "false", c.call(false)
    end

    test 'to do #to_i by "integer" type' do
      @i.configure(config_element('parse', '', @hash))
      c = @i.type_converters["i"]
      assert_equal 0, c.call("")
      assert_equal 0, c.call("0")
      assert_equal 0, c.call("a")
      assert_equal(-1000, c.call("-1000"))
      assert_equal 1, c.call(1)
      assert_equal 1, c.call(1.01)
      assert_equal 0, c.call(true)
      assert_equal 0, c.call(false)
    end

    test 'to do #to_f by "float" type' do
      @i.configure(config_element('parse', '', @hash))
      c = @i.type_converters["f"]
      assert_equal 0.0, c.call("")
      assert_equal 0.0, c.call("0")
      assert_equal 0.0, c.call("a")
      assert_equal(-1000.0, c.call("-1000"))
      assert_equal 1.0, c.call(1)
      assert_equal 1.01, c.call(1.01)
      assert_equal 0.0, c.call(true)
      assert_equal 0.0, c.call(false)
    end

    test 'to return true/false, which returns true only for true/yes/1 (C & perl style), by "bool"' do
      @i.configure(config_element('parse', '', @hash))
      c = @i.type_converters["b"]
      assert_false c.call("")
      assert_false c.call("0")
      assert_false c.call("a")
      assert_true c.call("1")
      assert_true c.call("true")
      assert_true c.call("True")
      assert_true c.call("YES")
      assert_true c.call(true)
      assert_false c.call(false)
      assert_false c.call("1.0")
    end

    test 'to parse time string by ruby default time parser without any options' do
      # "t1" => "time",
      with_timezone("UTC+02") do # -0200
        @i.configure(config_element('parse', '', @hash))
        c = @i.type_converters["t1"]
        assert_nil c.call("")
        assert_equal_event_time event_time("2016-10-21 01:54:30 -0200"), c.call("2016-10-21 01:54:30")
        assert_equal_event_time event_time("2016-10-21 03:54:30 -0200"), c.call("2016-10-21 01:54:30 -0400")
        assert_equal_event_time event_time("2016-10-21 01:55:24 -0200"), c.call("2016-10-21T01:55:24-02:00")
        assert_equal_event_time event_time("2016-10-21 01:55:24 -0200"), c.call("2016-10-21T03:55:24Z")
      end
    end

    test 'to parse time string with specified time format' do
      # "t2" => "time:%Y-%m-%d %H:%M:%S.%N",
      with_timezone("UTC+02") do # -0200
        @i.configure(config_element('parse', '', @hash))
        c = @i.type_converters["t2"]
        assert_nil c.call("")
        assert_equal_event_time event_time("2016-10-21 01:54:30.123000000 -0200"), c.call("2016-10-21 01:54:30.123")
        assert_equal_event_time event_time("2016-10-21 01:54:30.012345678 -0200"), c.call("2016-10-21 01:54:30.012345678")
        assert_nil c.call("2016/10/21 015430")
      end
    end

    test 'to parse time string with specified time format and timezone' do
      # "t3" => "time:+0100:%Y-%m-%d %H:%M:%S.%N",
      with_timezone("UTC+02") do # -0200
        @i.configure(config_element('parse', '', @hash))
        c = @i.type_converters["t3"]
        assert_nil c.call("")
        assert_equal_event_time event_time("2016-10-21 01:54:30.123000000 +0100"), c.call("2016-10-21 01:54:30.123")
        assert_equal_event_time event_time("2016-10-21 01:54:30.012345678 +0100"), c.call("2016-10-21 01:54:30.012345678")
      end
    end

    test 'to parse time string in unix timestamp' do
      # "t4" => "time:unixtime",
      with_timezone("UTC+02") do # -0200
        @i.configure(config_element('parse', '', @hash))
        c = @i.type_converters["t4"]
        assert_equal_event_time event_time("1970-01-01 00:00:00.0 +0000"), c.call("")
        assert_equal_event_time event_time("2016-10-21 01:54:30.0 -0200"), c.call("1477022070")
        assert_equal_event_time event_time("2016-10-21 01:54:30.0 -0200"), c.call("1477022070.01")
      end
    end

    test 'to parse time string in floating poing value' do
      # "t5" => "time:float",
      with_timezone("UTC+02") do # -0200
        @i.configure(config_element('parse', '', @hash))
        c = @i.type_converters["t5"]
        assert_equal_event_time event_time("1970-01-01 00:00:00.0 +0000"), c.call("")
        assert_equal_event_time event_time("2016-10-21 01:54:30.012 -0200"), c.call("1477022070.012")
        assert_equal_event_time event_time("2016-10-21 01:54:30.123456789 -0200"), c.call("1477022070.123456789")
      end
    end

    test 'to return array of string' do
      @i.configure(config_element('parse', '', @hash))
      c = @i.type_converters["a1"]
      assert_equal [], c.call("")
      assert_equal ["0"], c.call("0")
      assert_equal ["0"], c.call(0)
      assert_equal ["0", "1"], c.call("0,1")
      assert_equal ["0|1", "2"], c.call("0|1,2")
      assert_equal ["true"], c.call(true)
    end

    test 'to return array of string using specified delimiter' do
      @i.configure(config_element('parse', '', @hash))
      c = @i.type_converters["a2"]
      assert_equal [], c.call("")
      assert_equal ["0"], c.call("0")
      assert_equal ["0"], c.call(0)
      assert_equal ["0,1"], c.call("0,1")
      assert_equal ["0", "1,2"], c.call("0|1,2")
      assert_equal ["true"], c.call(true)
    end
  end

  sub_test_case 'example parser without any configurations' do
    setup do
      @current_time = Time.parse("2016-10-21 14:22:01.0 +1000")
      @current_event_time = Fluent::EventTime.new(@current_time.to_i, 0)
      # @current_time.to_i #=> 1477023721
      Timecop.freeze(@current_time)
      @i = ExampleParser.new
      @i.configure(config_element('parse', '', {}))
    end

    teardown do
      Timecop.return
    end

    test 'parser returns parsed JSON object, leaving empty/NULL strings, with current time' do
      json = '{"t1":"1477023720.101","s1":"","s2":"NULL","s3":"null","k1":1,"k2":"13.1","k3":"1","k4":"yes"}'
      @i.parse(json) do |time, record|
        assert_equal_event_time @current_event_time, time
        assert_equal "1477023720.101", record["t1"]
        assert_equal "", record["s1"]
        assert_equal "NULL", record["s2"]
        assert_equal "null", record["s3"]
        assert_equal 1, record["k1"]
        assert_equal "13.1", record["k2"]
        assert_equal "1", record["k3"]
        assert_equal "yes", record["k4"]
      end
    end
  end

  sub_test_case 'example parser fully configured' do
    setup do
      @current_time = Time.parse("2016-10-21 14:22:01.0 +1000")
      @current_event_time = Fluent::EventTime.new(@current_time.to_i, 0)
      # @current_time.to_i #=> 1477023721
      Timecop.freeze(@current_time)
      @i = ExampleParser.new
      hash = {
        'keep_time_key' => "no",
        'estimate_current_event' => "yes",
        'time_key' => "t1",
        'time_type' => "float",
        'null_empty_string' => 'yes',
        'null_value_pattern' => 'NULL|null',
        'types' => "k1:string, k2:integer, k3:float, k4:bool",
      }
      @i.configure(config_element('parse', '', hash))
    end

    teardown do
      Timecop.return
    end

    test 'parser returns parsed JSON object, leaving empty/NULL strings, with current time' do
      json = '{"t1":"1477023720.101","s1":"","s2":"NULL","s3":"null","k1":1,"k2":"13.1","k3":"1","k4":"yes"}'
      @i.parse(json) do |time, record|
        assert_equal_event_time Fluent::EventTime.new(1477023720, 101_000_000), time
        assert !record.has_key?("t1")
        assert{ record.has_key?("s1") && record["s1"].nil? }
        assert{ record.has_key?("s2") && record["s2"].nil? }
        assert{ record.has_key?("s3") && record["s3"].nil? }
        assert_equal "1", record["k1"]
        assert_equal 13, record["k2"]
        assert_equal 1.0, record["k3"]
        assert_equal true, record["k4"]
      end
    end

    test 'parser returns current time if a field is missing specified by time_key' do
      json = '{"s1":"","s2":"NULL","s3":"null","k1":1,"k2":"13.1","k3":"1","k4":"yes"}'
      @i.parse(json) do |time, record|
        assert_equal_event_time @current_event_time, time
        assert !record.has_key?("t1")
        assert{ record.has_key?("s1") && record["s1"].nil? }
        assert{ record.has_key?("s2") && record["s2"].nil? }
        assert{ record.has_key?("s3") && record["s3"].nil? }
        assert_equal "1", record["k1"]
        assert_equal 13, record["k2"]
        assert_equal 1.0, record["k3"]
        assert_equal true, record["k4"]
      end
    end
  end

  sub_test_case 'example parser configured not to estimate current time, and to keep time key' do
    setup do
      @current_time = Time.parse("2016-10-21 14:22:01.0 +1000")
      @current_event_time = Fluent::EventTime.new(@current_time.to_i, 0)
      # @current_time.to_i #=> 1477023721
      Timecop.freeze(@current_time)
      @i = ExampleParser.new
      hash = {
        'keep_time_key' => "yes",
        'estimate_current_event' => "no",
        'time_key' => "t1",
        'time_type' => "float",
        'null_empty_string' => 'yes',
        'null_value_pattern' => 'NULL|null',
        'types' => "k1:string, k2:integer, k3:float, k4:bool",
      }
      @i.configure(config_element('parse', '', hash))
    end

    teardown do
      Timecop.return
    end

    test 'parser returns parsed time with original field and value if the field of time exists' do
      json = '{"t1":"1477023720.101","s1":"","s2":"NULL","s3":"null","k1":1,"k2":"13.1","k3":"1","k4":"yes"}'
      @i.parse(json) do |time, record|
        assert_equal_event_time Fluent::EventTime.new(1477023720, 101_000_000), time
        assert_equal "1477023720.101", record["t1"]
        assert{ record.has_key?("s1") && record["s1"].nil? }
        assert{ record.has_key?("s2") && record["s2"].nil? }
        assert{ record.has_key?("s3") && record["s3"].nil? }
        assert_equal "1", record["k1"]
        assert_equal 13, record["k2"]
        assert_equal 1.0, record["k3"]
        assert_equal true, record["k4"]
      end
    end

    test 'parser returns nil as time if the field of time is missing' do
      json = '{"s1":"","s2":"NULL","s3":"null","k1":1,"k2":"13.1","k3":"1","k4":"yes"}'
      @i.parse(json) do |time, record|
        assert_nil time
        assert !record.has_key?("t1")
        assert{ record.has_key?("s1") && record["s1"].nil? }
        assert{ record.has_key?("s2") && record["s2"].nil? }
        assert{ record.has_key?("s3") && record["s3"].nil? }
        assert_equal "1", record["k1"]
        assert_equal 13, record["k2"]
        assert_equal 1.0, record["k3"]
        assert_equal true, record["k4"]
      end
    end
  end
end
