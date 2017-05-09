require_relative 'helper'
require 'fluent/test'
require 'fluent/time'

class TimeParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def test_call_with_parse
    parser = Fluent::TimeParser.new

    assert(parser.parse('2013-09-18 12:00:00 +0900').is_a?(Fluent::EventTime))

    time = event_time('2013-09-18 12:00:00 +0900')
    assert_equal(time, parser.parse('2013-09-18 12:00:00 +0900'))
  end

  def test_parse_with_strptime
    parser = Fluent::TimeParser.new('%d/%b/%Y:%H:%M:%S %z')

    assert(parser.parse('28/Feb/2013:12:00:00 +0900').is_a?(Fluent::EventTime))

    time = event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z')
    assert_equal(time, parser.parse('28/Feb/2013:12:00:00 +0900'))
  end

  def test_parse_nsec_with_strptime
    parser = Fluent::TimeParser.new('%d/%b/%Y:%H:%M:%S:%N %z')

    assert(parser.parse('28/Feb/2013:12:00:00:123456789 +0900').is_a?(Fluent::EventTime))

    time = event_time('28/Feb/2013:12:00:00:123456789 +0900', format: '%d/%b/%Y:%H:%M:%S:%N %z')
    assert_equal_event_time(time, parser.parse('28/Feb/2013:12:00:00:123456789 +0900'))
  end

  def test_parse_iso8601
    parser = Fluent::TimeParser.new('%iso8601')

    assert(parser.parse('2017-01-01T12:00:00+09:00').is_a?(Fluent::EventTime))

    time = event_time('2017-01-01T12:00:00+09:00')
    assert_equal(time, parser.parse('2017-01-01T12:00:00+09:00'))

    time_with_msec = event_time('2017-01-01T12:00:00.123+09:00')
    assert_equal(time_with_msec, parser.parse('2017-01-01T12:00:00.123+09:00'))
  end

  def test_parse_with_invalid_argument
    parser = Fluent::TimeParser.new

    [[], {}, nil, true, 10000, //, ->{}, '', :symbol].each { |v|
      assert_raise Fluent::TimeParser::TimeParseError do
        parser.parse(v)
      end
    }
  end

  def test_parse_time_in_localtime
    time = with_timezone("UTC+02") do
      parser = Fluent::TimeParser.new("%Y-%m-%d %H:%M:%S.%N", true)
      parser.parse("2016-09-02 18:42:31.123456789")
    end
    assert_equal_event_time(time, event_time("2016-09-02 18:42:31.123456789 -02:00", format: '%Y-%m-%d %H:%M:%S.%N %z'))
  end

  def test_parse_time_in_utc
    time = with_timezone("UTC-09") do
      parser = Fluent::TimeParser.new("%Y-%m-%d %H:%M:%S.%N", false)
      parser.parse("2016-09-02 18:42:31.123456789")
    end
    assert_equal_event_time(time, event_time("2016-09-02 18:42:31.123456789 UTC", format: '%Y-%m-%d %H:%M:%S.%N %z'))
  end

  def test_parse_string_with_expected_timezone
    time = with_timezone("UTC-09") do
      parser = Fluent::TimeParser.new("%Y-%m-%d %H:%M:%S.%N", nil, "-07:00")
      parser.parse("2016-09-02 18:42:31.123456789")
    end
    assert_equal_event_time(time, event_time("2016-09-02 18:42:31.123456789 -07:00", format: '%Y-%m-%d %H:%M:%S.%N %z'))
  end

  sub_test_case 'TimeMixin::Parser' do
    class DummyForTimeParser
      include Fluent::Configurable
      include Fluent::TimeMixin::Parser
    end

    test 'provides configuration parameters for TimeParser with default values for localtime' do
      time = with_timezone("UTC+07") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse'))

        assert_nil   i.time_format
        assert_true  i.localtime
        assert_false i.utc
        assert_nil   i.timezone

        parser = i.time_parser_create
        # time_format unspecified
        # localtime
        parser.parse("2016-09-02 18:42:31.012345678")
      end
      assert_equal_event_time(event_time("2016-09-02 18:42:31.012345678 -07:00", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test 'provides configuration parameters for TimeParser, configurable for any time format' do
      time = with_timezone("UTC+07") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S %N'}))
        parser = i.time_parser_create
        # time_format specified
        # localtime
        parser.parse("09/02/2016 18-42-31 012345678")
      end
      assert_equal_event_time(event_time("2016-09-02 18:42:31.012345678 -07:00", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test 'provides configuration parameters for TimeParser, configurable for UTC by localtime=false' do
      time = with_timezone("UTC+07") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S %N', 'localtime' => 'false'}))
        parser = i.time_parser_create
        # time_format specified
        # utc
        parser.parse("09/02/2016 18-42-31 012345678")
      end
      assert_equal_event_time(event_time("2016-09-02 18:42:31.012345678 UTC", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test 'provides configuration parameters for TimeParser, configurable for UTC by utc=true' do
      time = with_timezone("UTC+07") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S %N', 'utc' => 'true'}))
        parser = i.time_parser_create
        # time_format specified
        # utc
        parser.parse("09/02/2016 18-42-31 012345678")
      end
      assert_equal_event_time(event_time("2016-09-02 18:42:31.012345678 UTC", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test 'provides configuration parameters for TimeParser, configurable for any timezone' do
      time = with_timezone("UTC+07") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S %N', 'timezone' => '-01:00'}))
        parser = i.time_parser_create
        # time_format specified
        # -01:00
        parser.parse("09/02/2016 18-42-31 012345678")
      end
      assert_equal_event_time(event_time("2016-09-02 18:42:31.012345678 -01:00", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test 'specifying timezone without time format raises configuration error' do
      assert_raise Fluent::ConfigError.new("specifying timezone requires time format") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'utc' => 'true'}))
        i.time_parser_create
      end
      assert_raise Fluent::ConfigError.new("specifying timezone requires time format") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'localtime' => 'false'}))
        i.time_parser_create
      end
      assert_raise Fluent::ConfigError.new("specifying timezone requires time format") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'timezone' => '-0700'}))
        i.time_parser_create
      end
    end

    test '#time_parser_create returns TimeParser with specified time format and timezone' do
      time = with_timezone("UTC-09") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S %N'}))
        assert_equal '%m/%d/%Y %H-%M-%S %N', i.time_format
        assert_true i.localtime
        parser = i.time_parser_create(format: '%Y-%m-%d %H:%M:%S.%N %z')
        parser.parse("2016-09-05 17:59:38.987654321 -03:00")
      end
      assert_equal_event_time(event_time("2016-09-05 17:59:38.987654321 -03:00", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test '#time_parser_create returns TimeParser with localtime when specified it forcedly besides any configuration parameters' do
      time = with_timezone("UTC-09") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S', 'utc' => 'true'}))
        assert_equal '%m/%d/%Y %H-%M-%S', i.time_format
        assert_true i.utc
        parser = i.time_parser_create(format: '%Y-%m-%d %H:%M:%S.%N', force_localtime: true)
        parser.parse("2016-09-05 17:59:38.987654321")
      end
      assert_equal_event_time(event_time("2016-09-05 17:59:38.987654321 +09:00", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)

      time = with_timezone("UTC-09") do
        i = DummyForTimeParser.new
        i.configure(config_element('parse', '', {'time_format' => '%m/%d/%Y %H-%M-%S', 'timezone' => '+0000'}))
        assert_equal '%m/%d/%Y %H-%M-%S', i.time_format
        assert_equal '+0000', i.timezone
        parser = i.time_parser_create(format: '%Y-%m-%d %H:%M:%S.%N', force_localtime: true)
        parser.parse("2016-09-05 17:59:38.987654321")
      end
      assert_equal_event_time(event_time("2016-09-05 17:59:38.987654321 +09:00", format: '%Y-%m-%d %H:%M:%S.%N %z'), time)
    end

    test '#time_parser_create returns NumericTimeParser to parse time as unixtime when time_type unixtime specified' do
      i = DummyForTimeParser.new
      i.configure(config_element('parse', '', {'time_type' => 'unixtime'}))
      parser = i.time_parser_create
      time = event_time("2016-10-03 20:08:30.123456789 +0100", format: '%Y-%m-%d %H:%M:%S.%N %z')
      assert_equal_event_time(Fluent::EventTime.new(time.to_i), parser.parse("#{time.sec}"))
    end

    test '#time_parser_create returns NumericTimeParser to parse time as float when time_type float specified' do
      i = DummyForTimeParser.new
      i.configure(config_element('parse', '', {'time_type' => 'float'}))
      parser = i.time_parser_create
      time = event_time("2016-10-03 20:08:30.123456789 +0100", format: '%Y-%m-%d %H:%M:%S.%N %z')
      assert_equal_event_time(time, parser.parse("#{time.sec}.#{time.nsec}"))
    end
  end
end
