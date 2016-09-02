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
end
