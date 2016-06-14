require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class LabeledTSVParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def test_config_params
    parser = Fluent::TextParser::LabeledTSVParser.new

    assert_equal "\t", parser.delimiter
    assert_equal  ":", parser.label_delimiter

    parser.configure(
                     'delimiter'       => ',',
                     'label_delimiter' => '=',
                     )

    assert_equal ",", parser.delimiter
    assert_equal "=", parser.label_delimiter
  end

  def test_parse
    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure({})
    parser.parse("time:2013/02/28 12:00:00\thost:192.168.0.1\treq_id:111") { |time, record|
      assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }
  end

  def test_parse_with_customized_delimiter
    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure(
                     'delimiter'       => ',',
                     'label_delimiter' => '=',
                     )
    parser.parse('time=2013/02/28 12:00:00,host=192.168.0.1,req_id=111') { |time, record|
      assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }
  end

  def test_parse_with_customized_time_format
    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure(
                     'time_key'    => 'mytime',
                     'time_format' => '%d/%b/%Y:%H:%M:%S %z',
                     )
    parser.parse("mytime:28/Feb/2013:12:00:00 +0900\thost:192.168.0.1\treq_id:111") { |time, record|
      assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }
  end

  def test_parse_without_time
    time_at_start = Time.now.to_i

    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure({})
    parser.parse("host:192.168.0.1\treq_id:111") { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }

    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.estimate_current_event = false
    parser.configure({})
    parser.parse("host:192.168.0.1\treq_id:111") { |time, record|
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
      assert_nil time, "parser returns nil w/o time and if configured so"
    }
  end

  def test_parse_with_keep_time_key
    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure(
                     'time_format'=>"%d/%b/%Y:%H:%M:%S %z",
                     'keep_time_key'=>'true',
                     )
    text = '28/Feb/2013:12:00:00 +0900'
    parser.parse("time:#{text}") do |time, record|
      assert_equal text, record['time']
    end
  end

  def test_parse_with_null_value_pattern
    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure(
                     'null_value_pattern'=>'^(-|null|NULL)$'
                     )
    parser.parse("a:-\tb:null\tc:NULL\td:\te:--\tf:nuLL") do |time, record|
      assert_nil record['a']
      assert_nil record['b']
      assert_nil record['c']
      assert_equal record['d'], ''
      assert_equal record['e'], '--'
      assert_equal record['f'], 'nuLL'
    end
  end

  def test_parse_with_null_empty_string
    parser = Fluent::TextParser::LabeledTSVParser.new
    parser.configure(
                     'null_empty_string'=>true
                     )
    parser.parse("a:\tb: ") do |time, record|
      assert_nil record['a']
      assert_equal record['b'], ' '
    end
  end
end
