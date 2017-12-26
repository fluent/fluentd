require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class LabeledTSVParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def test_config_params
    parser = Fluent::Test::Driver::Parser.new(Fluent::TextParser::LabeledTSVParser)

    assert_equal "\t", parser.instance.delimiter
    assert_equal  ":", parser.instance.label_delimiter

    parser.configure(
                     'delimiter'       => ',',
                     'label_delimiter' => '=',
                     )

    assert_equal ",", parser.instance.delimiter
    assert_equal "=", parser.instance.label_delimiter
  end

  def test_parse
    parser = Fluent::Test::Driver::Parser.new(Fluent::TextParser::LabeledTSVParser)
    parser.configure({})
    parser.instance.parse("time:2013/02/28 12:00:00\thost:192.168.0.1\treq_id:111") { |time, record|
      assert_equal(event_time('2013/02/28 12:00:00', format: '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }
  end

  def test_parse_with_customized_delimiter
    parser = Fluent::Test::Driver::Parser.new(Fluent::TextParser::LabeledTSVParser)
    parser.configure(
                     'delimiter'       => ',',
                     'label_delimiter' => '=',
                     )
    parser.instance.parse('time=2013/02/28 12:00:00,host=192.168.0.1,req_id=111') { |time, record|
      assert_equal(event_time('2013/02/28 12:00:00', format: '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }
  end

  def test_parse_with_customized_time_format
    parser = Fluent::Test::Driver::Parser.new(Fluent::TextParser::LabeledTSVParser)
    parser.configure(
                     'time_key'    => 'mytime',
                     'time_format' => '%d/%b/%Y:%H:%M:%S %z',
                     )
    parser.instance.parse("mytime:28/Feb/2013:12:00:00 +0900\thost:192.168.0.1\treq_id:111") { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }
  end

  def test_parse_without_time
    time_at_start = Time.now.to_i

    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::LabeledTSVParser)
    parser.configure({})
    parser.instance.parse("host:192.168.0.1\treq_id:111") { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
    }

    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::LabeledTSVParser)
    parser.configure({'estimate_current_event' => 'no'})
    parser.instance.parse("host:192.168.0.1\treq_id:111") { |time, record|
      assert_equal({
                     'host'   => '192.168.0.1',
                     'req_id' => '111',
                   }, record)
      assert_nil time, "parser returns nil w/o time and if configured so"
    }
  end

  def test_parse_with_keep_time_key
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::LabeledTSVParser)
    parser.configure(
                     'time_format'=>"%d/%b/%Y:%H:%M:%S %z",
                     'keep_time_key'=>'true',
                     )
    text = '28/Feb/2013:12:00:00 +0900'
    parser.instance.parse("time:#{text}") do |time, record|
      assert_equal text, record['time']
    end
  end

  def test_parse_with_null_value_pattern
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::LabeledTSVParser)
    parser.configure(
                     'null_value_pattern'=>'^(-|null|NULL)$'
                     )
    parser.instance.parse("a:-\tb:null\tc:NULL\td:\te:--\tf:nuLL") do |time, record|
      assert_nil record['a']
      assert_nil record['b']
      assert_nil record['c']
      assert_equal record['d'], ''
      assert_equal record['e'], '--'
      assert_equal record['f'], 'nuLL'
    end
  end

  def test_parse_with_null_empty_string
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::LabeledTSVParser)
    parser.configure(
                     'null_empty_string'=>true
                     )
    parser.instance.parse("a:\tb: ") do |time, record|
      assert_nil record['a']
      assert_equal record['b'], ' '
    end
  end

  data("single space" => ["k1=v1 k2=v2", { "k1" => "v1", "k2" => "v2" }],
       "multiple space" => ["k1=v1    k2=v2", { "k1" => "v1", "k2" => "v2" }],
       "reverse" => ["k2=v2 k1=v1", { "k1" => "v1", "k2" => "v2" }],
       "tab" => ["k2=v2\tk1=v1", { "k1" => "v1", "k2" => "v2" }],
       "tab and space" => ["k2=v2\t k1=v1", { "k1" => "v1", "k2" => "v2" }])
  def test_parse_with_delimiter_pattern(data)
    text, expected = data
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::LabeledTSVParser)
    parser.configure(
                     'delimiter_pattern' => '/\s+/',
                     'label_delimiter' => '='
                    )
    parser.instance.parse(text) do |_time, record|
      assert_equal(expected, record)
    end
  end
end
