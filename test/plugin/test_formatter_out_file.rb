require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_out_file'

class OutFileFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = {})
    d = Fluent::Test::Driver::Formatter.new(Fluent::Plugin::OutFileFormatter)
    case conf
    when Fluent::Config::Element
      d.configure(conf)
    when Hash
      d.configure({'utc' => true}.merge(conf))
    else
      d.configure(conf)
    end
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome'}
  end

  data('both true' => 'true', 'both false' => 'false')
  def test_configured_with_both_of_utc_and_localtime(value)
    assert_raise(Fluent::ConfigError.new("both of utc and localtime are specified, use only one of them")) do
      create_driver({'utc' => value, 'localtime' => value})
    end
  end

  time_i = Time.parse("2016-07-26 21:08:30 -0700").to_i
  data(
    'configured for localtime by localtime' => ['localtime', 'true',  time_i, "2016-07-26T21:08:30-07:00"],
    'configured for localtime by utc'       => ['utc',       'false', time_i, "2016-07-26T21:08:30-07:00"],
    'configured for utc by localtime'       => ['localtime', 'false', time_i, "2016-07-27T04:08:30Z"],
    'configured for utc by utc'             => ['utc',       'true',  time_i, "2016-07-27T04:08:30Z"],
  )
  def test_configured_with_utc_or_localtime(data)
    key, value, time_i, expected = data
    time = Time.at(time_i)
    begin
      oldtz, ENV['TZ'] = ENV['TZ'], "UTC+07"
      d = create_driver(config_element('ROOT', '', {key => value}))
      tag = 'test'
      assert_equal "#{expected}\t#{tag}\t#{Yajl.dump(record)}\n", d.instance.format(tag, time, record)
    ensure
      ENV['TZ'] = oldtz
    end
  end

  def test_format
    d = create_driver({})
    formatted = d.instance.format(tag, @time, record)

    assert_equal("#{time2str(@time)}\t#{tag}\t#{Yajl.dump(record)}\n", formatted)
  end

  def test_format_without_time
    d = create_driver('output_time' => 'false')
    formatted = d.instance.format(tag, @time, record)

    assert_equal("#{tag}\t#{Yajl.dump(record)}\n", formatted)
  end

  def test_format_without_tag
    d = create_driver('output_tag' => 'false')
    formatted = d.instance.format(tag, @time, record)

    assert_equal("#{time2str(@time)}\t#{Yajl.dump(record)}\n", formatted)
  end

  def test_format_without_time_and_tag
    d = create_driver('output_tag' => 'false', 'output_time' => 'false')
    formatted = d.instance.format('tag', @time, record)

    assert_equal("#{Yajl.dump(record)}\n", formatted)
  end

  def test_format_without_time_and_tag_against_string_literal_configure
    d = create_driver(%[
                        utc         true
                        output_tag  false
                        output_time false
                      ])
    formatted = d.instance.format('tag', @time, record)

    assert_equal("#{Yajl.dump(record)}\n", formatted)
  end
end
