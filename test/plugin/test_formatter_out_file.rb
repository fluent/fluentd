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
