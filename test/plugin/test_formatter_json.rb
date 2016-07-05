require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_json'

class JsonFormatterTest < ::Test::Unit::TestCase

  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::JSONFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome'}
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_format(data)
    d = create_driver('json_parser' => data)
    formatted = d.instance.format(tag, @time, record)

    assert_equal("#{Yajl.dump(record)}\n", formatted)
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_format_with_include_tag(data)
    d = create_driver('include_tag_key' => 'true', 'tag_key' => 'foo', 'json_parser' => data)
    formatted = d.instance.format(tag, @time, record.dup)

    r = record
    r['foo'] = tag
    assert_equal("#{Yajl.dump(r)}\n", formatted)
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_format_with_include_time(data)
    d = create_driver('include_time_key' => 'true', 'localtime' => '', 'json_parser' => data)
    formatted = d.instance.format(tag, @time, record.dup)

    r = record
    r['time'] = time2str(@time, localtime: true)
    assert_equal("#{Yajl.dump(r)}\n", formatted)
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_format_with_include_time_as_number(data)
    d = create_driver('include_time_key' => 'true', 'time_as_epoch' => 'true', 'time_key' => 'epoch', 'json_parser' => data)
    formatted = d.instance.format(tag, @time, record.dup)

    r = record
    r['epoch'] = @time
    assert_equal("#{Yajl.dump(r)}\n", formatted)
  end
end
