require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_msgpack'

class MessagePackFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::MessagePackFormatter).configure(conf)
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

    assert_equal(record.to_msgpack, formatted)
  end

  def test_format_with_include_tag
    d = create_driver('include_tag_key' => 'true', 'tag_key' => 'foo')
    formatted = d.instance.format(tag, @time, record.dup)

    r = record
    r['foo'] = tag
    assert_equal(r.to_msgpack, formatted)
  end

  def test_format_with_include_time
    d = create_driver('include_time_key' => 'true', 'localtime' => '')
    formatted = d.instance.format(tag, @time, record.dup)

    r = record
    r['time'] = time2str(@time, localtime: true)
    assert_equal(r.to_msgpack, formatted)
  end

  def test_format_with_include_time_as_number
    d = create_driver('include_time_key' => 'true', 'time_as_epoch' => 'true', 'time_key' => 'epoch')
    formatted = d.instance.format(tag, @time, record.dup)

    r = record
    r['epoch'] = @time
    assert_equal(r.to_msgpack, formatted)
  end
end
