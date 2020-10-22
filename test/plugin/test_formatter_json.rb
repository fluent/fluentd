require_relative '../helper'
require 'json'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_json'

class JsonFormatterTest < ::Test::Unit::TestCase

  def setup
    @time = event_time
    @default_newline = if Fluent.windows?
                         "\r\n"
                       else
                         "\n"
                       end
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

  def symbolic_record
    {:message => :awesome}
  end

  data('oj with LF' => ['oj', "lf", "\n"],
       'oj with CRLF' => ['oj', "crlf", "\r\n"],
       'yajl with LF' => ['yajl', "lf", "\n"],
       'yajl with CRLF' => ['yajl', "crlf", "\r\n"]
      )
  def test_format(data)
    parser, newline_conf, newline = data
    d = create_driver('json_parser' => parser, 'newline' => newline_conf)
    formatted = d.instance.format(tag, @time, record)

    assert_equal("#{JSON.generate(record)}#{newline}", formatted)
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_format_without_nl(data)
    d = create_driver('json_parser' => data, 'add_newline' => false)
    formatted = d.instance.format(tag, @time, record)

    assert_equal(JSON.generate(record), formatted)
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_format_with_symbolic_record(data)
    d = create_driver('json_parser' => data)
    formatted = d.instance.format(tag, @time, symbolic_record)

    assert_equal("#{JSON.generate(record)}#{@default_newline}", formatted)
  end
end
