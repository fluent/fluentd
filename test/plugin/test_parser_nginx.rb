require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser_nginx'

class NginxParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @expected = {
      'remote'  => '127.0.0.1',
      'host'    => '192.168.0.1',
      'user'    => '-',
      'method'  => 'GET',
      'path'    => '/',
      'code'    => '200',
      'size'    => '777',
      'referer' => '-',
      'agent'   => 'Opera/12.0'
    }
  end

  def create_driver
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::NginxParser.new).configure({})
  end

  def test_parse
    d = create_driver
    d.instance.parse('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"') { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected, record)
    }
  end

  def test_parse_with_empty_included_path
    d = create_driver
    d.instance.parse('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET /a[ ]b HTTP/1.1" 200 777 "-" "Opera/12.0"') { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected.merge('path' => '/a[ ]b'), record)
    }
  end

  def test_parse_without_http_version
    d = create_driver
    d.instance.parse('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET /" 200 777 "-" "Opera/12.0"') { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected, record)
    }
  end
end
