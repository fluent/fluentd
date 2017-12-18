require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class Apache2ParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::Apache2Parser)
    @expected = {
      'user'    => nil,
      'method'  => 'GET',
      'code'    => 200,
      'size'    => 777,
      'host'    => '192.168.0.1',
      'path'    => '/',
      'referer' => nil,
      'agent'   => 'Opera/12.0'
    }
    @parser.configure({})
  end

  def test_parse
    @parser.instance.parse('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"') { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected, record)
    }
    assert_equal(Fluent::Plugin::Apache2Parser::REGEXP,
                 @parser.instance.patterns['format'])
    assert_equal(Fluent::Plugin::Apache2Parser::TIME_FORMAT,
                 @parser.instance.patterns['time_format'])
  end

  def test_parse_without_http_version
    @parser.instance.parse('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET /" 200 777 "-" "Opera/12.0"') { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected, record)
    }
  end

  def test_parse_with_escape_sequence
    @parser.instance.parse('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET /\" HTTP/1.1" 200 777 "referer \\\ \"" "user agent \\\ \""') { |_, record|
      assert_equal('/\"', record['path'])
      assert_equal('referer \\\ \"', record['referer'])
      assert_equal('user agent \\\ \"', record['agent'])
    }
  end
end
