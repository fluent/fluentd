require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser_apache_error'

class ApacheErrorParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @expected = {
      'level' => 'error',
      'client' => '127.0.0.1',
      'message' => 'client denied by server configuration'
    }
  end

  def create_driver
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::ApacheErrorParser.new).configure({})
  end

  def test_parse
    d = create_driver
    d.instance.parse('[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied by server configuration') { |time, record|
      assert_equal(event_time('Wed Oct 11 14:32:52 2000'), time)
      assert_equal(@expected, record)
    }
  end

  def test_parse_with_pid
    d = create_driver
    d.instance.parse('[Wed Oct 11 14:32:52 2000] [error] [pid 1000] [client 127.0.0.1] client denied by server configuration') { |time, record|
      assert_equal(event_time('Wed Oct 11 14:32:52 2000'), time)
      assert_equal(@expected.merge('pid' => '1000'), record)
    }
  end

  def test_parse_without_client
    d = create_driver
    d.instance.parse('[Wed Oct 11 14:32:52 2000] [notice] Apache/2.2.15 (Unix) DAV/2 configured -- resuming normal operations') { |time, record|
      assert_equal(event_time('Wed Oct 11 14:32:52 2000'), time)
      assert_equal({
                     'level' => 'notice',
                     'message' => 'Apache/2.2.15 (Unix) DAV/2 configured -- resuming normal operations'
                   }, record)
    }
  end
end
