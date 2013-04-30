require 'helper'
require 'fluent/test'
require 'fluent/parser'

module ParserTest
  include Fluent

  def str2time(str_time, format = nil)
    if format
      Time.strptime(str_time, format).to_i
    else
      Time.parse(str_time)
    end
  end

  class ApacheParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_FACTORIES['apache'].call
    end

    def test_call
      time, record = @parser.call('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777')

      assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
        'user'    => '-',
        'method'  => 'GET',
        'code'    => '200',
        'size'    => '777',
        'host'    => '192.168.0.1',
        'path'    => '/'
      }, record)
    end
  end

  class Apache2ParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::ApacheParser.new
    end

    def test_call
      time, record = @parser.call('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"')

      assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
        'user'    => nil,
        'method'  => 'GET',
        'code'    => 200,
        'size'    => 777,
        'host'    => '192.168.0.1',
        'path'    => '/',
        'referer' => nil,
        'agent'   => 'Opera/12.0'
      }, record)
    end
  end

  class SyslogParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_FACTORIES['syslog'].call
    end

    def test_call
      time, record = @parser.call('Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test')

      assert_equal(str2time('Feb 28 12:00:00', '%b %d %H:%M:%S'), time)
      assert_equal({
        'host'    => '192.168.0.1',
        'ident'   => 'fluentd',
        'pid'     => '11111',
        'message' => '[error] Syslog test'
      }, record)
    end
  end

  class JsonParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::JSONParser.new
    end

    def test_call
      time, record = @parser.call('{"time":1362020400,"host":"192.168.0.1","size":777,"method":"PUT"}')

      assert_equal(str2time('2013-02-28 12:00:00 +0900').to_i, time)
      assert_equal({
        'host'   => '192.168.0.1',
        'size'   => 777,
        'method' => 'PUT',
      }, record)
    end
  end

  class NginxParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_FACTORIES['nginx'].call
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

    def test_call
      time, record = @parser.call('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"')

      assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected, record)
    end

    def test_call_with_empty_included_path
      time, record = @parser.call('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET /a[ ]b HTTP/1.1" 200 777 "-" "Opera/12.0"')

      assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal(@expected.merge('path' => '/a[ ]b'), record)
    end
  end

  class LabeledTSVParserTest < ::Test::Unit::TestCase
    include ParserTest

    def test_config_params
      parser = TextParser::LabeledTSVParser.new

      assert_equal "\t", parser.delimiter
      assert_equal  ":", parser.label_delimiter

      parser.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )

      assert_equal ",", parser.delimiter
      assert_equal "=", parser.label_delimiter
    end

    def test_call
      parser = TextParser::LabeledTSVParser.new
      time, record = parser.call("time:2013/02/28 12:00:00\thost:192.168.0.1\treq_id:111")

      assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
        'host'   => '192.168.0.1',
        'req_id' => '111',
      }, record)
    end

    def test_call_with_customized_delimiter
      parser = TextParser::LabeledTSVParser.new
      parser.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )
      time, record = parser.call('time=2013/02/28 12:00:00,host=192.168.0.1,req_id=111')

      assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
        'host'   => '192.168.0.1',
        'req_id' => '111',
      }, record)
    end

    def test_call_with_customized_time_format
      parser = TextParser::LabeledTSVParser.new
      parser.configure(
        'time_key'    => 'mytime',
        'time_format' => '%d/%b/%Y:%H:%M:%S %z',
      )
      time, record = parser.call("mytime:28/Feb/2013:12:00:00 +0900\thost:192.168.0.1\treq_id:111")

      assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
        'host'   => '192.168.0.1',
        'req_id' => '111',
      }, record)
    end
  end
end
