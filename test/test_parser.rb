require 'helper'
require 'fluent/test'
require 'fluent/parser'

module ParserTest
  include Fluent

  def str2time(str_time, format = nil)
    if format
      Time.strptime(str_time, format).to_i
    else
      Time.parse(str_time).to_i
    end
  end

  class TimeParserTest < ::Test::Unit::TestCase
    include ParserTest

    def test_call_with_parse
      parser = TextParser::TimeParser.new(nil)

      time = str2time('2013-09-18 12:00:00 +0900')
      assert_equal(time, parser.parse('2013-09-18 12:00:00 +0900'))
    end

    def test_call_with_strptime
      parser = TextParser::TimeParser.new('%d/%b/%Y:%H:%M:%S %z')

      time = str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z')
      assert_equal(time, parser.parse('28/Feb/2013:12:00:00 +0900'))
    end

    def test_call_with_invalid_argument
      parser = TextParser::TimeParser.new(nil)

      [[], {}, nil, true, 10000].each { |v|
        assert_raise Fluent::TextParser::ParserError do
          parser.parse(v)
        end
      }
    end
  end

  class RegexpParserTest < ::Test::Unit::TestCase
    include ParserTest

    def internal_test_case(parser)
      text = '192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] [14/Feb/2013:12:00:00 +0900] "true /,/user HTTP/1.1" 200 777'
      [parser.call(text), parser.call(text) { |time, record| return time, record}].each { |time, record|
        assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal({
          'user' => '-',
          'flag' => true,
          'code' => 200.0,
          'size' => 777,
          'date' => str2time('14/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'),
          'host' => '192.168.0.1',
          'path' => ['/', '/user']
        }, record)
      }
    end

    def test_call_with_typed
      # Use Regexp.new instead of // literal to avoid different parser behaviour in 1.9 and 2.0 
      internal_test_case(TextParser::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!), 'time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'))
    end

    def test_call_with_configure
      # Specify conf by configure method instaed of intializer
      regexp = Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!)
      parser = TextParser::RegexpParser.new(regexp)
      parser.configure('time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer')
      internal_test_case(parser)
      assert_equal(regexp, parser.patterns['format'])
      assert_equal("%d/%b/%Y:%H:%M:%S %z", parser.patterns['time_format'])
    end

    def test_call_with_typed_and_name_separator
      internal_test_case(TextParser::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!), 'time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user|string,date|time|%d/%b/%Y:%H:%M:%S %z,flag|bool,path|array,code|float,size|integer', 'types_label_delimiter'=>'|'))
    end

    def test_call_without_time
      time_at_start = Time.now.to_i
      text = "tagomori_satoshi tagomoris 34\n"

      parser = TextParser::RegexpParser.new(Regexp.new(%q!^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$!))
      parser.configure('types'=>'name:string,user:string,age:bool')

      [parser.call(text), parser.call(text) { |time, record| return time, record}].each { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal "tagomori_satoshi", record["name"]
        assert_equal "tagomoris", record["user"]
        assert_equal 34, record["age"]
      }

      parser2 = TextParser::RegexpParser.new(Regexp.new(%q!^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$!))
      parser2.configure('types'=>'name:string,user:string,age:bool')
      parser2.time_default_current = false

      [parser2.call(text), parser2.call(text) { |time, record| return time, record}].each { |time, record|
        assert_equal "tagomori_satoshi", record["name"]
        assert_equal "tagomoris", record["user"]
        assert_equal 34, record["age"]

        assert_nil time, "parser returns nil if configured so"
      }

    end
  end

  class ApacheParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_REGISTRY.lookup('apache').call
    end

    def test_call
      @parser.call('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777') { |time, record|
        assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal({
          'user'    => '-',
          'method'  => 'GET',
          'code'    => '200',
          'size'    => '777',
          'host'    => '192.168.0.1',
          'path'    => '/'
        }, record)
      }
    end
  end

  class ApacheErrorParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_REGISTRY.lookup('apache_error').call
      @expected = {
        'level' => 'error',
        'client' => '127.0.0.1',
        'message' => 'client denied by server configuration'
      }
    end

    def test_call
      @parser.call('[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied by server configuration') { |time, record|
        assert_equal(str2time('Wed Oct 11 14:32:52 2000'), time)
        assert_equal(@expected, record)
      }
    end

    def test_call_with_pid
      @parser.call('[Wed Oct 11 14:32:52 2000] [error] [pid 1000] [client 127.0.0.1] client denied by server configuration') { |time, record|
        assert_equal(str2time('Wed Oct 11 14:32:52 2000'), time)
        assert_equal(@expected.merge('pid' => '1000'), record)
      }
    end

    def test_call_without_client
      @parser.call('[Wed Oct 11 14:32:52 2000] [notice] Apache/2.2.15 (Unix) DAV/2 configured -- resuming normal operations') { |time, record|
        assert_equal(str2time('Wed Oct 11 14:32:52 2000'), time)
        assert_equal({
          'level' => 'notice',
          'message' => 'Apache/2.2.15 (Unix) DAV/2 configured -- resuming normal operations'
        }, record)
      }
    end
  end

  class Apache2ParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::ApacheParser.new
    end

    def test_call
      @parser.call('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"') { |time, record|
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
      }
      assert_equal(TextParser::ApacheParser::REGEXP, @parser.patterns['format'])
      assert_equal(TextParser::ApacheParser::TIME_FORMAT, @parser.patterns['time_format'])
    end
  end

  class SyslogParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_REGISTRY.lookup('syslog').call
      @expected = {
        'host'    => '192.168.0.1',
        'ident'   => 'fluentd',
        'pid'     => '11111',
        'message' => '[error] Syslog test'
      }
    end

    def test_call
      @parser.configure({})
      @parser.call('Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
        assert_equal(str2time('Feb 28 12:00:00', '%b %d %H:%M:%S'), time)
        assert_equal(@expected, record)
      }
      assert_equal(TextParser::SyslogParser::REGEXP, @parser.patterns['format'])
      assert_equal("%b %d %H:%M:%S", @parser.patterns['time_format'])
    end

    def test_call_with_time_format
      @parser.configure('time_format' => '%b %d %M:%S:%H')
      @parser.call('Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
        assert_equal(str2time('Feb 28 12:00:00', '%b %d %H:%M:%S'), time)
        assert_equal(@expected, record)
      }
      assert_equal('%b %d %M:%S:%H', @parser.patterns['time_format'])
    end

    def test_call_with_priority
      @parser.configure('with_priority' => true)
      @parser.call('<6>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
        assert_equal(str2time('Feb 28 12:00:00', '%b %d %H:%M:%S'), time)
        assert_equal(@expected.merge('pri' => 6), record)
      }
      assert_equal(TextParser::SyslogParser::REGEXP_WITH_PRI, @parser.patterns['format'])
      assert_equal("%b %d %H:%M:%S", @parser.patterns['time_format'])
    end

    def test_call_without_colon
      @parser.configure({})
      @parser.call('Feb 28 12:00:00 192.168.0.1 fluentd[11111] [error] Syslog test') { |time, record|
        assert_equal(str2time('Feb 28 12:00:00', '%b %d %H:%M:%S'), time)
        assert_equal(@expected, record)
      }
      assert_equal(TextParser::SyslogParser::REGEXP, @parser.patterns['format'])
      assert_equal("%b %d %H:%M:%S", @parser.patterns['time_format'])
    end
  end

  class JsonParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::JSONParser.new
    end

    def test_call
      @parser.call('{"time":1362020400,"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
        assert_equal(str2time('2013-02-28 12:00:00 +0900').to_i, time)
        assert_equal({
          'host'   => '192.168.0.1',
          'size'   => 777,
          'method' => 'PUT',
        }, record)
      }
    end

    def test_call_without_time
      time_at_start = Time.now.to_i

      @parser.call('{"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal({
          'host'   => '192.168.0.1',
          'size'   => 777,
          'method' => 'PUT',
        }, record)
      }

      parser = TextParser::JSONParser.new
      parser.estimate_current_event = false
      parser.configure({})
      parser.call('{"host":"192.168.0.1","size":777,"method":"PUT"}') { |time, record|
        assert_equal({
          'host'   => '192.168.0.1',
          'size'   => 777,
          'method' => 'PUT',
        }, record)
        assert_nil time, "parser return nil w/o time and if specified so"
      }
    end

    def test_call_with_invalid_time
      assert_raise Fluent::TextParser::ParserError do
        @parser.call('{"time":[],"k":"v"}') { |time, record| }
      end
    end
  end

  class NginxParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_REGISTRY.lookup('nginx').call
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
      @parser.call('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"') { |time, record|
        assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal(@expected, record)
      }
    end

    def test_call_with_empty_included_path
      @parser.call('127.0.0.1 192.168.0.1 - [28/Feb/2013:12:00:00 +0900] "GET /a[ ]b HTTP/1.1" 200 777 "-" "Opera/12.0"') { |time, record|
        assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal(@expected.merge('path' => '/a[ ]b'), record)
      }
    end
  end

  class TSVParserTest < ::Test::Unit::TestCase
    include ParserTest

    def test_config_params
      parser = TextParser::TSVParser.new

      assert_equal "\t", parser.delimiter

      parser.configure(
        'keys' => 'a,b',
        'delimiter' => ',',
      )

      assert_equal ",", parser.delimiter
    end

    def test_call
      parser = TextParser::TSVParser.new
      parser.configure('keys' => 'time,a,b', 'time_key' => 'time')
      parser.call("2013/02/28 12:00:00\t192.168.0.1\t111") { |time, record|
        assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
        assert_equal({
          'a' => '192.168.0.1',
          'b' => '111',
        }, record)
      }
    end

    def test_call_with_time
      time_at_start = Time.now.to_i

      parser = TextParser::TSVParser.new
      parser.configure('keys' => 'a,b')
      parser.call("192.168.0.1\t111") { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal({
          'a' => '192.168.0.1',
          'b' => '111',
        }, record)
      }

      parser = TextParser::TSVParser.new
      parser.estimate_current_event = false
      parser.configure('keys' => 'a,b', 'time_key' => 'time')
      parser.call("192.168.0.1\t111") { |time, record|
        assert_equal({
          'a' => '192.168.0.1',
          'b' => '111',
        }, record)
        assert_nil time, "parser returns nil w/o time and if configured so"
      }
    end
  end

  class CSVParserTest < ::Test::Unit::TestCase
    include ParserTest

    def test_call
      parser = TextParser::CSVParser.new
      parser.configure('keys' => 'time,c,d', 'time_key' => 'time')
      parser.call("2013/02/28 12:00:00,192.168.0.1,111") { |time, record|
        assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
        assert_equal({
          'c' => '192.168.0.1',
          'd' => '111',
        }, record)
      }
    end

    def test_call_without_time
      time_at_start = Time.now.to_i

      parser = TextParser::CSVParser.new
      parser.configure('keys' => 'c,d')
      parser.call("192.168.0.1,111") { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal({
          'c' => '192.168.0.1',
          'd' => '111',
        }, record)
      }

      parser = TextParser::CSVParser.new
      parser.estimate_current_event = false
      parser.configure('keys' => 'c,d', 'time_key' => 'time')
      parser.call("192.168.0.1,111") { |time, record|
        assert_equal({
          'c' => '192.168.0.1',
          'd' => '111',
        }, record)
        assert_nil time, "parser returns nil w/o time and if configured so"
      }
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
      parser.configure({})
      parser.call("time:2013/02/28 12:00:00\thost:192.168.0.1\treq_id:111") { |time, record|
        assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
        assert_equal({
          'host'   => '192.168.0.1',
          'req_id' => '111',
        }, record)
      }
    end

    def test_call_with_customized_delimiter
      parser = TextParser::LabeledTSVParser.new
      parser.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )
      parser.call('time=2013/02/28 12:00:00,host=192.168.0.1,req_id=111') { |time, record|
        assert_equal(str2time('2013/02/28 12:00:00', '%Y/%m/%d %H:%M:%S'), time)
        assert_equal({
          'host'   => '192.168.0.1',
          'req_id' => '111',
        }, record)
      }
    end

    def test_call_with_customized_time_format
      parser = TextParser::LabeledTSVParser.new
      parser.configure(
        'time_key'    => 'mytime',
        'time_format' => '%d/%b/%Y:%H:%M:%S %z',
      )
      parser.call("mytime:28/Feb/2013:12:00:00 +0900\thost:192.168.0.1\treq_id:111") { |time, record|
        assert_equal(str2time('28/Feb/2013:12:00:00 +0900', '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal({
          'host'   => '192.168.0.1',
          'req_id' => '111',
        }, record)
      }
    end

    def test_call_without_time
      time_at_start = Time.now.to_i

      parser = TextParser::LabeledTSVParser.new
      parser.configure({})
      parser.call("host:192.168.0.1\treq_id:111") { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal({
          'host'   => '192.168.0.1',
          'req_id' => '111',
        }, record)
      }

      parser = TextParser::LabeledTSVParser.new
      parser.estimate_current_event = false
      parser.configure({})
      parser.call("host:192.168.0.1\treq_id:111") { |time, record|
        assert_equal({
          'host'   => '192.168.0.1',
          'req_id' => '111',
        }, record)
        assert_nil time, "parser returns nil w/o time and if configured so"
      }
    end
 end

  class NoneParserTest < ::Test::Unit::TestCase
    include ParserTest

    def test_config_params
      parser = TextParser::NoneParser.new
      parser.configure({})
      assert_equal "message", parser.message_key

      parser.configure('message_key' => 'foobar')
      assert_equal "foobar", parser.message_key
    end

    def test_call
      parser = TextParser::TEMPLATE_REGISTRY.lookup('none').call
      parser.configure({})
      parser.call('log message!') { |time, record|
        assert_equal({'message' => 'log message!'}, record)
      }
    end

    def test_call_with_message_key
      parser = TextParser::NoneParser.new
      parser.configure('message_key' => 'foobar')
      parser.call('log message!') { |time, record|
        assert_equal({'foobar' => 'log message!'}, record)
      }
    end

    def test_call_without_default_time
      time_at_start = Time.now.to_i

      parser = TextParser::TEMPLATE_REGISTRY.lookup('none').call
      parser.configure({})
      parser.call('log message!') { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal({'message' => 'log message!'}, record)
      }

      parser = TextParser::TEMPLATE_REGISTRY.lookup('none').call
      parser.estimate_current_event = false
      parser.configure({})
      parser.call('log message!') { |time, record|
        assert_equal({'message' => 'log message!'}, record)
        assert_nil time, "parser returns nil w/o time if configured so"
      }
    end
  end

  class MultilineParserTest < ::Test::Unit::TestCase
    include ParserTest

    def create_parser(conf)
      parser = TextParser::TEMPLATE_REGISTRY.lookup('multiline').call
      parser.configure(conf)
      parser
    end

    def test_configure_with_invalid_params
      [{'format100' => '/(?<msg>.*)/'}, {'format1' => '/(?<msg>.*)/', 'format3' => '/(?<msg>.*)/'}, 'format1' => '/(?<msg>.*)'].each { |config|
        assert_raise(ConfigError) {
          create_parser(config)
        }
      }
    end

    def test_call
      parser = create_parser('format1' => '/^(?<time>\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}) \[(?<thread>.*)\] (?<level>[^\s]+)(?<message>.*)/')
      parser.call(<<EOS.chomp) { |time, record|
2013-3-03 14:27:33 [main] ERROR Main - Exception
javax.management.RuntimeErrorException: null
\tat Main.main(Main.java:16) ~[bin/:na]
EOS

        assert_equal(str2time('2013-3-03 14:27:33').to_i, time)
        assert_equal({
          "thread"  => "main",
          "level"   => "ERROR",
          "message" => " Main - Exception\njavax.management.RuntimeErrorException: null\n\tat Main.main(Main.java:16) ~[bin/:na]"
        }, record)
      }
    end

    def test_call_with_firstline
      parser = create_parser('format_firstline' => '/----/', 'format1' => '/time=(?<time>\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}).*message=(?<message>.*)/')
      parser.call(<<EOS.chomp) { |time, record|
----
time=2013-3-03 14:27:33 
message=test1
EOS

        assert(parser.firstline?('----'))
        assert_equal(str2time('2013-3-03 14:27:33').to_i, time)
        assert_equal({"message" => "test1"}, record)
      }
    end

    def test_call_with_multiple_formats
      parser = create_parser('format_firstline' => '/^Started/',
        'format1' => '/Started (?<method>[^ ]+) "(?<path>[^"]+)" for (?<host>[^ ]+) at (?<time>[^ ]+ [^ ]+ [^ ]+)\n/',
        'format2' => '/Processing by (?<controller>[^\u0023]+)\u0023(?<controller_method>[^ ]+) as (?<format>[^ ]+?)\n/',
        'format3' => '/(  Parameters: (?<parameters>[^ ]+)\n)?/',
        'format4' => '/  Rendered (?<template>[^ ]+) within (?<layout>.+) \([\d\.]+ms\)\n/',
        'format5' => '/Completed (?<code>[^ ]+) [^ ]+ in (?<runtime>[\d\.]+)ms \(Views: (?<view_runtime>[\d\.]+)ms \| ActiveRecord: (?<ar_runtime>[\d\.]+)ms\)/'
        )
      parser.call(<<EOS.chomp) { |time, record|
Started GET "/users/123/" for 127.0.0.1 at 2013-06-14 12:00:11 +0900
Processing by UsersController#show as HTML
  Parameters: {"user_id"=>"123"}
  Rendered users/show.html.erb within layouts/application (0.3ms)
Completed 200 OK in 4ms (Views: 3.2ms | ActiveRecord: 0.0ms)
EOS

        assert(parser.firstline?('Started GET "/users/123/" for 127.0.0.1...'))
        assert_equal(str2time('2013-06-14 12:00:11 +0900').to_i, time)
        assert_equal({
          "method" => "GET",
          "path" => "/users/123/",
          "host" => "127.0.0.1",
          "controller" => "UsersController",
          "controller_method" => "show",
          "format" => "HTML",
          "parameters" => "{\"user_id\"=>\"123\"}",
          "template" => "users/show.html.erb",
          "layout" => "layouts/application",
          "code" => "200",
          "runtime" => "4",
          "view_runtime" => "3.2",
          "ar_runtime" => "0.0"
        }, record)
      }
    end
  end

  class TextParserTest < ::Test::Unit::TestCase
    include ParserTest

    class MultiEventTestParser
      include Fluent::Configurable

      def call(text)
        2.times { |i|
          record = {}
          record['message'] = text
          record['number'] = i
          yield Fluent::Engine.now, record
        }
      end
    end

    TextParser.register_template('multi_event_test', Proc.new { MultiEventTestParser.new })

    def test_lookup_unknown_format
      assert_raise ConfigError do
        TextParser::TEMPLATE_REGISTRY.lookup('unknown')
      end
    end

    def test_lookup_known_parser
      $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(__FILE__)), 'scripts'))
      assert_nothing_raised ConfigError do
        TextParser::TEMPLATE_REGISTRY.lookup('known')
      end
    end

    def test_parse_with_return
      parser = TextParser.new
      parser.configure('format' => 'none')
      time, record = parser.parse('log message!')
      assert_equal({'message' => 'log message!'}, record)
    end

    def test_parse_with_block
      parser = TextParser.new
      parser.configure('format' => 'none')
      parser.parse('log message!') { |time, record|
        assert_equal({'message' => 'log message!'}, record)
      }
    end

    def test_multi_event_parser
      parser = TextParser.new
      parser.configure('format' => 'multi_event_test')
      i = 0
      parser.parse('log message!') { |time, record|
        assert_equal('log message!', record['message'])
        assert_equal(i, record['number'])
        i += 1
      }
    end

    def test_setting_estimate_current_event_value
      p1 = TextParser.new
      assert_nil p1.estimate_current_event
      assert_nil p1.parser

      p1.configure('format' => 'none')
      assert_equal true, p1.parser.estimate_current_event

      p2 = TextParser.new
      assert_nil p2.estimate_current_event
      assert_nil p2.parser

      p2.estimate_current_event = false

      p2.configure('format' => 'none')
      assert_equal false, p2.parser.estimate_current_event
    end
  end
end
