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
        assert_raise ArgumentError do
          parser.parse(v)
        end
      }
    end
  end

  class RegexpParserTest < ::Test::Unit::TestCase
    include ParserTest

    def internal_test_case(parser)
      time, record = parser.call('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] [14/Feb/2013:12:00:00 +0900] "true /,/user HTTP/1.1" 200 777')

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
    end

    def test_call_with_typed
      # Use Regexp.new instead of // literal to avoid different parser behaviour in 1.9 and 2.0 
      internal_test_case(TextParser::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!), 'time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'))
    end

    def test_call_with_configure
      # Specify conf by configure method instaed of intializer
      parser = TextParser::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!))
      parser.configure('time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer')
      internal_test_case(parser)
    end

    def test_call_with_typed_and_name_separator
      internal_test_case(TextParser::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!), 'time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user|string,date|time|%d/%b/%Y:%H:%M:%S %z,flag|bool,path|array,code|float,size|integer', 'types_label_delimiter'=>'|'))
    end
  end

  class ApacheParserTest < ::Test::Unit::TestCase
    include ParserTest

    def setup
      @parser = TextParser::TEMPLATE_REGISTRY.lookup('apache').call
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
      @parser = TextParser::TEMPLATE_REGISTRY.lookup('syslog').call
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
      parser.configure({})
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

  class NoneParserTest < ::Test::Unit::TestCase
    include ParserTest

    def test_config_params
      parser = TextParser::NoneParser.new
      assert_equal "message", parser.message_key

      parser.configure('message_key' => 'foobar')
      assert_equal "foobar", parser.message_key
    end

    def test_call
      parser = TextParser::TEMPLATE_REGISTRY.lookup('none').call
      time, record = parser.call('log message!')

      assert_equal({'message' => 'log message!'}, record)
    end

    def test_call_with_message_key
      parser = TextParser::NoneParser.new
      parser.configure('message_key' => 'foobar')
      time, record = parser.call('log message!')

      assert_equal({'foobar' => 'log message!'}, record)
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
      time, record = parser.call(<<EOS.chomp)
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
    end

    def test_call_with_firstline
      parser = create_parser('format_firstline' => '/----/', 'format1' => '/time=(?<time>\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}).*message=(?<message>.*)/')
      time, record = parser.call(<<EOS.chomp)
----
time=2013-3-03 14:27:33 
message=test1
EOS

      assert(parser.firstline?('----'))
      assert_equal(str2time('2013-3-03 14:27:33').to_i, time)
      assert_equal({"message" => "test1"}, record)
    end

    def test_call_with_multiple_formats
      parser = create_parser('format_firstline' => '/^Started/',
        'format1' => '/Started (?<method>[^ ]+) "(?<path>[^"]+)" for (?<host>[^ ]+) at (?<time>[^ ]+ [^ ]+ [^ ]+)\n/',
        'format2' => '/Processing by (?<controller>[^\u0023]+)\u0023(?<controller_method>[^ ]+) as (?<format>[^ ]+?)\n/',
        'format3' => '/(  Parameters: (?<parameters>[^ ]+)\n)?/',
        'format4' => '/  Rendered (?<template>[^ ]+) within (?<layout>.+) \([\d\.]+ms\)\n/',
        'format5' => '/Completed (?<code>[^ ]+) [^ ]+ in (?<runtime>[\d\.]+)ms \(Views: (?<view_runtime>[\d\.]+)ms \| ActiveRecord: (?<ar_runtime>[\d\.]+)ms\)/'
        )
      time, record = parser.call(<<EOS.chomp)
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
    end
  end

  class ParserLookupTest < ::Test::Unit::TestCase
    include ParserTest

    def test_unknown_format
      assert_raise ConfigError do
        TextParser::TEMPLATE_REGISTRY.lookup('unknown')
      end
    end

    def test_find_parser
      $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(__FILE__)), 'scripts'))
      assert_nothing_raised ConfigError do
        TextParser::TEMPLATE_REGISTRY.lookup('known')
      end
    end
  end
end
