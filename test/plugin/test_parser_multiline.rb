require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class MultilineParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_parser(conf)
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::MultilineParser).configure(conf)
    parser
  end

  def test_configure_with_invalid_params
    [{'format100' => '/(?<msg>.*)/'}, {'format1' => '/(?<msg>.*)/', 'format3' => '/(?<msg>.*)/'}, 'format1' => '/(?<msg>.*)'].each { |config|
      assert_raise(Fluent::ConfigError) {
        create_parser(config)
      }
    }
  end

  def test_parse
    parser = create_parser('format1' => '/^(?<time>\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}) \[(?<thread>.*)\] (?<level>[^\s]+)(?<message>.*)/')
    parser.instance.parse(<<EOS.chomp) { |time, record|
2013-3-03 14:27:33 [main] ERROR Main - Exception
javax.management.RuntimeErrorException: null
\tat Main.main(Main.java:16) ~[bin/:na]
EOS

      assert_equal(event_time('2013-3-03 14:27:33').to_i, time)
      assert_equal({
                     "thread"  => "main",
                     "level"   => "ERROR",
                     "message" => " Main - Exception\njavax.management.RuntimeErrorException: null\n\tat Main.main(Main.java:16) ~[bin/:na]"
                   }, record)
    }
  end

  def test_parse_with_firstline
    parser = create_parser('format_firstline' => '/----/', 'format1' => '/time=(?<time>\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}).*message=(?<message>.*)/')
    parser.instance.parse(<<EOS.chomp) { |time, record|
----
time=2013-3-03 14:27:33
message=test1
EOS

      assert(parser.instance.firstline?('----'))
      assert_equal(event_time('2013-3-03 14:27:33').to_i, time)
      assert_equal({"message" => "test1"}, record)
    }
  end

  def test_parse_with_multiple_formats
    parser = create_parser('format_firstline' => '/^Started/',
                           'format1' => '/Started (?<method>[^ ]+) "(?<path>[^"]+)" for (?<host>[^ ]+) at (?<time>[^ ]+ [^ ]+ [^ ]+)\n/',
                           'format2' => '/Processing by (?<controller>[^\u0023]+)\u0023(?<controller_method>[^ ]+) as (?<format>[^ ]+?)\n/',
                           'format3' => '/(  Parameters: (?<parameters>[^ ]+)\n)?/',
                           'format4' => '/  Rendered (?<template>[^ ]+) within (?<layout>.+) \([\d\.]+ms\)\n/',
                           'format5' => '/Completed (?<code>[^ ]+) [^ ]+ in (?<runtime>[\d\.]+)ms \(Views: (?<view_runtime>[\d\.]+)ms \| ActiveRecord: (?<ar_runtime>[\d\.]+)ms\)/'
                           )
    parser.instance.parse(<<EOS.chomp) { |time, record|
Started GET "/users/123/" for 127.0.0.1 at 2013-06-14 12:00:11 +0900
Processing by UsersController#show as HTML
  Parameters: {"user_id"=>"123"}
  Rendered users/show.html.erb within layouts/application (0.3ms)
Completed 200 OK in 4ms (Views: 3.2ms | ActiveRecord: 0.0ms)
EOS

      assert(parser.instance.firstline?('Started GET "/users/123/" for 127.0.0.1...'))
      assert_equal(event_time('2013-06-14 12:00:11 +0900').to_i, time)
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

  def test_parse_with_keep_time_key
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::MultilineParser).configure(
      'format1' => '/^(?<time>\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})/',
      'keep_time_key' => 'true'
    )
    text = '2013-3-03 14:27:33'
    parser.instance.parse(text) { |time, record|
      assert_equal text, record['time']
    }
  end
end
