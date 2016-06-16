require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class RegexpParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def internal_test_case(parser)
    text = '192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] [14/Feb/2013:12:00:00 +0900] "true /,/user HTTP/1.1" 200 777'
    parser.parse(text) { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
                     'user' => '-',
                     'flag' => true,
                     'code' => 200.0,
                     'size' => 777,
                     'date' => event_time('14/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'),
                     'host' => '192.168.0.1',
                     'path' => ['/', '/user']
                   }, record)
    }
  end

  def test_parse_with_typed
    # Use Regexp.new instead of // literal to avoid different parser behaviour in 1.9 and 2.0
    internal_test_case(Fluent::Plugin::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!), 'time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'))
  end

  def test_parse_with_configure
    # Specify conf by configure method instaed of intializer
    regexp = Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!)
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new(regexp))
    parser.configure('time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer')
    internal_test_case(parser.instance)
    assert_equal(regexp, parser.instance.patterns['format'])
    assert_equal("%d/%b/%Y:%H:%M:%S %z", parser.instance.patterns['time_format'])
  end

  def test_parse_with_typed_and_name_separator
    internal_test_case(Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new(Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!), 'time_format'=>"%d/%b/%Y:%H:%M:%S %z", 'types'=>'user|string,date|time|%d/%b/%Y:%H:%M:%S %z,flag|bool,path|array,code|float,size|integer', 'types_label_delimiter'=>'|')).instance)
  end

  def test_parse_with_time_key
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new(/(?<logtime>[^\]]*)/))
    parser.instance.configure(
                              'time_format'=>"%Y-%m-%d %H:%M:%S %z",
                              'time_key'=>'logtime',
                              )
    text = '2013-02-28 12:00:00 +0900'
    parser.instance.parse(text) do |time, record|
      assert_equal Fluent::EventTime.parse(text), time
    end
  end

  def test_parse_without_time
    time_at_start = Time.now.to_i
    text = "tagomori_satoshi tagomoris 34\n"

    parser = Fluent::Plugin::RegexpParser.new(Regexp.new(%q!^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$!))
    parser.configure('types'=>'name:string,user:string,age:integer')

    parser.parse(text) { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal "tagomori_satoshi", record["name"]
      assert_equal "tagomoris", record["user"]
      assert_equal 34, record["age"]
    }

    parser2 = Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new(Regexp.new(%q!^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$!)))
    parser2.configure('types'=>'name:string,user:string,age:integer')
    parser2.instance.estimate_current_event = false

    parser2.instance.parse(text) { |time, record|
      assert_equal "tagomori_satoshi", record["name"]
      assert_equal "tagomoris", record["user"]
      assert_equal 34, record["age"]

      assert_nil time, "parser returns nil if configured so"
    }
  end

  def test_parse_with_keep_time_key
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new(
                                                Regexp.new(%q!(?<time>.*)!),
                                                'time_format'=>"%d/%b/%Y:%H:%M:%S %z",
                                                'keep_time_key'=>'true',
                                                )
                                              )
    text = '28/Feb/2013:12:00:00 +0900'
    parser.instance.parse(text) do |time, record|
      assert_equal text, record['time']
    end
  end

  def test_parse_with_keep_time_key_with_typecast
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new(
                                                Regexp.new(%q!(?<time>.*)!),
                                                'time_format'=>"%d/%b/%Y:%H:%M:%S %z",
                                                'keep_time_key'=>'true',
                                                'types'=>'time:time:%d/%b/%Y:%H:%M:%S %z',
                                                )
                                              )
    text = '28/Feb/2013:12:00:00 +0900'
    parser.instance.parse(text) do |time, record|
      assert_equal 1362020400, record['time']
    end
  end
end
