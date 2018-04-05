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

  sub_test_case "Fluent::Compat::TextParser::RegexpParser" do
    def create_driver(regexp, conf = {}, initialize_conf: false)
      if initialize_conf
        Fluent::Test::Driver::Parser.new(Fluent::Compat::TextParser::RegexpParser.new(regexp, conf))
      else
        # Fluent::Test::Driver::Parser.new(Fluent::Compat::TextParser::RegexpParser.new(regexp)).configure(conf)
        instance = Fluent::Compat::TextParser::RegexpParser.new(regexp)
        instance.configure(conf)
        d = Struct.new(:instance).new
        d.instance = instance
        d
      end
    end

    def test_parse_with_typed
      # Use Regexp.new instead of // literal to avoid different parser behaviour in 1.9 and 2.0
      regexp = Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!)
      conf = {
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'types' => 'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'
      }
      d = create_driver(regexp, conf, initialize_conf: true)
      internal_test_case(d.instance)
    end

    def test_parse_with_configure
      # Specify conf by configure method instead of initializer
      regexp = Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!)
      conf = {
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'types' => 'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'
      }
      d = create_driver(regexp, conf)
      internal_test_case(d.instance)
      assert_equal(regexp, d.instance.patterns['format'])
      assert_equal("%d/%b/%Y:%H:%M:%S %z", d.instance.patterns['time_format'])
    end

    def test_parse_with_typed_and_name_separator
      regexp = Regexp.new(%q!^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$!)
      conf = {
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'types' => 'user|string,date|time|%d/%b/%Y:%H:%M:%S %z,flag|bool,path|array,code|float,size|integer',
        'types_label_delimiter' => '|'
      }
      d = create_driver(regexp, conf)
      internal_test_case(d.instance)
    end

    def test_parse_with_time_key
      conf = {
        'time_format' => "%Y-%m-%d %H:%M:%S %z",
        'time_key' => 'logtime'
      }
      d = create_driver(/(?<logtime>[^\]]*)/, conf)
      text = '2013-02-28 12:00:00 +0900'
      d.instance.parse(text) do |time, _record|
        assert_equal Fluent::EventTime.parse(text), time
      end
    end

    def test_parse_without_time
      time_at_start = Time.now.to_i
      text = "tagomori_satoshi tagomoris 34\n"

      regexp = Regexp.new(%q!^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$!)
      conf = {
        'types' => 'name:string,user:string,age:integer'
      }
      d = create_driver(regexp, conf)

      d.instance.parse(text) { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal "tagomori_satoshi", record["name"]
        assert_equal "tagomoris", record["user"]
        assert_equal 34, record["age"]
      }
    end

    def test_parse_without_time_estimate_curent_event_false
      text = "tagomori_satoshi tagomoris 34\n"
      regexp = Regexp.new(%q!^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$!)
      conf = {
        'types' => 'name:string,user:string,age:integer'
      }
      d = create_driver(regexp, conf)
      d.instance.estimate_current_event = false
      d.instance.parse(text) { |time, record|
        assert_equal "tagomori_satoshi", record["name"]
        assert_equal "tagomoris", record["user"]
        assert_equal 34, record["age"]

        assert_nil time, "parser returns nil if configured so"
      }
    end

    def test_parse_with_keep_time_key
      regexp = Regexp.new(%q!(?<time>.*)!)
      conf = {
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'keep_time_key' => 'true',
      }
      d = create_driver(regexp, conf)
      text = '28/Feb/2013:12:00:00 +0900'
      d.instance.parse(text) do |_time, record|
        assert_equal text, record['time']
      end
    end

    def test_parse_with_keep_time_key_with_typecast
      regexp = Regexp.new(%q!(?<time>.*)!)
      conf = {
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'keep_time_key' => 'true',
        'types' => 'time:time:%d/%b/%Y:%H:%M:%S %z',
      }
      d = create_driver(regexp, conf)
      text = '28/Feb/2013:12:00:00 +0900'
      d.instance.parse(text) do |_time, record|
        assert_equal 1362020400, record['time']
      end
    end
  end

  sub_test_case "Fluent::Plugin::RegexpParser" do
    def create_driver(conf)
      Fluent::Test::Driver::Parser.new(Fluent::Plugin::RegexpParser.new).configure(conf)
    end

    sub_test_case "configure" do
      def test_default_options
        conf = {
          'expression' => %q!/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$/!,
          'time_format' => "%d/%b/%Y:%H:%M:%S %z",
          'types' => 'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'
        }
        d = create_driver(conf)
        regexp = d.instance.expression
        assert_equal(0, regexp.options)
      end

      data(
        ignorecase: ["i", Regexp::IGNORECASE],
        multiline: ["m", Regexp::MULTILINE],
        ignorecase_multiline: ["im", Regexp::IGNORECASE | Regexp::MULTILINE],
      )
      def test_options(data)
        regexp_option, expected = data
        conf = {
          'expression' => %Q!/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$/#{regexp_option}!,
          'time_format' => "%d/%b/%Y:%H:%M:%S %z",
          'types' => 'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'
        }
        d = create_driver(conf)
        regexp = d.instance.expression
        assert_equal(expected, regexp.options)
      end
    end

    def test_parse_with_typed
      conf = {
        'expression' => %q!/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$/!,
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'types' => 'user:string,date:time:%d/%b/%Y:%H:%M:%S %z,flag:bool,path:array,code:float,size:integer'
      }
      d = create_driver(conf)
      internal_test_case(d.instance)
    end

    def test_parse_with_typed_by_json_hash
      conf = {
        'expression' => %q!/^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] \[(?<date>[^\]]*)\] "(?<flag>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)$/!,
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'types' => '{"user":"string","date":"time:%d/%b/%Y:%H:%M:%S %z","flag":"bool","path":"array","code":"float","size":"integer"}',
      }
      d = create_driver(conf)
      internal_test_case(d.instance)
    end

    def test_parse_with_time_key
      conf = {
        'expression' => %q!/(?<logtime>[^\]]*)/!,
        'time_format' => "%Y-%m-%d %H:%M:%S %z",
        'time_key' => 'logtime'
      }
      d = create_driver(conf)
      text = '2013-02-28 12:00:00 +0900'
      d.instance.parse(text) do |time, _record|
        assert_equal Fluent::EventTime.parse(text), time
      end
    end

    def test_parse_without_time
      time_at_start = Time.now.to_i
      text = "tagomori_satoshi tagomoris 34\n"

      conf = {
        'expression' => %q!/^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$/!,
        'types' => 'name:string,user:string,age:integer'
      }
      d = create_driver(conf)

      d.instance.parse(text) { |time, record|
        assert time && time >= time_at_start, "parser puts current time without time input"
        assert_equal "tagomori_satoshi", record["name"]
        assert_equal "tagomoris", record["user"]
        assert_equal 34, record["age"]
      }
    end

    def test_parse_without_time_estimate_curent_event_false
      text = "tagomori_satoshi tagomoris 34\n"
      conf = {
        'expression' => %q!/^(?<name>[^ ]*) (?<user>[^ ]*) (?<age>\d*)$/!,
        'types' => 'name:string,user:string,age:integer'
      }
      d = create_driver(conf)
      d.instance.estimate_current_event = false
      d.instance.parse(text) { |time, record|
        assert_equal "tagomori_satoshi", record["name"]
        assert_equal "tagomoris", record["user"]
        assert_equal 34, record["age"]

        assert_nil time, "parser returns nil if configured so"
      }
    end

    def test_parse_with_keep_time_key
      conf = {
        'expression' => %q!/(?<time>.*)/!,
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'keep_time_key' => 'true',
      }
      d = create_driver(conf)
      text = '28/Feb/2013:12:00:00 +0900'
      d.instance.parse(text) do |_time, record|
        assert_equal text, record['time']
      end
    end

    def test_parse_with_keep_time_key_with_typecast
      conf = {
        'expression' => %q!/(?<time>.*)/!,
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'keep_time_key' => 'true',
        'types' => 'time:time:%d/%b/%Y:%H:%M:%S %z',
      }
      d = create_driver(conf)
      text = '28/Feb/2013:12:00:00 +0900'
      d.instance.parse(text) do |_time, record|
        assert_equal 1362020400, record['time']
      end
    end
  end
end
