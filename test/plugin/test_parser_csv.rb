require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser_csv'

class CSVParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf={})
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::CSVParser).configure(conf)
  end

  data('array param' => '["time","c","d"]', 'string param' => 'time,c,d')
  def test_parse(param)
    d = create_driver('keys' => param, 'time_key' => 'time')
    d.instance.parse("2013/02/28 12:00:00,192.168.0.1,111") { |time, record|
      assert_equal(event_time('2013/02/28 12:00:00', format: '%Y/%m/%d %H:%M:%S'), time)
      assert_equal({
                     'c' => '192.168.0.1',
                     'd' => '111',
                   }, record)
    }
  end

  data('array param' => '["c","d"]', 'string param' => 'c,d')
  def test_parse_without_time(param)
    time_at_start = Time.now.to_i

    d = create_driver('keys' => param)
    d.instance.parse("192.168.0.1,111") { |time, record|
      assert time && time >= time_at_start, "parser puts current time without time input"
      assert_equal({
                     'c' => '192.168.0.1',
                     'd' => '111',
                   }, record)
    }

    d = Fluent::Test::Driver::Parser.new(Fluent::Plugin::CSVParser)
    d.configure('keys' => param, 'estimate_current_event' => 'no')
    d.instance.parse("192.168.0.1,111") { |time, record|
      assert_equal({
                     'c' => '192.168.0.1',
                     'd' => '111',
                   }, record)
      assert_nil time, "parser returns nil w/o time and if configured so"
    }
  end

  def test_parse_with_keep_time_key
    d = create_driver(
                     'keys'=>'time',
                     'time_key'=>'time',
                     'time_format'=>"%d/%b/%Y:%H:%M:%S %z",
                     'keep_time_key'=>'true',
                     )
    text = '28/Feb/2013:12:00:00 +0900'
    d.instance.parse(text) do |time, record|
      assert_equal text, record['time']
    end
  end

  data('array param' => '["a","b","c","d","e","f"]', 'string param' => 'a,b,c,d,e,f')
  def test_parse_with_null_value_pattern(param)
    d = create_driver(
                     'keys'=>param,
                     'null_value_pattern'=>'^(-|null|NULL)$'
                     )
    d.instance.parse("-,null,NULL,,--,nuLL") do |time, record|
      assert_nil record['a']
      assert_nil record['b']
      assert_nil record['c']
      assert_nil record['d']
      assert_equal record['e'], '--'
      assert_equal record['f'], 'nuLL'
    end
  end

  data('array param' => '["a","b"]', 'string param' => 'a,b')
  def test_parse_with_null_empty_string(param)
    d = create_driver(
                     'keys'=>param,
                     'null_empty_string'=>true
                     )
    d.instance.parse(", ") do |time, record|
      assert_nil record['a']
      assert_equal record['b'], ' '
    end
  end

  data('array param' => '["a","b","c"]', 'string param' => 'a,b,c')
  def test_parse_with_option_delimiter(param)
    d = create_driver(
                     'keys'=>param,
                     'delimiter'=>' ',
                     )
    d.instance.parse("123 456 789") do |time, record|
      assert_equal record['a'], '123'
      assert_equal record['b'], '456'
      assert_equal record['c'], '789'
    end
  end

  def test_parse_empty_line
    d = create_driver('keys' => '["k1","k2","k3"]')
    d.instance.parse("") do |time, record|
      assert_nil time
      assert_nil record
    end
  end

  sub_test_case 'parser' do
    data('normal' => 'normal',
         'fast' => 'fast')
    def test_compatibility_between_normal_and_fast_parser(param)
      d = create_driver(
        'keys' => 'time,key1,key2,key3,key4,key5',
        'time_key' => 'time',
        'time_format' => "%d/%b/%Y:%H:%M:%S %z",
        'keep_time_key' => 'false',
        'parser_type' => param
      )

      # non quoted
      text = '28/Feb/2013:12:00:00 +0900,value1,value2,value3,value4,value5'
      expected = {'key1' => 'value1', 'key2' => 'value2', 'key3' => "value3",
                  'key4' => 'value4', 'key5' => "value5"}
      d.instance.parse(text) do |time, record|
        assert_equal(event_time("28/Feb/2013:12:00:00 +0900", format: '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal expected, record
      end

      # quoted
      text = '28/Feb/2013:12:00:00 +0900,"value1","val,ue2","va,lu,e3","val ue4",""'
      expected = {'key1' => 'value1', 'key2' => 'val,ue2', 'key3' => "va,lu,e3",
                  'key4' => 'val ue4', 'key5' => ""}
      d.instance.parse(text) do |time, record|
        assert_equal(event_time("28/Feb/2013:12:00:00 +0900", format: '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal expected, record
      end

      # mixed
      text = '28/Feb/2013:12:00:00 +0900,message,"mes,sage","me,ssa,ge",mess age,""'
      expected = {'key1' => 'message', 'key2' => 'mes,sage', 'key3' => "me,ssa,ge",
                  'key4' => 'mess age', 'key5' => ""}
      d.instance.parse(text) do |time, record|
        assert_equal(event_time("28/Feb/2013:12:00:00 +0900", format: '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal expected, record
      end

      # escaped
      text = '28/Feb/2013:12:00:00 +0900,"message","mes""sage","""message""",,""""""'
      expected = {'key1' => 'message', 'key2' => 'mes"sage', 'key3' => '"message"',
                  'key4' => nil, 'key5' => '""'}
      d.instance.parse(text) do |time, record|
        assert_equal(event_time("28/Feb/2013:12:00:00 +0900", format: '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal expected, record
      end

      # escaped quotes next to a delimiter inside the quoted value
      text = '28/Feb/2013:12:00:00 +0900,"mes,sa""ge","mes""sa,ge","me,""ssa"",ge","""mes,sage""",mess age'
      expected = {'key1' => 'mes,sa"ge', 'key2' => 'mes"sa,ge', 'key3' => 'me,"ssa",ge',
                  'key4' => '"mes,sage"', 'key5' => 'mess age'}
      d.instance.parse(text) do |time, record|
        assert_equal(event_time("28/Feb/2013:12:00:00 +0900", format: '%d/%b/%Y:%H:%M:%S %z'), time)
        assert_equal expected, record
      end
    end

    def test_incompatibility_between_normal_and_fast_parser
      normal = create_driver(
        'keys' => 'key1,key2',
        'parser_type' => 'normal'
      )
      fast = create_driver(
        'keys' => 'key1,key2',
        'parser_type' => 'fast'
      )

      # unexpected quote position
      text = 'a"b,"a"""c"'
      assert_raise(CSV::MalformedCSVError) {
        normal.instance.parse(text) { |t, r| }
      }
      assert_nothing_raised {
        # generate broken record
        fast.instance.parse(text) { |t, r| }
      }

      # incorrect the number of column
      text = 'a,b,c'
      expected = {"key1" => 'a', "key2" => 'b'}
      normal.instance.parse(text) { |t, r|
        assert_equal expected, r
      }
      fast.instance.parse(text) { |t, r|
        assert_not_equal expected, r
      }

      # quotes in the middle of a quoted value keep the following columns
      fast = create_driver(
        'keys' => 'method,path,status,size',
        'parser_type' => 'fast'
      )
      [
        ['GET,"/search?q=a"b",200,1234', '/search?q=a"b'],
        ['GET,"a \\"b",200,1234', 'a \\"b'],
        ['GET,"a,b"c"",200,1234', 'a,b"c"'],
      ].each do |line, path|
        fast.instance.parse(line) { |t, r|
          assert_equal({'method' => 'GET', 'path' => path, 'status' => '200', 'size' => '1234'}, r)
        }
      end

      # And more...
    end
  end

  sub_test_case 'fast parser on long lines' do
    def parse_fast(line)
      d = create_driver('keys' => 'method,path,status,size', 'parser_engine' => 'fast')
      Timeout.timeout(5) do
        d.instance.parse(line) { |_time, record| return record }
      end
    end

    def test_many_quotes_without_a_closing_piece
      assert_nothing_raised { parse_fast(%("a"b") * 1 + (',' + %("a"b")) * 3999) }
    end

    def test_many_escaped_quotes_in_one_valid_value
      record = parse_fast('GET,"' + (',' + 'a""') * 16_000 + '",200,1234')
      assert_equal('GET', record['method'])
      assert_equal(',a"' * 16_000, record['path'])
      assert_equal('200', record['status'])
    end

    def test_many_escaped_quotes_in_multibyte_value
      line = ('GET,"' + 'あ""' * 64_000 + ',z",200,1234').encode(Encoding::UTF_8)
      record = parse_fast(line)
      assert_equal({'method' => 'GET', 'path' => 'あ"' * 64_000 + ',z', 'status' => '200', 'size' => '1234'}, record)
    end

    def test_quoted_value_with_delimiter_and_no_escaped_quote
      record = parse_fast('GET,"a,b",200,1234')
      assert_equal({'method' => 'GET', 'path' => 'a,b', 'status' => '200', 'size' => '1234'}, record)
    end
  end

  # "parser_type" config shouldn't hide Fluent::Plugin::Parser#plugin_type
  # https://github.com/fluent/fluentd/issues/3296
  data('normal' => :normal, 'fast' => :fast)
  def test_parser_type_method(engine)
    d = create_driver('keys' => '["time"]','time_key' => 'time', 'parser_type' => engine.to_s)
    assert_equal(:text_per_line, d.instance.parser_type)
  end

  data('normal' => :normal, 'fast' => :fast)
  def test_parser_engine(engine)
    d = create_driver('keys' => '["time"]', 'time_key' => 'time', 'parser_engine' => engine.to_s)
    assert_equal(engine, d.instance.parser_engine)
  end
end
