require_relative '../helper'
require 'timecop'
require 'fluent/test/driver/filter'
require 'fluent/plugin/filter_parser'

class ParserFilterTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @tag = 'test'
    @default_time = Time.parse('2010-05-04 03:02:01 UTC')
    Timecop.freeze(@default_time)
  end

  def teardown
    Timecop.return
  end

  CONFIG = %[
    key_name     message
    format       /^(?<x>.)(?<y>.) (?<time>.+)$/
    time_format  %Y%m%d%H%M%S
    reserve_data true
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::ParserFilter).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      create_driver('')
    }
    assert_raise(Fluent::ConfigError) {
      create_driver %[
        format unknown_format_that_will_never_be_implemented
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format /(?<x>.)/
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format /(?<x>.)/
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format /(?<x>.)/
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format /(?<x>.)/
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format json
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format ltsv
        key_name foo
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format /^col1=(?<col1>.+) col2=(?<col2>.+)$/
        key_name message
        suppress_parse_error_log true
      ]
    }
    assert_nothing_raised {
      create_driver %[
        format /^col1=(?<col1>.+) col2=(?<col2>.+)$/
        key_name message
        suppress_parse_error_log false
      ]
    }
    d = create_driver %[
      key_name foo
      format /(?<x>.)/
    ]
    assert_false d.instance.reserve_data
  end

  # CONFIG = %[
  #   remove_prefix test
  #   add_prefix    parsed
  #   key_name      message
  #   format        /^(?<x>.)(?<y>.) (?<time>.+)$/
  #   time_format   %Y%m%d%H%M%S
  #   reserve_data  true
  # ]
  def test_filter
    d1 = create_driver(CONFIG)
    time = Time.parse("2012-01-02 13:14:15").to_i
    d1.run do
      d1.feed(@tag, time, {'message' => '12 20120402182059'})
      d1.feed(@tag, time, {'message' => '34 20120402182100'})
      d1.feed(@tag, time, {'message' => '56 20120402182100'})
      d1.feed(@tag, time, {'message' => '78 20120402182101'})
      d1.feed(@tag, time, {'message' => '90 20120402182100'})
    end
    filtered = d1.filtered
    assert_equal 5, filtered.length

    first = filtered[0]
    assert_equal Time.parse("2012-04-02 18:20:59").to_i, first[0]
    assert_equal '1', first[1]['x']
    assert_equal '2', first[1]['y']
    assert_equal '12 20120402182059', first[1]['message']

    second = filtered[1]
    assert_equal Time.parse("2012-04-02 18:21:00").to_i, second[0]
    assert_equal '3', second[1]['x']
    assert_equal '4', second[1]['y']

    third = filtered[2]
    assert_equal Time.parse("2012-04-02 18:21:00").to_i, third[0]
    assert_equal '5', third[1]['x']
    assert_equal '6', third[1]['y']

    fourth = filtered[3]
    assert_equal Time.parse("2012-04-02 18:21:01").to_i, fourth[0]
    assert_equal '7', fourth[1]['x']
    assert_equal '8', fourth[1]['y']

    fifth = filtered[4]
    assert_equal Time.parse("2012-04-02 18:21:00").to_i, fifth[0]
    assert_equal '9', fifth[1]['x']
    assert_equal '0', fifth[1]['y']

    d2 = create_driver(%[
      key_name data
      format   /^(?<x>.)(?<y>.) (?<t>.+)$/
    ])
    time = Fluent::EventTime.from_time(@default_time) # EventTime emit test
    d2.run do
      d2.feed(@tag, time, {'data' => '12 20120402182059'})
      d2.feed(@tag, time, {'data' => '34 20120402182100'})
    end
    filtered = d2.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_nil first[1]['data']
    assert_equal '1', first[1]['x']
    assert_equal '2', first[1]['y']
    assert_equal '20120402182059', first[1]['t']

    second = filtered[1]
    assert_equal time, second[0]
    assert_nil second[1]['data']
    assert_equal '3', second[1]['x']
    assert_equal '4', second[1]['y']
    assert_equal '20120402182100', second[1]['t']

    d3 = create_driver(%[
      tag parsed
      key_name      data
      format        /^(?<x>[0-9])(?<y>[0-9]) (?<t>.+)$/
    ])
    time = Time.parse("2012-04-02 18:20:59").to_i
    d3.run do
      d3.feed(@tag, time, {'data' => '12 20120402182059'})
      d3.feed(@tag, time, {'data' => '34 20120402182100'})
      d3.feed(@tag, time, {'data' => 'xy 20120402182101'})
    end
    filtered = d3.filtered
    assert_equal 2, filtered.length

    d3x = create_driver(%[
      tag parsed
      key_name      data
      format        /^(?<x>\\d)(?<y>\\d) (?<t>.+)$/
      reserve_data  yes
    ])
    time = Time.parse("2012-04-02 18:20:59").to_i
    d3x.run do
      d3x.feed(@tag, time, {'data' => '12 20120402182059'})
      d3x.feed(@tag, time, {'data' => '34 20120402182100'})
      d3x.feed(@tag, time, {'data' => 'xy 20120402182101'})
    end
    filtered = d3x.filtered
    assert_equal 3, filtered.length

    d4 = create_driver(%[
      tag parsed
      key_name      data
      format        json
    ])
    time = Time.parse("2012-04-02 18:20:59").to_i
    d4.run do
      d4.feed(@tag, time, {'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'})
      d4.feed(@tag, time, {'data' => 'foobar', 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d4.filtered
    assert_equal 1, filtered.length

    d4x = create_driver(%[
      tag parsed
      key_name      data
      format        json
      reserve_data  yes
    ])
    time = @default_time.to_i
    d4x.run do
      d4x.feed(@tag, time, {'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'})
      d4x.feed(@tag, time, {'data' => 'foobar', 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d4x.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_equal '{"xxx":"first","yyy":"second"}', first[1]['data']
    assert_equal 'first', first[1]['xxx']
    assert_equal 'second', first[1]['yyy']

    second = filtered[1]
    assert_equal time, second[0]
    assert_equal 'foobar', second[1]['data']
    assert_equal 'x', second[1]['xxx']
    assert_equal 'y', second[1]['yyy']
  end

  CONFIG_LTSV =  %[
    format ltsv
    key_name data
  ]
  def test_filter_ltsv
    d = create_driver(CONFIG_LTSV)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => "xxx:first\tyyy:second", 'xxx' => 'x', 'yyy' => 'y'})
      d.feed(@tag, time, {'data' => "xxx:first\tyyy:second2", 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_nil first[1]['data']
    assert_equal 'first', first[1]['xxx']
    assert_equal 'second', first[1]['yyy']

    second = filtered[1]
    assert_equal time, second[0]
    assert_nil first[1]['data']
    assert_equal 'first', second[1]['xxx']
    assert_equal 'second2', second[1]['yyy']

    d = create_driver(CONFIG_LTSV + %[reserve_data yes])
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => "xxx:first\tyyy:second", 'xxx' => 'x', 'yyy' => 'y'})
      d.feed(@tag, time, {'data' => "xxx:first\tyyy:second2", 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_equal "xxx:first\tyyy:second", first[1]['data']
    assert_equal 'first', first[1]['xxx']
    assert_equal 'second', first[1]['yyy']

    second = filtered[1]
    assert_equal time, second[0]
    assert_equal "xxx:first\tyyy:second", first[1]['data']
    assert_equal 'first', second[1]['xxx']
    assert_equal 'second2', second[1]['yyy']

    # convert types
    d = create_driver(CONFIG_LTSV + %[types i:integer,s:string,f:float,b:bool])
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => "i:1\ts:2\tf:3\tb:true\tx:123"})
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_equal 1, first[1]['i']
    assert_equal '2', first[1]['s']
    assert_equal 3.0, first[1]['f']
    assert_equal true, first[1]['b']
    assert_equal '123', first[1]['x']
  end

  CONFIG_TSV =  %[
    format tsv
    key_name data
    keys key1,key2,key3
  ]
  def test_filter_tsv
    d = create_driver(CONFIG_TSV)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => "value1\tvalue2\tvalueThree", 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_nil first[1]['data']
    assert_equal 'value1', first[1]['key1']
    assert_equal 'value2', first[1]['key2']
    assert_equal 'valueThree', first[1]['key3']
  end

  CONFIG_CSV =  %[
    format csv
    key_name data
    keys key1,key2,key3
  ]
  def test_filter_csv
    d = create_driver(CONFIG_CSV)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => 'value1,"value2","value""ThreeYes!"', 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal time, first[0]
    assert_nil first[1]['data']
    assert_equal 'value1', first[1]['key1']
    assert_equal 'value2', first[1]['key2']
    assert_equal 'value"ThreeYes!', first[1]['key3']
  end

  CONFIG_HASH_VALUE_FIELD = %[
    format       json
    key_name     data
    hash_value_field parsed
  ]
  CONFIG_HASH_VALUE_FIELD_RESERVE_DATA = %[
    format       json
    key_name     data
    reserve_data yes
    hash_value_field parsed
  ]
  CONFIG_HASH_VALUE_FIELD_WITH_INJECT_KEY_PREFIX = %[
    format       json
    key_name     data
    hash_value_field parsed
    inject_key_prefix data.
  ]
  def test_filter_inject_hash_value_field
    original = {'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'}

    d = create_driver(CONFIG_HASH_VALUE_FIELD)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, original)
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal time, first[0]

    record = first[1]
    assert_equal 1, record.keys.size
    assert_equal({"xxx"=>"first","yyy"=>"second"}, record['parsed'])

    d = create_driver(CONFIG_HASH_VALUE_FIELD_RESERVE_DATA)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, original)
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal time, first[0]

    record = first[1]
    assert_equal 4, record.keys.size
    assert_equal original['data'], record['data']
    assert_equal original['xxx'], record['xxx']
    assert_equal original['yyy'], record['yyy']
    assert_equal({"xxx"=>"first","yyy"=>"second"}, record['parsed'])

    d = create_driver(CONFIG_HASH_VALUE_FIELD_WITH_INJECT_KEY_PREFIX)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, original)
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal time, first[0]

    record = first[1]
    assert_equal 1, record.keys.size
    assert_equal({"data.xxx"=>"first","data.yyy"=>"second"}, record['parsed'])
  end

  CONFIG_DONT_PARSE_TIME = %[
    key_name data
    format json
    time_parse no
  ]
  def test_time_should_be_reserved
    t = Time.now.to_i
    d = create_driver(CONFIG_DONT_PARSE_TIME)
    d.run do
      d.feed(@tag, t, {'data' => '{"time":1383190430, "f1":"v1"}'})
      d.feed(@tag, t, {'data' => '{"time":"1383190430", "f1":"v1"}'})
      d.feed(@tag, t, {'data' => '{"time":"2013-10-31 12:34:03 +0900", "f1":"v1"}'})
    end
    filtered = d.filtered
    assert_equal 3, filtered.length

    assert_equal 'v1', filtered[0][1]['f1']
    assert_equal 1383190430, filtered[0][1]['time']
    assert_equal t, filtered[0][0]

    assert_equal 'v1', filtered[1][1]['f1']
    assert_equal "1383190430", filtered[1][1]['time']
    assert_equal t, filtered[1][0]

    assert_equal 'v1', filtered[2][1]['f1']
    assert_equal '2013-10-31 12:34:03 +0900', filtered[2][1]['time']
    assert_equal t, filtered[2][0]
  end

  CONFIG_INVALID_TIME_VALUE = %[
    remove_prefix test
    key_name data
    format json
  ] # 'time' is implicit @time_key
  def test_filter_invalid_time_data
    # should not raise errors
    time = Time.now.to_i
    d = create_driver(CONFIG_INVALID_TIME_VALUE)
    assert_nothing_raised {
      d.run do
        d.feed(@tag, time, {'data' => '{"time":[], "f1":"v1"}'})
        d.feed(@tag, time, {'data' => '{"time":"thisisnottime", "f1":"v1"}'})
      end
    }
    filtered = d.filtered
    assert_equal 1, filtered.length

    assert_equal 0, filtered[0][0]
    assert_equal 'v1', filtered[0][1]['f1']
    assert_equal 0, filtered[0][1]['time'].to_i
  end

  # REGEXP = /^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/

  CONFIG_NOT_REPLACE = %[
    remove_prefix test
    key_name      data
    format        /^(?<message>.*)$/
  ]
  CONFIG_INVALID_BYTE = CONFIG_NOT_REPLACE + %[
    replace_invalid_sequence true
  ]
  def test_filter_invalid_byte
    invalid_utf8 = "\xff".force_encoding('UTF-8')

    d = create_driver(CONFIG_NOT_REPLACE)
    assert_raise(ArgumentError) {
      d.run do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => invalid_utf8})
      end
    }

    d = create_driver(CONFIG_INVALID_BYTE)
    assert_nothing_raised {
      d.run do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => invalid_utf8})
      end
    }
    filtered = d.filtered
    assert_equal 1, filtered.length
    assert_nil filtered[0][1]['data']
    assert_equal '?'.force_encoding('UTF-8'), filtered[0][1]['message']

    d = create_driver(CONFIG_INVALID_BYTE + %[reserve_data yes])
    assert_nothing_raised {
      d.run do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => invalid_utf8})
      end
    }
    filtered = d.filtered
    assert_equal 1, filtered.length
    assert_equal invalid_utf8, filtered[0][1]['data']
    assert_equal '?'.force_encoding('UTF-8'), filtered[0][1]['message']

    invalid_ascii = "\xff".force_encoding('US-ASCII')
    d = create_driver(CONFIG_INVALID_BYTE)
    assert_nothing_raised {
      d.run do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => invalid_ascii})
      end
    }
    filtered = d.filtered
    assert_equal 1, filtered.length
    assert_nil filtered[0][1]['data']
    assert_equal '?'.force_encoding('US-ASCII'), filtered[0][1]['message']
  end

  CONFIG_NOT_IGNORE = %[
    remove_prefix    test
    key_name         data
    format           json
    hash_value_field parsed
  ]
  CONFIG_IGNORE = CONFIG_NOT_IGNORE + %[
    ignore_key_not_exist true
  ]
  CONFIG_PASS_SAME_RECORD = CONFIG_IGNORE + %[
    reserve_data true
  ]
  def test_filter_key_not_exist
    d = create_driver(CONFIG_NOT_IGNORE)
    assert_nothing_raised {
      d.run(shutdown: false) do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'foo' => 'bar'})
      end
    }
    assert_match(/data does not exist/, d.instance.log.logs.first)
    d.instance_shutdown

    d = create_driver(CONFIG_IGNORE)
    assert_nothing_raised {
      d.run(shutdown: false) do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'foo' => 'bar'})
      end
    }
    assert_not_match(/data does not exist/, d.instance.log.logs.first)
    d.instance_shutdown

    d = create_driver(CONFIG_PASS_SAME_RECORD)
    assert_nothing_raised {
      d.run do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'foo' => 'bar'})
      end
    }
    filtered = d.filtered
    assert_equal 1, filtered.length
    assert_nil filtered[0][1]['data']
    assert_equal 'bar', filtered[0][1]['foo']
  end

  # suppress_parse_error_log test
  CONFIG_DISABELED_SUPPRESS_PARSE_ERROR_LOG = %[
    tag hogelog
    format /^col1=(?<col1>.+) col2=(?<col2>.+)$/
    key_name message
    suppress_parse_error_log false
  ]
  CONFIG_ENABELED_SUPPRESS_PARSE_ERROR_LOG = %[
    tag hogelog
    format /^col1=(?<col1>.+) col2=(?<col2>.+)$/
    key_name message
    suppress_parse_error_log true
  ]
  CONFIG_DEFAULT_SUPPRESS_PARSE_ERROR_LOG = %[
    tag hogelog
    format /^col1=(?<col1>.+) col2=(?<col2>.+)$/
    key_name message
  ]

  INVALID_MESSAGE = 'foo bar'
  VALID_MESSAGE   = 'col1=foo col2=bar'

  # if call warn() raise exception
  class DummyLoggerWarnedException < StandardError; end
  class DummyLogger
    def reset
    end
    def warn(message)
      raise DummyLoggerWarnedException
    end
  end

  def swap_logger(instance)
    raise "use with block" unless block_given?
    dummy = DummyLogger.new
    saved_logger = instance.log
    instance.log = dummy
    restore = lambda { instance.log = saved_logger }

    yield

    restore.call
  end

  def test_parser_error_warning
    d = create_driver(CONFIG_INVALID_TIME_VALUE)
    swap_logger(d.instance) do
      assert_raise(DummyLoggerWarnedException) {
        d.run do
          d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => '{"time":[], "f1":"v1"}'})
        end
      }
    end
  end

  class DefaultSuppressParseErrorLogTest < self
    setup do
      # default(disabled) 'suppress_parse_error_log' is not specify
      @d = create_driver(CONFIG_DEFAULT_SUPPRESS_PARSE_ERROR_LOG)
    end

    def test_raise_exception
      swap_logger(@d.instance) do
        assert_raise(DummyLoggerWarnedException) {
          @d.run do
            @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => INVALID_MESSAGE})
          end
        }
      end
    end

    def test_nothing_raised
      swap_logger(@d.instance) do
      assert_nothing_raised {
        @d.run do
          @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => VALID_MESSAGE})
        end
      }
      end
    end
  end

  class DisabledSuppressParseErrorLogTest < self
    setup do
      # disabled 'suppress_parse_error_log'
      @d = create_driver(CONFIG_DISABELED_SUPPRESS_PARSE_ERROR_LOG)
    end

    def test_raise_exception
      swap_logger(@d.instance) do
        assert_raise(DummyLoggerWarnedException) {
          @d.run do
            @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => INVALID_MESSAGE})
          end
        }
      end
    end

    def test_nothing_raised
      swap_logger(@d.instance) do
      assert_nothing_raised {
        @d.run do
          @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => VALID_MESSAGE})
        end
      }
      end
    end
  end

  class EnabledSuppressParseErrorLogTest < self
    setup do
      # enabled 'suppress_parse_error_log'
      @d = create_driver(CONFIG_ENABELED_SUPPRESS_PARSE_ERROR_LOG)
    end

    def test_nothing_raised
      swap_logger(@d.instance) do
      assert_nothing_raised {
        @d.run do
          @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => INVALID_MESSAGE})
          @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => VALID_MESSAGE})
        end
      }
      end
    end
  end
end
