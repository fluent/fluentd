require_relative '../helper'
require 'fluent/test/driver/filter'
require 'fluent/plugin/filter_parser'

class ParserFilterTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    key_name      message
    format        /^(?<x>.)(?<y>.) (?<time>.+)$/
    time_format   %Y%m%d%H%M%S
    reserve_data  true
  ]

  def create_driver(conf=CONFIG,tag='test')
    Fluent::Test::FilterTestDriver.new(Fluent::Plugin::ParserFilter, tag).configure(conf)
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
    assert_equal false, d.instance.reserve_data
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
    d1 = create_driver(CONFIG, 'test.no.change')
    time = Time.parse("2012-01-02 13:14:15").to_i
    d1.run do
      d1.filter({'message' => '12 20120402182059'}, time)
      d1.filter({'message' => '34 20120402182100'}, time)
      d1.filter({'message' => '56 20120402182100'}, time)
      d1.filter({'message' => '78 20120402182101'}, time)
      d1.filter({'message' => '90 20120402182100'}, time)
    end
    filtered = d1.filtered_as_array
    assert_equal 5, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal Time.parse("2012-04-02 18:20:59").to_i, first[1]
    assert_equal '1', first[2]['x']
    assert_equal '2', first[2]['y']
    assert_equal '12 20120402182059', first[2]['message']

    second = filtered[1]
    assert_equal 'test.no.change', second[0]
    assert_equal Time.parse("2012-04-02 18:21:00").to_i, second[1]
    assert_equal '3', second[2]['x']
    assert_equal '4', second[2]['y']

    third = filtered[2]
    assert_equal 'test.no.change', third[0]
    assert_equal Time.parse("2012-04-02 18:21:00").to_i, third[1]
    assert_equal '5', third[2]['x']
    assert_equal '6', third[2]['y']

    fourth = filtered[3]
    assert_equal 'test.no.change', fourth[0]
    assert_equal Time.parse("2012-04-02 18:21:01").to_i, fourth[1]
    assert_equal '7', fourth[2]['x']
    assert_equal '8', fourth[2]['y']

    fifth = filtered[4]
    assert_equal 'test.no.change', fifth[0]
    assert_equal Time.parse("2012-04-02 18:21:00").to_i, fifth[1]
    assert_equal '9', fifth[2]['x']
    assert_equal '0', fifth[2]['y']

    d2 = create_driver(%[
      tag parsed
      key_name      data
      format        /^(?<x>.)(?<y>.) (?<t>.+)$/
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d2.run do
      d2.filter({'data' => '12 20120402182059'}, time)
      d2.filter({'data' => '34 20120402182100'}, time)
    end
    filtered = d2.filtered_as_array
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_nil first[2]['data']
    assert_equal '1', first[2]['x']
    assert_equal '2', first[2]['y']
    assert_equal '20120402182059', first[2]['t']

    second = filtered[1]
    assert_equal 'test.no.change', second[0]
    assert_equal time, second[1]
    assert_nil second[2]['data']
    assert_equal '3', second[2]['x']
    assert_equal '4', second[2]['y']
    assert_equal '20120402182100', second[2]['t']

    d3 = create_driver(%[
      tag parsed
      key_name      data
      format        /^(?<x>[0-9])(?<y>[0-9]) (?<t>.+)$/
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d3.run do
      d3.filter({'data' => '12 20120402182059'}, time)
      d3.filter({'data' => '34 20120402182100'}, time)
      d3.filter({'data' => 'xy 20120402182101'}, time)
    end
    filtered = d3.filtered_as_array
    assert_equal 2, filtered.length

    d3x = create_driver(%[
      tag parsed
      key_name      data
      format        /^(?<x>\\d)(?<y>\\d) (?<t>.+)$/
      reserve_data  yes
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d3x.run do
      d3x.filter({'data' => '12 20120402182059'}, time)
      d3x.filter({'data' => '34 20120402182100'}, time)
      d3x.filter({'data' => 'xy 20120402182101'}, time)
    end
    filtered = d3x.filtered_as_array
    assert_equal 3, filtered.length

    d4 = create_driver(%[
      tag parsed
      key_name      data
      format        json
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d4.run do
      d4.filter({'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'}, time)
      d4.filter({'data' => 'foobar', 'xxx' => 'x', 'yyy' => 'y'}, time)
    end
    filtered = d4.filtered_as_array
    assert_equal 1, filtered.length

    d4x = create_driver(%[
      tag parsed
      key_name      data
      format        json
      reserve_data  yes
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d4x.run do
      d4x.filter({'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'}, time)
      d4x.filter({'data' => 'foobar', 'xxx' => 'x', 'yyy' => 'y'}, time)
    end
    filtered = d4x.filtered_as_array
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_equal '{"xxx":"first","yyy":"second"}', first[2]['data']
    assert_equal 'first', first[2]['xxx']
    assert_equal 'second', first[2]['yyy']

    second = filtered[1]
    assert_equal 'test.no.change', second[0]
    assert_equal time, second[1]
    assert_equal 'foobar', second[2]['data']
    assert_equal 'x', second[2]['xxx']
    assert_equal 'y', second[2]['yyy']
  end

  CONFIG_LTSV =  %[
    format ltsv
    key_name data
  ]
  def test_filter_ltsv
    d = create_driver(CONFIG_LTSV, 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter({'data' => "xxx:first\tyyy:second", 'xxx' => 'x', 'yyy' => 'y'}, time)
      d.filter({'data' => "xxx:first\tyyy:second2", 'xxx' => 'x', 'yyy' => 'y'}, time)
    end
    filtered = d.filtered_as_array
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_nil first[2]['data']
    assert_equal 'first', first[2]['xxx']
    assert_equal 'second', first[2]['yyy']

    second = filtered[1]
    assert_equal 'test.no.change', second[0]
    assert_equal time, second[1]
    assert_nil first[2]['data']
    assert_equal 'first', second[2]['xxx']
    assert_equal 'second2', second[2]['yyy']

    d = create_driver(CONFIG_LTSV + %[
      reserve_data yes
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter({'data' => "xxx:first\tyyy:second", 'xxx' => 'x', 'yyy' => 'y'}, time)
      d.filter({'data' => "xxx:first\tyyy:second2", 'xxx' => 'x', 'yyy' => 'y'}, time)
    end
    filtered = d.filtered_as_array
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_equal "xxx:first\tyyy:second", first[2]['data']
    assert_equal 'first', first[2]['xxx']
    assert_equal 'second', first[2]['yyy']

    second = filtered[1]
    assert_equal 'test.no.change', second[0]
    assert_equal time, second[1]
    assert_equal "xxx:first\tyyy:second", first[2]['data']
    assert_equal 'first', second[2]['xxx']
    assert_equal 'second2', second[2]['yyy']

    # convert types
    d = create_driver(CONFIG_LTSV + %[
      types i:integer,s:string,f:float,b:bool
    ], 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter({'data' => "i:1\ts:2\tf:3\tb:true\tx:123"}, time)
    end
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_equal 1, first[2]['i']
    assert_equal '2', first[2]['s']
    assert_equal 3.0, first[2]['f']
    assert_equal true, first[2]['b']
    assert_equal '123', first[2]['x']
  end

  CONFIG_TSV =  %[
    format tsv
    key_name data
    keys key1,key2,key3
  ]
  def test_filter_tsv
    d = create_driver(CONFIG_TSV, 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter({'data' => "value1\tvalue2\tvalueThree", 'xxx' => 'x', 'yyy' => 'y'}, time)
    end
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_nil first[2]['data']
    assert_equal 'value1', first[2]['key1']
    assert_equal 'value2', first[2]['key2']
    assert_equal 'valueThree', first[2]['key3']
  end

  CONFIG_CSV =  %[
    format csv
    key_name data
    keys key1,key2,key3
  ]
  def test_filter_csv
    d = create_driver(CONFIG_CSV, 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter({'data' => 'value1,"value2","value""ThreeYes!"', 'xxx' => 'x', 'yyy' => 'y'}, time)
    end
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]
    assert_nil first[2]['data']
    assert_equal 'value1', first[2]['key1']
    assert_equal 'value2', first[2]['key2']
    assert_equal 'value"ThreeYes!', first[2]['key3']
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

    d = create_driver(CONFIG_HASH_VALUE_FIELD, 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter(original, time)
    end
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]

    record = first[2]
    assert_equal 1, record.keys.size
    assert_equal({"xxx"=>"first","yyy"=>"second"}, record['parsed'])

    d = create_driver(CONFIG_HASH_VALUE_FIELD_RESERVE_DATA, 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter(original, time)
    end
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]

    record = first[2]
    assert_equal 4, record.keys.size
    assert_equal original['data'], record['data']
    assert_equal original['xxx'], record['xxx']
    assert_equal original['yyy'], record['yyy']
    assert_equal({"xxx"=>"first","yyy"=>"second"}, record['parsed'])

    d = create_driver(CONFIG_HASH_VALUE_FIELD_WITH_INJECT_KEY_PREFIX, 'test.no.change')
    time = Time.parse("2012-04-02 18:20:59").to_i
    d.run do
      d.filter(original, time)
    end
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal 'test.no.change', first[0]
    assert_equal time, first[1]

    record = first[2]
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
    d = create_driver(CONFIG_DONT_PARSE_TIME, 'test.no.change')

    d.run do
      d.filter({'data' => '{"time":1383190430, "f1":"v1"}'}, t)
      d.filter({'data' => '{"time":"1383190430", "f1":"v1"}'}, t)
      d.filter({'data' => '{"time":"2013-10-31 12:34:03 +0900", "f1":"v1"}'}, t)
    end
    filtered = d.filtered_as_array
    assert_equal 3, filtered.length

    assert_equal 'test.no.change', filtered[0][0]
    assert_equal 'v1', filtered[0][2]['f1']
    assert_equal 1383190430, filtered[0][2]['time']
    assert_equal t, filtered[0][1]

    assert_equal 'test.no.change', filtered[1][0]
    assert_equal 'v1', filtered[1][2]['f1']
    assert_equal "1383190430", filtered[1][2]['time']
    assert_equal t, filtered[1][1]

    assert_equal 'test.no.change', filtered[2][0]
    assert_equal 'v1', filtered[2][2]['f1']
    assert_equal '2013-10-31 12:34:03 +0900', filtered[2][2]['time']
    assert_equal t, filtered[2][1]
  end

  CONFIG_INVALID_TIME_VALUE = %[
    remove_prefix test
    key_name data
    format json
  ] # 'time' is implicit @time_key
  def test_filter_invalid_time_data
    # should not raise errors
    t = Time.now.to_i
    d = create_driver(CONFIG_INVALID_TIME_VALUE, 'test.no.change')
    assert_nothing_raised {
      d.run do
        d.filter({'data' => '{"time":[], "f1":"v1"}'}, t)
        d.filter({'data' => '{"time":"thisisnottime", "f1":"v1"}'}, t)
      end
    }
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length

    assert_equal 'test.no.change', filtered[0][0]
    assert_equal 0, filtered[0][1]
    assert_equal 'v1', filtered[0][2]['f1']
    assert_equal 0, filtered[0][2]['time'].to_i
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

    d = create_driver(CONFIG_NOT_REPLACE, 'test.no.change')
    assert_raise(ArgumentError) {
      d.run do
        d.filter({'data' => invalid_utf8}, Time.now.to_i)
      end
    }

    d = create_driver(CONFIG_INVALID_BYTE, 'test.in')
    assert_nothing_raised {
      d.run do
        d.emit({'data' => invalid_utf8}, Time.now.to_i)
      end
    }
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length
    assert_nil filtered[0][2]['data']
    assert_equal '?'.force_encoding('UTF-8'), filtered[0][2]['message']

    d = create_driver(CONFIG_INVALID_BYTE + %[
      reserve_data yes
    ], 'test.no.change')
    assert_nothing_raised {
      d.run do
        d.filter({'data' => invalid_utf8}, Time.now.to_i)
      end
    }
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length
    assert_equal invalid_utf8, filtered[0][2]['data']
    assert_equal '?'.force_encoding('UTF-8'), filtered[0][2]['message']

    invalid_ascii = "\xff".force_encoding('US-ASCII')
    d = create_driver(CONFIG_INVALID_BYTE, 'test.no.change')
    assert_nothing_raised {
      d.run do
        d.filter({'data' => invalid_ascii}, Time.now.to_i)
      end
    }
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length
    assert_nil filtered[0][2]['data']
    assert_equal '?'.force_encoding('US-ASCII'), filtered[0][2]['message']
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
    d = create_driver(CONFIG_NOT_IGNORE, 'test.no.ignore')
    assert_nothing_raised {
      d.run do
        d.filter({'foo' => 'bar'}, Time.now.to_i)
      end
    }
    assert_match(/data does not exist/, d.instance.log.out.logs.first)

    d = create_driver(CONFIG_IGNORE, 'test.ignore')
    assert_nothing_raised {
      d.run do
        d.filter({'foo' => 'bar'}, Time.now.to_i)
      end
    }
    assert_not_match(/data does not exist/, d.instance.log.out.logs.first)

    d = create_driver(CONFIG_PASS_SAME_RECORD, 'test.pass_same_record')
    assert_nothing_raised {
      d.run do
        d.filter({'foo' => 'bar'}, Time.now.to_i)
      end
    }
    filtered = d.filtered_as_array
    assert_equal 1, filtered.length
    assert_nil filtered[0][2]['data']
    assert_equal 'bar', filtered[0][2]['foo']
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
    d = create_driver(CONFIG_INVALID_TIME_VALUE, 'test.no.change')
    swap_logger(d.instance) do
      assert_raise(DummyLoggerWarnedException) {
        d.run do
          d.filter({'data' => '{"time":[], "f1":"v1"}'}, Time.now.to_i)
        end
      }
    end
  end

  class DefaultSuppressParseErrorLogTest < self
    def setup
      # default(disabled) 'suppress_parse_error_log' is not specify
      @d = create_driver(CONFIG_DEFAULT_SUPPRESS_PARSE_ERROR_LOG, 'test.no.change')
    end

    def test_raise_exception
      swap_logger(@d.instance) do
        assert_raise(DummyLoggerWarnedException) {
          @d.run do
            @d.filter({'message' => INVALID_MESSAGE}, Time.now.to_i)
          end
        }
      end
    end

    def test_nothing_raised
      swap_logger(@d.instance) do
      assert_nothing_raised {
        @d.run do
          @d.filter({'message' => VALID_MESSAGE}, Time.now.to_i)
        end
      }
      end
    end
  end

  class DisabledSuppressParseErrorLogTest < self
    def setup
      # disabled 'suppress_parse_error_log'
      @d = create_driver(CONFIG_DISABELED_SUPPRESS_PARSE_ERROR_LOG, 'test.no.change')
    end

    def test_raise_exception
      swap_logger(@d.instance) do
        assert_raise(DummyLoggerWarnedException) {
          @d.run do
            @d.filter({'message' => INVALID_MESSAGE}, Time.now.to_i)
          end
        }
      end
    end

    def test_nothing_raised
      swap_logger(@d.instance) do
      assert_nothing_raised {
        @d.run do
          @d.filter({'message' => VALID_MESSAGE}, Time.now.to_i)
        end
      }
      end
    end
  end

  class EnabledSuppressParseErrorLogTest < self
    def setup
      # enabled 'suppress_parse_error_log'
      @d = create_driver(CONFIG_ENABELED_SUPPRESS_PARSE_ERROR_LOG, 'test.no.change')
    end

    def test_nothing_raised
      swap_logger(@d.instance) do
      assert_nothing_raised {
        @d.run do
          @d.filter({'message' => INVALID_MESSAGE}, Time.now.to_i)
          @d.filter({'message' => VALID_MESSAGE},   Time.now.to_i)
        end
      }
      end
    end
  end
end
