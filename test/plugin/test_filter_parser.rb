require_relative '../helper'
require 'timecop'
require 'fluent/test/driver/filter'
require 'fluent/plugin/filter_parser'
require 'flexmock/test_unit'

class ParserFilterTest < Test::Unit::TestCase
  include FlexMock::TestCase

  def setup
    Fluent::Test.setup
    @tag = 'test'
    @default_time = Time.parse('2010-05-04 03:02:01 UTC')
    Timecop.freeze(@default_time)
  end

  def teardown
    super
    Timecop.return
  end

  def assert_equal_parsed_time(expected, actual)
    if expected.is_a?(Integer)
      assert_equal(expected, actual.to_i)
    else
      assert_equal_event_time(expected, actual)
    end
  end

  ParserError = Fluent::Plugin::Parser::ParserError
  CONFIG = %[
    key_name     message
    reserve_data true
    <parse>
      @type regexp
      expression /^(?<x>.)(?<y>.) (?<time>.+)$/
      time_format %Y%m%d%H%M%S
    </parse>
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
        key_name foo
        <parse>
          @type unknown_format_that_will_never_be_implemented
        </parse>
      ]
    }
    assert_nothing_raised {
      create_driver %[
        key_name foo
        <parse>
          @type regexp
          expression /(?<x>.)/
        </parse>
      ]
    }
    assert_nothing_raised {
      create_driver %[
        key_name foo
        <parse>
          @type json
        </parse>
      ]
    }
    assert_nothing_raised {
      create_driver %[
        key_name foo
        format json
      ]
    }
    assert_nothing_raised {
      create_driver %[
        key_name foo
        <parse>
          @type ltsv
        </parse>
      ]
    }
    assert_nothing_raised {
      create_driver %[
        key_name message
        <parse>
          @type regexp
          expression /^col1=(?<col1>.+) col2=(?<col2>.+)$/
        </parse>
      ]
    }
    d = create_driver %[
      key_name foo
      <parse>
        @type regexp
        expression /(?<x>.)/
      </parse>
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
    time = event_time("2012-01-02 13:14:15")
    d1.run(default_tag: @tag) do
      d1.feed(time, {'message' => '12 20120402182059'})
      d1.feed(time, {'message' => '34 20120402182100'})
      d1.feed(time, {'message' => '56 20120402182100'})
      d1.feed(time, {'message' => '78 20120402182101'})
      d1.feed(time, {'message' => '90 20120402182100'})
    end
    filtered = d1.filtered
    assert_equal 5, filtered.length

    first = filtered[0]
    assert_equal_event_time event_time("2012-04-02 18:20:59"), first[0]
    assert_equal '1', first[1]['x']
    assert_equal '2', first[1]['y']
    assert_equal '12 20120402182059', first[1]['message']

    second = filtered[1]
    assert_equal_event_time event_time("2012-04-02 18:21:00"), second[0]
    assert_equal '3', second[1]['x']
    assert_equal '4', second[1]['y']

    third = filtered[2]
    assert_equal_event_time event_time("2012-04-02 18:21:00"), third[0]
    assert_equal '5', third[1]['x']
    assert_equal '6', third[1]['y']

    fourth = filtered[3]
    assert_equal_event_time event_time("2012-04-02 18:21:01"), fourth[0]
    assert_equal '7', fourth[1]['x']
    assert_equal '8', fourth[1]['y']

    fifth = filtered[4]
    assert_equal_event_time event_time("2012-04-02 18:21:00"), fifth[0]
    assert_equal '9', fifth[1]['x']
    assert_equal '0', fifth[1]['y']

    d2 = create_driver(%[
      key_name data
      format   /^(?<x>.)(?<y>.) (?<t>.+)$/
    ])
    time = Fluent::EventTime.from_time(@default_time) # EventTime emit test
    d2.run(default_tag: @tag) do
      d2.feed(time, {'data' => '12 20120402182059'})
      d2.feed(time, {'data' => '34 20120402182100'})
    end
    filtered = d2.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal_event_time time, first[0]
    assert_nil first[1]['data']
    assert_equal '1', first[1]['x']
    assert_equal '2', first[1]['y']
    assert_equal '20120402182059', first[1]['t']

    second = filtered[1]
    assert_equal_event_time time, second[0]
    assert_nil second[1]['data']
    assert_equal '3', second[1]['x']
    assert_equal '4', second[1]['y']
    assert_equal '20120402182100', second[1]['t']

    d3 = create_driver(%[
      key_name data
      <parse>
        @type regexp
        expression /^(?<x>[0-9])(?<y>[0-9]) (?<t>.+)$/
      </parse>
    ])
    time = Time.parse("2012-04-02 18:20:59").to_i
    d3.run(default_tag: @tag) do
      d3.feed(time, {'data' => '12 20120402182059'})
      d3.feed(time, {'data' => '34 20120402182100'})
      d3.feed(time, {'data' => 'xy 20120402182101'})
    end
    filtered = d3.filtered
    assert_equal 2, filtered.length

    d4 = create_driver(%[
      key_name data
      <parse>
        @type json
      </parse>
    ])
    time = Time.parse("2012-04-02 18:20:59").to_i
    d4.run(default_tag: @tag) do
      d4.feed(time, {'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'})
      d4.feed(time, {'data' => 'foobar', 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d4.filtered
    assert_equal 1, filtered.length

  end

  data(:keep_key_name => false,
       :remove_key_name => true)
  def test_filter_with_reserved_data(remove_key_name)
    d1 = create_driver(%[
      key_name data
      reserve_data yes
      remove_key_name_field #{remove_key_name}
      <parse>
        @type regexp
        expression /^(?<x>\\d)(?<y>\\d) (?<t>.+)$/
      </parse>
    ])
    time = event_time("2012-04-02 18:20:59")
    d1.run(default_tag: @tag) do
      d1.feed(time, {'data' => '12 20120402182059'})
      d1.feed(time, {'data' => '34 20120402182100'})
      d1.feed(time, {'data' => 'xy 20120402182101'})
    end
    filtered = d1.filtered
    assert_equal 3, filtered.length

    d2 = create_driver(%[
      key_name data
      reserve_data yes
      remove_key_name_field #{remove_key_name}
      <parse>
        @type json
      </parse>
    ])
    time = Fluent::EventTime.from_time(@default_time)
    d2.run(default_tag: @tag) do
      d2.feed(time, {'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'})
      d2.feed(time, {'data' => 'foobar', 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d2.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal_event_time time, first[0]
    if remove_key_name
      assert_not_include first[1], 'data'
    else
      assert_equal '{"xxx":"first","yyy":"second"}', first[1]['data']
    end
    assert_equal 'first', first[1]['xxx']
    assert_equal 'second', first[1]['yyy']

    second = filtered[1]
    assert_equal_event_time time, second[0]
    assert_equal 'foobar', second[1]['data']
    assert_equal 'x', second[1]['xxx']
    assert_equal 'y', second[1]['yyy']
  end


  CONFIG_LTSV =  %[
    key_name data
    <parse>
      @type ltsv
    </parse>
  ]
  CONFIG_LTSV_WITH_TYPES =  %[
    key_name data
    <parse>
      @type ltsv
      types i:integer,s:string,f:float,b:bool
    </parse>
  ]
  data(:event_time => lambda { |time| Fluent::EventTime.from_time(time) },
       :int_time => lambda { |time| time.to_i })
  def test_filter_ltsv(time_parse)
    d = create_driver(CONFIG_LTSV)
    time = time_parse.call(@default_time)
    d.run(default_tag: @tag) do
      d.feed(time, {'data' => "xxx:first\tyyy:second", 'xxx' => 'x', 'yyy' => 'y'})
      d.feed(time, {'data' => "xxx:first\tyyy:second2", 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal_parsed_time time, first[0]
    assert_nil first[1]['data']
    assert_equal 'first', first[1]['xxx']
    assert_equal 'second', first[1]['yyy']

    second = filtered[1]
    assert_equal_parsed_time time, second[0]
    assert_nil first[1]['data']
    assert_equal 'first', second[1]['xxx']
    assert_equal 'second2', second[1]['yyy']

    d = create_driver(CONFIG_LTSV + %[reserve_data yes])
    time = @default_time.to_i
    d.run(default_tag: @tag) do
      d.feed(time, {'data' => "xxx:first\tyyy:second", 'xxx' => 'x', 'yyy' => 'y'})
      d.feed(time, {'data' => "xxx:first\tyyy:second2", 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 2, filtered.length

    first = filtered[0]
    assert_equal_parsed_time time, first[0]
    assert_equal "xxx:first\tyyy:second", first[1]['data']
    assert_equal 'first', first[1]['xxx']
    assert_equal 'second', first[1]['yyy']

    second = filtered[1]
    assert_equal_parsed_time time, second[0]
    assert_equal "xxx:first\tyyy:second", first[1]['data']
    assert_equal 'first', second[1]['xxx']
    assert_equal 'second2', second[1]['yyy']

    # convert types
    #d = create_driver(CONFIG_LTSV + %[
    d = create_driver(CONFIG_LTSV_WITH_TYPES)
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => "i:1\ts:2\tf:3\tb:true\tx:123"})
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal_parsed_time time, first[0]
    assert_equal 1, first[1]['i']
    assert_equal '2', first[1]['s']
    assert_equal 3.0, first[1]['f']
    assert_equal true, first[1]['b']
    assert_equal '123', first[1]['x']
  end

  CONFIG_TSV =  %[
    key_name data
    <parse>
      @type tsv
      keys key1,key2,key3
    </parse>
  ]
  data(:event_time => lambda { |time| Fluent::EventTime.from_time(time) },
       :int_time => lambda { |time| time.to_i })
  def test_filter_tsv(time_parse)
    d = create_driver(CONFIG_TSV)
    time = time_parse.call(@default_time)
    d.run do
      d.feed(@tag, time, {'data' => "value1\tvalue2\tvalueThree", 'xxx' => 'x', 'yyy' => 'y'})
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal_parsed_time time, first[0]
    assert_nil first[1]['data']
    assert_equal 'value1', first[1]['key1']
    assert_equal 'value2', first[1]['key2']
    assert_equal 'valueThree', first[1]['key3']
  end

  CONFIG_CSV =  %[
    key_name data
    <parse>
      @type csv
      keys key1,key2,key3
    </parse>
  ]
  CONFIG_CSV_COMPAT =  %[
    key_name data
    format csv
    keys key1,key2,key3
  ]
  data(new_conf: CONFIG_CSV,
       compat_conf: CONFIG_CSV_COMPAT)
  def test_filter_csv(conf)
    d = create_driver(conf)
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

  def test_filter_with_nested_record
    d = create_driver(%[
      key_name $.data.log
      <parse>
        @type csv
        keys key1,key2,key3
      </parse>
    ])
    time = @default_time.to_i
    d.run do
      d.feed(@tag, time, {'data' => {'log' => 'value1,"value2","value""ThreeYes!"'}, 'xxx' => 'x', 'yyy' => 'y'})
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
    key_name data
    hash_value_field parsed
    <parse>
      @type json
    </parse>
  ]
  CONFIG_HASH_VALUE_FIELD_RESERVE_DATA = %[
    key_name data
    reserve_data yes
    hash_value_field parsed
    <parse>
      @type json
    </parse>
  ]
  CONFIG_HASH_VALUE_FIELD_WITH_INJECT_KEY_PREFIX = %[
    key_name data
    hash_value_field parsed
    inject_key_prefix data.
    <parse>
      @type json
    </parse>
  ]
  data(:event_time => lambda { |time| Fluent::EventTime.from_time(time) },
       :int_time => lambda { |time| time.to_i })
  def test_filter_inject_hash_value_field(time_parse)
    original = {'data' => '{"xxx":"first","yyy":"second"}', 'xxx' => 'x', 'yyy' => 'y'}

    d = create_driver(CONFIG_HASH_VALUE_FIELD)
    time = time_parse.call(@default_time)
    d.run do
      d.feed(@tag, time, original)
    end
    filtered = d.filtered
    assert_equal 1, filtered.length

    first = filtered[0]
    assert_equal_parsed_time time, first[0]

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
    assert_equal_parsed_time time, first[0]

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
    assert_equal_parsed_time time, first[0]

    record = first[1]
    assert_equal 1, record.keys.size
    assert_equal({"data.xxx"=>"first","data.yyy"=>"second"}, record['parsed'])
  end

  CONFIG_DONT_PARSE_TIME = %[
    key_name data
    reserve_time true
    <parse>
      @type json
      keep_time_key true
    </parse>
  ]
  CONFIG_DONT_PARSE_TIME_COMPAT = %[
    key_name data
    reserve_time true
    format json
    keep_time_key true
  ]
  data(new_conf: CONFIG_DONT_PARSE_TIME,
       compat_conf: CONFIG_DONT_PARSE_TIME_COMPAT)
  def test_time_should_be_reserved(conf)
    t = Time.now.to_i
    d = create_driver(conf)
    d.run(default_tag: @tag) do
      d.feed(t, {'data' => '{"time":1383190430, "f1":"v1"}'})
      d.feed(t, {'data' => '{"time":"1383190430", "f1":"v1"}'})
      d.feed(t, {'data' => '{"time":"2013-10-31 12:34:03 +0900", "f1":"v1"}'})
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
    key_name data
    <parse>
      @type json
    </parse>
  ] # 'time' is implicit @time_key
  def test_filter_invalid_time_data
    # should not raise errors
    time = Time.now.to_i
    d = create_driver(CONFIG_INVALID_TIME_VALUE)
    assert_nothing_raised {
      d.run(default_tag: @tag) do
        d.feed(time, {'data' => '{"time":[], "f1":"v1"}'})
        d.feed(time, {'data' => '{"time":"thisisnottime", "f1":"v1"}'})
      end
    }
    filtered = d.filtered
    assert_equal 1, filtered.length

    assert_equal 0, filtered[0][0].to_i
    assert_equal 'v1', filtered[0][1]['f1']
    assert_equal nil, filtered[0][1]['time']
  end

  # REGEXP = /^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$/

  CONFIG_NOT_REPLACE = %[
    key_name data
    <parse>
      @type regexp
      expression /^(?<message>.*)$/
    </parse>
  ]
  CONFIG_INVALID_BYTE = CONFIG_NOT_REPLACE + %[
    replace_invalid_sequence true
  ]
  def test_filter_invalid_byte
    invalid_utf8 = "\xff".force_encoding('UTF-8')

    d = create_driver(CONFIG_NOT_REPLACE)
    d.run do
      d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => invalid_utf8})
    end
    error_event = d.error_events.first
    assert_equal "test", error_event[0]
    assert_instance_of ArgumentError, error_event[3]

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
    key_name data
    hash_value_field parsed
    <parse>
      @type json
    </parse>
  ]
  CONFIG_PASS_SAME_RECORD = CONFIG_NOT_IGNORE + %[
    reserve_data true
  ]
  def test_filter_key_not_exist
    d = create_driver(CONFIG_NOT_IGNORE)
    flexmock(d.instance.router).should_receive(:emit_error_event).
      with(String, Integer, Hash, ArgumentError.new("data does not exist")).once
    assert_nothing_raised {
      d.run do
        d.feed(@tag, Fluent::EventTime.now.to_i, {'foo' => 'bar'})
      end
    }

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

  # emit_error_event test
  INVALID_MESSAGE = 'foo bar'
  VALID_MESSAGE   = 'col1=foo col2=bar'

  def test_call_emit_error_event_when_parser_error
    d = create_driver(CONFIG_INVALID_TIME_VALUE)
    flexmock(d.instance.router).should_receive(:emit_error_event).
      with(String, Integer, Hash, ParserError).once
    d.run do
      d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => '{"time":[], "f1":"v1"}'})
    end
  end

  CONFIG_UNMATCHED_PATTERN_LOG = %[
    key_name message
    <parse>
      @type regexp
      expression /^col1=(?<col1>.+) col2=(?<col2>.+)$/
    </parse>
  ]

  class UnmatchedPatternLogTest < self
    setup do
      @d = create_driver(CONFIG_UNMATCHED_PATTERN_LOG)
    end

    def test_call_emit_error_event_when_pattern_is_mismached
      flexmock(@d.instance.router).should_receive(:emit_error_event).
        with(String, Integer, Hash, ParserError.new("pattern not match with data '#{INVALID_MESSAGE}'")).once
      @d.run do
        @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => INVALID_MESSAGE})
      end
    end

    def test_not_call_emit_error_event_when_pattern_is_mached
      flexmock(@d.instance.router).should_receive(:emit_error_event).never
      @d.run do
        @d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => VALID_MESSAGE})
      end
    end
  end

  class EmitInvalidRecordToErrorTest < self
    def test_pattern_is_mismached_with_emit_invalid_record_to_error
      d = create_driver(CONFIG_UNMATCHED_PATTERN_LOG + "emit_invalid_record_to_error false")
      flexmock(d.instance.router).should_receive(:emit_error_event).never
      assert_nothing_raised {
        d.run do
          d.feed(@tag, Fluent::EventTime.now.to_i, {'message' => INVALID_MESSAGE})
        end
      }
      assert_equal 0, d.filtered.length
    end

    def test_parser_error_with_emit_invalid_record_to_error
      d = create_driver(CONFIG_INVALID_TIME_VALUE + "emit_invalid_record_to_error false")
      flexmock(d.instance.router).should_receive(:emit_error_event).never
      assert_nothing_raised {
        d.run do
          d.feed(@tag, Fluent::EventTime.now.to_i, {'data' => '{"time":[], "f1":"v1"}'})
        end
      }
      assert_equal 0, d.filtered.length
    end

    def test_key_not_exist_with_emit_invalid_record_to_error
      d = create_driver(CONFIG_NOT_IGNORE + "emit_invalid_record_to_error false")
      flexmock(d.instance.router).should_receive(:emit_error_event).never
      assert_nothing_raised {
        d.run do
          d.feed(@tag, Fluent::EventTime.now.to_i, {'foo' => 'bar'})
        end
      }
      assert_equal 0, d.filtered.length
    end
  end
end
