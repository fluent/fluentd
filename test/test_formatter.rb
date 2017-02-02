require_relative 'helper'
require 'fluent/test'
require 'fluent/formatter'

module FormatterTest
  include Fluent

  def time2str(time, localtime = false, format = nil)
    if format
      if localtime
        Time.at(time).strftime(format)
      else
        Time.at(time).utc.strftime(format)
      end
    else
      if localtime
        Time.at(time).iso8601
      else
        Time.at(time).utc.iso8601
      end
    end
  end

  def tag
    'tag'
  end

  def record
    {'message' => 'awesome'}
  end

  def symbolic_record
    {:message => :awesome}
  end

  def with_timezone(tz)
    oldtz, ENV['TZ'] = ENV['TZ'], tz
    yield
  ensure
    ENV['TZ'] = oldtz
  end

  class BaseFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def test_call
      formatter = Formatter.new
      formatter.configure({})
      assert_raise NotImplementedError do
        formatter.format('tag', Engine.now, {})
      end
    end
  end

  class BaseFormatterTestWithTestDriver < ::Test::Unit::TestCase
    include FormatterTest

    def create_driver(conf={})
      formatter = Fluent::Test::FormatterTestDriver.new(Formatter).configure(conf)
    end

    def test_call
      d = create_driver
      assert_raise NotImplementedError do
        d.format('tag', Engine.now, {})
      end
    end

    def test_call_with_string_literal_configure
      d = create_driver('')
      assert_raise NotImplementedError do
        d.format('tag', Engine.now, {})
      end
    end
  end

  class OutFileFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = Fluent::Test::FormatterTestDriver.new('out_file')
      @time = Engine.now
    end

    def configure(conf)
      @formatter.configure({'utc' => true}.merge(conf))
    end

    def test_format
      configure({})
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{time2str(@time)}\t#{tag}\t#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_time
      configure('output_time' => 'false')
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{tag}\t#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_tag
      configure('output_tag' => 'false')
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{time2str(@time)}\t#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_time_and_tag
      configure('output_tag' => 'false', 'output_time' => 'false')
      formatted = @formatter.format('tag', @time, record)

      assert_equal("#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_time_and_tag_against_string_literal_configure
      @formatter.configure(%[
        utc         true
        output_tag  false
        output_time false
      ])
      formatted = @formatter.format('tag', @time, record)

      assert_equal("#{Yajl.dump(record)}\n", formatted)
    end
  end

  class JsonFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = Fluent::Test::FormatterTestDriver.new(TextFormatter::JSONFormatter)
      @time = Engine.now
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    def test_format(data)
      @formatter.configure('json_parser' => data)
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{Yajl.dump(record)}\n", formatted)
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    def test_format_without_nl(data)
      @formatter.configure('json_parser' => data, 'add_newline' => false)
      formatted = @formatter.format(tag, @time, record)

      assert_equal(Yajl.dump(record), formatted)
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    def test_format_with_symbolic_record(data)
      @formatter.configure('json_parser' => data)
      formatted = @formatter.format(tag, @time, symbolic_record)

      assert_equal("#{JSON.generate(record)}\n", formatted)
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    def test_format_with_include_tag(data)
      @formatter.configure('include_tag_key' => 'true', 'tag_key' => 'foo', 'json_parser' => data)
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['foo'] = tag
      assert_equal("#{Yajl.dump(r)}\n", formatted)
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    def test_format_with_include_time(data)
      @formatter.configure('include_time_key' => 'true', 'localtime' => '', 'json_parser' => data)
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['time'] = time2str(@time, true)
      assert_equal("#{Yajl.dump(r)}\n", formatted)
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    def test_format_with_include_time_as_number(data)
      @formatter.configure('include_time_key' => 'true', 'time_as_epoch' => 'true', 'time_key' => 'epoch', 'json_parser' => data)
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['epoch'] = @time
      assert_equal("#{Yajl.dump(r)}\n", formatted)
    end
  end

  class MessagePackFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = TextFormatter::MessagePackFormatter.new
      @time = Engine.now
    end

    def test_format
      @formatter.configure({})
      formatted = @formatter.format(tag, @time, record)

      assert_equal(record.to_msgpack, formatted)
    end

    def test_format_with_include_tag
      @formatter.configure('include_tag_key' => 'true', 'tag_key' => 'foo')
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['foo'] = tag
      assert_equal(r.to_msgpack, formatted)
    end

    def test_format_with_include_time
      @formatter.configure('include_time_key' => 'true', 'localtime' => '')
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['time'] = time2str(@time, true)
      assert_equal(r.to_msgpack, formatted)
    end

    def test_format_with_include_time_as_number
      @formatter.configure('include_time_key' => 'true', 'time_as_epoch' => 'true', 'time_key' => 'epoch')
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['epoch'] = @time
      assert_equal(r.to_msgpack, formatted)
    end
  end

  class LabeledTSVFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = TextFormatter::LabeledTSVFormatter.new
      @time = Engine.now
    end

    def test_config_params
      assert_equal "\t", @formatter.delimiter
      assert_equal  ":", @formatter.label_delimiter

      @formatter.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )

      assert_equal ",", @formatter.delimiter
      assert_equal "=", @formatter.label_delimiter
    end

    def test_format
      @formatter.configure({})
      formatted = @formatter.format(tag, @time, record)

      assert_equal("message:awesome\n", formatted)
    end

    def test_format_without_nl
      @formatter.configure('add_newline' => false)
      formatted = @formatter.format(tag, @time, record)

      assert_equal("message:awesome", formatted)
    end

    def test_format_with_tag
      @formatter.configure('include_tag_key' => 'true')
      formatted = @formatter.format(tag, @time, record)

      assert_equal("message:awesome\ttag:tag\n", formatted)
    end

    def test_format_with_time
      @formatter.configure('include_time_key' => 'true', 'time_format' => '%Y')
      formatted = @formatter.format(tag, @time, record)

      assert_equal("message:awesome\ttime:#{Time.now.year}\n", formatted)
    end

    def test_format_with_customized_delimiters
      @formatter.configure(
        'include_tag_key' => 'true',
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )
      formatted = @formatter.format(tag, @time, record)

      assert_equal("message=awesome,tag=tag\n", formatted)
    end
  end

  class CsvFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = TextFormatter::CsvFormatter.new
      @time = Engine.now
    end
    
    def test_config_params
      assert_equal ',', @formatter.delimiter
      assert_equal true, @formatter.force_quotes
      assert_nil @formatter.fields
    end

    data('empty array' => [],
         'array including empty string' => ['', ''])
    def test_empty_fields(param)
      assert_raise ConfigError do
        @formatter.configure('fields' => param)
      end
    end

    data(
      'tab_char' => ["\t", '\t'],
      'tab_string' => ["\t", 'TAB'],
      'pipe' => ['|', '|'])
    def test_config_params_with_customized_delimiters(data)
      expected, target = data
      @formatter.configure('fields' => 'f1', 'delimiter' => target)
      assert_equal expected, @formatter.delimiter
    end

    def test_format
      @formatter.configure('fields' => 'message,message2')
      formatted = @formatter.format(tag, @time, {
        'message' => 'awesome',
        'message2' => 'awesome2'
      })
      assert_equal("\"awesome\",\"awesome2\"\n", formatted)
    end

    def test_format_without_nl
      @formatter.configure('fields' => 'message,message2', 'add_newline' => false)
      formatted = @formatter.format(tag, @time, {
        'message' => 'awesome',
        'message2' => 'awesome2'
      })
      assert_equal("\"awesome\",\"awesome2\"", formatted)
    end

    def test_format_with_tag
      @formatter.configure(
        'fields' => 'tag,message,message2',
        'include_tag_key' => 'true'
      )
      formatted = @formatter.format(tag, @time, {
        'message' => 'awesome',
        'message2' => 'awesome2'
      })
      assert_equal("\"tag\",\"awesome\",\"awesome2\"\n", formatted)
    end

    def test_format_with_time
      @formatter.configure(
        'fields' => 'time,message,message2',
        'include_time_key' => 'true',
        'time_format' => '%Y'
      )
      formatted = @formatter.format(tag, @time, {
        'message' => 'awesome',
        'message2' => 'awesome2'
      })
      assert_equal("\"#{Time.now.year}\",\"awesome\",\"awesome2\"\n",
                   formatted)
    end

    def test_format_with_customized_delimiters
      @formatter.configure(
        'fields' => 'message,message2',
        'delimiter' => '\t'
      )
      formatted = @formatter.format(tag, @time, {
        'message' => 'awesome',
        'message2' => 'awesome2'
      })
      assert_equal("\"awesome\"\t\"awesome2\"\n", formatted)
    end

    def test_format_with_non_quote
      @formatter.configure(
        'fields' => 'message,message2',
        'force_quotes' => 'false'
      )
      formatted = @formatter.format(tag, @time, {
        'message' => 'awesome',
        'message2' => 'awesome2'
      })
      assert_equal("awesome,awesome2\n", formatted)
    end

    data(
      'nil' => {
        'message' => 'awesome',
        'message2' => nil,
        'message3' => 'awesome3'
      },
      'blank' => {
        'message' => 'awesome',
        'message2' => '',
        'message3' => 'awesome3'
      })
    def test_format_with_empty_fields(data)
      @formatter.configure(
        'fields' => 'message,message2,message3'
      )
      formatted = @formatter.format(tag, @time, data)
      assert_equal("\"awesome\",\"\",\"awesome3\"\n", formatted)
    end

    data(
      'normally' => 'one,two,three',
      'white_space' => 'one , two , three',
      'blank' => 'one,,two,three')
    def test_config_params_with_fields(data)
      @formatter.configure('fields' => data)
      assert_equal %w(one two three), @formatter.fields
    end
  end

  class SingleValueFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def test_config_params
      formatter = TextFormatter::SingleValueFormatter.new
      assert_equal "message", formatter.message_key

      formatter.configure('message_key' => 'foobar')
      assert_equal "foobar", formatter.message_key
    end

    def test_format
      formatter = TextFormatter::TEMPLATE_REGISTRY.lookup('single_value').call
      formatted = formatter.format('tag', Engine.now, {'message' => 'awesome'})
      assert_equal("awesome\n", formatted)
    end

    def test_format_without_newline
      formatter = TextFormatter::TEMPLATE_REGISTRY.lookup('single_value').call
      formatter.configure('add_newline' => 'false')
      formatted = formatter.format('tag', Engine.now, {'message' => 'awesome'})
      assert_equal("awesome", formatted)
    end

    def test_format_with_message_key
      formatter = TextFormatter::SingleValueFormatter.new
      formatter.configure('message_key' => 'foobar')
      formatted = formatter.format('tag', Engine.now, {'foobar' => 'foo'})

      assert_equal("foo\n", formatted)
    end
  end

  class FormatterLookupTest < ::Test::Unit::TestCase
    include FormatterTest

    def test_unknown_format
      assert_raise ConfigError do
        TextFormatter::TEMPLATE_REGISTRY.lookup('unknown')
      end
    end

    data('register_formatter' => 'known', 'register_template' => 'known_old')
    def test_find_formatter(data)
      $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(__FILE__)), 'scripts'))
      assert_nothing_raised ConfigError do
        TextFormatter::TEMPLATE_REGISTRY.lookup(data)
      end
      $LOAD_PATH.shift
    end
  end

  class TimeFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @time = Time.new(2014, 9, 27, 0, 0, 0, 0).to_i
      @fmt  = "%Y%m%d %H%M%z"  # YYYYMMDD HHMM[+-]HHMM
    end

    def format(format, localtime, timezone)
      formatter = Fluent::TimeFormatter.new(format, localtime, timezone)
      formatter.format(@time)
    end

    def test_default_utc_nil
      assert_equal("2014-09-27T00:00:00Z", format(nil, false, nil))
    end

    def test_default_utc_pHH_MM
      assert_equal("2014-09-27T01:30:00+01:30", format(nil, false, "+01:30"))
    end

    def test_default_utc_nHH_MM
      assert_equal("2014-09-26T22:30:00-01:30", format(nil, false, "-01:30"))
    end

    def test_default_utc_pHHMM
      assert_equal("2014-09-27T02:30:00+02:30", format(nil, false, "+0230"))
    end

    def test_default_utc_nHHMM
      assert_equal("2014-09-26T21:30:00-02:30", format(nil, false, "-0230"))
    end

    def test_default_utc_pHH
      assert_equal("2014-09-27T03:00:00+03:00", format(nil, false, "+03"))
    end

    def test_default_utc_nHH
      assert_equal("2014-09-26T21:00:00-03:00", format(nil, false, "-03"))
    end

    def test_default_utc_timezone_1
      # Asia/Tokyo (+09:00) does not have daylight saving time.
      assert_equal("2014-09-27T09:00:00+09:00", format(nil, false, "Asia/Tokyo"))
    end

    def test_default_utc_timezone_2
      # Pacific/Honolulu (-10:00) does not have daylight saving time.
      assert_equal("2014-09-26T14:00:00-10:00", format(nil, false, "Pacific/Honolulu"))
    end

    def test_default_utc_timezone_3
      # America/Argentina/Buenos_Aires (-03:00) does not have daylight saving time.
      assert_equal("2014-09-26T21:00:00-03:00", format(nil, false, "America/Argentina/Buenos_Aires"))
    end

    def test_default_utc_timezone_4
      # Europe/Paris has daylight saving time. Its UTC offset is +01:00 and its
      # UTC offset in DST is +02:00. In September, Europe/Paris is in DST.
      assert_equal("2014-09-27T02:00:00+02:00", format(nil, false, "Europe/Paris"))
    end

    def test_default_utc_timezone_5
      # Europe/Paris has daylight saving time. Its UTC offset is +01:00 and its
      # UTC offset in DST is +02:00. In January, Europe/Paris is not in DST.
      @time = Time.new(2014, 1, 24, 0, 0, 0, 0).to_i
      assert_equal("2014-01-24T01:00:00+01:00", format(nil, false, "Europe/Paris"))
    end

    def test_default_utc_invalid
      assert_equal("2014-09-27T00:00:00Z", format(nil, false, "Invalid"))
    end

    def test_default_localtime_nil_1
      with_timezone("UTC-04") do
        assert_equal("2014-09-27T04:00:00+04:00", format(nil, true, nil))
      end
    end

    def test_default_localtime_nil_2
      with_timezone("UTC+05") do
        assert_equal("2014-09-26T19:00:00-05:00", format(nil, true, nil))
      end
    end

    def test_default_localtime_timezone
      # 'timezone' takes precedence over 'localtime'.
      with_timezone("UTC-06") do
        assert_equal("2014-09-27T07:00:00+07:00", format(nil, true, "+07"))
      end
    end

    def test_specific_utc_nil
      assert_equal("20140927 0000+0000", format(@fmt, false, nil))
    end

    def test_specific_utc_pHH_MM
      assert_equal("20140927 0830+0830", format(@fmt, false, "+08:30"))
    end

    def test_specific_utc_nHH_MM
      assert_equal("20140926 1430-0930", format(@fmt, false, "-09:30"))
    end

    def test_specific_utc_pHHMM
      assert_equal("20140927 1030+1030", format(@fmt, false, "+1030"))
    end

    def test_specific_utc_nHHMM
      assert_equal("20140926 1230-1130", format(@fmt, false, "-1130"))
    end

    def test_specific_utc_pHH
      assert_equal("20140927 1200+1200", format(@fmt, false, "+12"))
    end

    def test_specific_utc_nHH
      assert_equal("20140926 1100-1300", format(@fmt, false, "-13"))
    end

    def test_specific_utc_timezone_1
      # Europe/Moscow (+04:00) does not have daylight saving time.
      assert_equal("20140927 0400+0400", format(@fmt, false, "Europe/Moscow"))
    end

    def test_specific_utc_timezone_2
      # Pacific/Galapagos (-06:00) does not have daylight saving time.
      assert_equal("20140926 1800-0600", format(@fmt, false, "Pacific/Galapagos"))
    end

    def test_specific_utc_timezone_3
      # America/Argentina/Buenos_Aires (-03:00) does not have daylight saving time.
      assert_equal("20140926 2100-0300", format(@fmt, false, "America/Argentina/Buenos_Aires"))
    end

    def test_specific_utc_timezone_4
      # America/Los_Angeles has daylight saving time. Its UTC offset is -08:00 and its
      # UTC offset in DST is -07:00. In September, America/Los_Angeles is in DST.
      assert_equal("20140926 1700-0700", format(@fmt, false, "America/Los_Angeles"))
    end

    def test_specific_utc_timezone_5
      # America/Los_Angeles has daylight saving time. Its UTC offset is -08:00 and its
      # UTC offset in DST is -07:00. In January, America/Los_Angeles is not in DST.
      @time = Time.new(2014, 1, 24, 0, 0, 0, 0).to_i
      assert_equal("20140123 1600-0800", format(@fmt, false, "America/Los_Angeles"))
    end

    def test_specific_utc_invalid
      assert_equal("20140927 0000+0000", format(@fmt, false, "Invalid"))
    end

    def test_specific_localtime_nil_1
      with_timezone("UTC-07") do
        assert_equal("20140927 0700+0700", format(@fmt, true, nil))
      end
    end

    def test_specific_localtime_nil_2
      with_timezone("UTC+08") do
        assert_equal("20140926 1600-0800", format(@fmt, true, nil))
      end
    end

    def test_specific_localtime_timezone
      # 'timezone' takes precedence over 'localtime'.
      with_timezone("UTC-09") do
        assert_equal("20140926 1400-1000", format(@fmt, true, "-10"))
      end
    end
  end

  class TimeConfigTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = TextFormatter::LabeledTSVFormatter.new
      @time      = Time.new(2014, 9, 27, 0, 0, 0, 0).to_i
    end

    def format(conf)
      @formatter.configure({'include_time_key' => true}.merge(conf))
      formatted = @formatter.format("tag", @time, {})
      # Drop the leading "time:" and the trailing "\n".
      formatted[5..-2]
    end

    def test_none
      with_timezone("UTC-01") do
        # 'localtime' is true by default.
        assert_equal("2014-09-27T01:00:00+01:00", format({}))
      end
    end

    def test_utc
      with_timezone("UTC-01") do
        # 'utc' takes precedence over 'localtime'.
        assert_equal("2014-09-27T00:00:00Z", format("utc" => true))
      end
    end

    def test_timezone
      with_timezone("UTC-01") do
        # 'timezone' takes precedence over 'localtime'.
        assert_equal("2014-09-27T02:00:00+02:00", format("timezone" => "+02"))
      end
    end

    def test_utc_timezone
      with_timezone("UTC-01") do
        # 'timezone' takes precedence over 'utc'.
        assert_equal("2014-09-27T09:00:00+09:00", format("utc" => true, "timezone" => "Asia/Tokyo"))
      end
    end
  end
end
