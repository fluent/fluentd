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
      Fluent::Test::FormatterTestDriver.new(Formatter).configure(conf)
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
      assert_equal [], @formatter.fields
    end

    data(
      'tab_char' => ["\t", '\t'],
      'tab_string' => ["\t", 'TAB'],
      'pipe' => ['|', '|'])
    def test_config_params_with_customized_delimiters(data)
      expected, target = data
      @formatter.configure('delimiter' => target)
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
      formatter = Fluent::Plugin.new_formatter('single_value')
      formatted = formatter.format('tag', Engine.now, {'message' => 'awesome'})
      assert_equal("awesome\n", formatted)
    end

    def test_format_without_newline
      formatter = Fluent::Plugin.new_formatter('single_value')
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
        Fluent::Plugin.new_formatter('unknown')
      end
    end

    data('register_formatter' => 'known', 'register_template' => 'known_old')
    def test_find_formatter(data)
      $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(__FILE__)), 'scripts'))
      assert_nothing_raised ConfigError do
        Fluent::Plugin.new_formatter(data)
      end
      $LOAD_PATH.shift
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
