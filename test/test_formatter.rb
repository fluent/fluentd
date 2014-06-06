require 'helper'
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

  class OutFileFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = TextFormatter::TEMPLATE_REGISTRY.lookup('out_file').call
      @time = Engine.now
    end

    def test_format
      @formatter.configure({})
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{time2str(@time)}\t#{tag}\t#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_time
      @formatter.configure('output_time' => 'false')
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{tag}\t#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_tag
      @formatter.configure('output_tag' => 'false')
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{time2str(@time)}\t#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_without_time_and_tag
      @formatter.configure('output_tag' => 'false', 'output_time' => 'false')
      formatted = @formatter.format('tag', @time, record)

      assert_equal("#{Yajl.dump(record)}\n", formatted)
    end
  end

  class JsonFormatterTest < ::Test::Unit::TestCase
    include FormatterTest

    def setup
      @formatter = TextFormatter::JSONFormatter.new
      @time = Engine.now
    end

    def test_format
      @formatter.configure({})
      formatted = @formatter.format(tag, @time, record)

      assert_equal("#{Yajl.dump(record)}\n", formatted)
    end

    def test_format_with_include_tag
      @formatter.configure('include_tag_key' => 'true', 'tag_key' => 'foo')
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['foo'] = tag
      assert_equal("#{Yajl.dump(r)}\n", formatted)
    end

    def test_format_with_include_time
      @formatter.configure('include_time_key' => 'true', 'localtime' => '')
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['time'] = time2str(@time, true)
      assert_equal("#{Yajl.dump(r)}\n", formatted)
    end

    def test_format_with_include_time_as_number
      @formatter.configure('include_time_key' => 'true', 'time_as_epoch' => 'true', 'time_key' => 'epoch')
      formatted = @formatter.format(tag, @time, record.dup)

      r = record
      r['epoch'] = @time
      assert_equal("#{Yajl.dump(r)}\n", formatted)
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

    def test_find_formatter
      $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(__FILE__)), 'scripts'))
      assert_nothing_raised ConfigError do
        TextFormatter::TEMPLATE_REGISTRY.lookup('known')
      end
    end
  end
end
