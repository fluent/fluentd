require_relative '../helper'
require 'fluent/test/driver/formatter'
require 'fluent/plugin/formatter_ltsv'

class LabeledTSVFormatterTest < ::Test::Unit::TestCase
  def setup
    @time = event_time
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Formatter.new(Fluent::Plugin::LabeledTSVFormatter).configure(conf)
  end

  def tag
    "tag"
  end

  def record
    {'message' => 'awesome'}
  end

  def test_config_params
    d = create_driver
    assert_equal "\t", d.instance.delimiter
    assert_equal  ":", d.instance.label_delimiter

    d = create_driver(
      'delimiter'       => ',',
      'label_delimiter' => '=',
    )

    assert_equal ",", d.instance.delimiter
    assert_equal "=", d.instance.label_delimiter
  end

  def test_format
    d = create_driver({})
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\n", formatted)
  end

  def test_format_with_tag
    d = create_driver('include_tag_key' => 'true')
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\ttag:tag\n", formatted)
  end

  def test_format_with_time
    d = create_driver('include_time_key' => 'true', 'time_format' => '%Y')
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message:awesome\ttime:#{Time.now.year}\n", formatted)
  end

  def test_format_with_customized_delimiters
    d = create_driver(
      'include_tag_key' => 'true',
      'delimiter'       => ',',
      'label_delimiter' => '=',
    )
    formatted = d.instance.format(tag, @time, record)

    assert_equal("message=awesome,tag=tag\n", formatted)
  end

  sub_test_case "time config" do
    def setup
      @time = event_time("2014-09-27 00:00:00 +00:00").to_i
    end

    def with_timezone(tz)
      oldtz, ENV['TZ'] = ENV['TZ'], tz
      yield
    ensure
      ENV['TZ'] = oldtz
    end

    def format(conf)
      d = create_driver({'include_time_key' => true}.merge(conf))
      formatted = d.instance.format("tag", @time, {})
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
