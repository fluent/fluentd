require_relative '../helper'
require 'fluent/plugin_helper/extract'
require 'fluent/time'

class ExtractHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :extract
  end

  class Dummy2 < Fluent::Plugin::TestBase
    helpers :extract
    config_section :extract do
      config_set_default :tag_key, 'tag2'
    end
  end

  def config_extract_section(hash = {})
    config_element('ROOT', '', {}, [config_element('extract', '', hash)])
  end

  setup do
    Fluent::Test.setup
    @d = Dummy.new
  end

  teardown do
    if @d
      @d.stop unless @d.stopped?
      @d.shutdown unless @d.shutdown?
      @d.close unless @d.closed?
      @d.terminate unless @d.terminated?
    end
  end

  test 'can override default parameters, but not overwrite whole definition' do
    d = Dummy.new
    d.configure(config_element())
    assert_nil d.extract_config

    d = Dummy2.new
    d.configure(config_element('ROOT', '', {}, [config_element('extract')]))
    assert d.extract_config
    assert_equal 'tag2', d.extract_config.tag_key
  end

  test 'returns nil in default' do
    @d.configure(config_extract_section())
    @d.start
    assert_nil @d.instance_eval{ @_extract_tag_key }
    assert_nil @d.instance_eval{ @_extract_time_key }
    assert_nil @d.instance_eval{ @_extract_time_parser }

    record = {"key1" => "value1", "key2" => 2, "tag" => "yay", "time" => Time.now.to_i}

    assert_nil @d.extract_tag_from_record(record)
    assert_nil @d.extract_time_from_record(record)
  end

  test 'can be configured as specified' do
    @d.configure(config_extract_section(
        "tag_key" => "tag",
        "time_key" => "time",
        "time_type" => "unixtime",
    ))

    assert_equal "tag", @d.instance_eval{ @_extract_tag_key }
    assert_equal "time", @d.instance_eval{ @_extract_time_key }
    assert_equal :unixtime, @d.instance_eval{ @extract_config.time_type }
    assert_not_nil @d.instance_eval{ @_extract_time_parser }
  end

  sub_test_case 'extract_tag_from_record' do
    test 'returns tag string from specified tag_key field' do
      @d.configure(config_extract_section("tag_key" => "tag"))
      @d.start
      @d.after_start

      tag = @d.extract_tag_from_record({"tag" => "tag.test.code", "message" => "yay!"})
      assert_equal "tag.test.code", tag
    end

    test 'returns tag as string by stringifying values from specified key' do
      @d.configure(config_extract_section("tag_key" => "tag"))
      @d.start
      @d.after_start

      tag = @d.extract_tag_from_record({"tag" => 100, "message" => "yay!"})
      assert_equal "100", tag
    end
  end

  sub_test_case 'extract_time_from_record' do
    test 'returns EventTime object from specified time_key field, parsed as float in default' do
      @d.configure(config_extract_section("time_key" => "t"))
      @d.start
      @d.after_start

      # 1473135272 => 2016-09-06 04:14:32 UTC
      t = @d.extract_time_from_record({"t" => 1473135272.5, "message" => "yay!"})
      assert_equal_event_time(Fluent::EventTime.new(1473135272, 500_000_000), t)

      t = @d.extract_time_from_record({"t" => "1473135272.5", "message" => "yay!"})
      assert_equal_event_time(Fluent::EventTime.new(1473135272, 500_000_000), t)
    end

    test 'returns EventTime object, parsed as unixtime when configured so' do
      @d.configure(config_extract_section("time_key" => "t", "time_type" => "unixtime"))
      @d.start
      @d.after_start

      t = @d.extract_time_from_record({"t" => 1473135272, "message" => "yay!"})
      assert_equal_event_time(Fluent::EventTime.new(1473135272, 0), t)

      t = @d.extract_time_from_record({"t" => "1473135272", "message" => "yay!"})
      assert_equal_event_time(Fluent::EventTime.new(1473135272, 0), t)

      t = @d.extract_time_from_record({"t" => 1473135272.5, "message" => "yay!"})
      assert_equal_event_time(Fluent::EventTime.new(1473135272, 0), t)
    end

    test 'returns EventTime object, parsed by default time parser of ruby with timezone in data' do
      t = with_timezone("UTC-02") do
        @d.configure(config_extract_section("time_key" => "t", "time_type" => "string"))
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "2016-09-06 13:27:01 +0900", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 +0900"), t)
    end

    test 'returns EventTime object, parsed by default time parser of ruby as localtime' do
      t = with_timezone("UTC-02") do
        @d.configure(config_extract_section("time_key" => "t", "time_type" => "string"))
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "2016-09-06 13:27:01", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 +0200"), t)
    end

    test 'returns EventTime object, parsed as configured time_format with timezone' do
      t = with_timezone("UTC-02") do
        @d.configure(config_extract_section("time_key" => "t", "time_type" => "string", "time_format" => "%H:%M:%S, %m/%d/%Y, %z"))
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "13:27:01, 09/06/2016, -0700", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 -0700"), t)
    end

    test 'returns EventTime object, parsed as configured time_format in localtime without timezone' do
      t = with_timezone("UTC-02") do
        @d.configure(config_extract_section("time_key" => "t", "time_type" => "string", "time_format" => "%H:%M:%S, %m/%d/%Y"))
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "13:27:01, 09/06/2016", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 +0200"), t)
    end

    test 'returns EventTime object, parsed as configured time_format in utc without timezone, localtime: false' do
      t = with_timezone("UTC-02") do
        c = config_extract_section("time_key" => "t", "time_type" => "string", "time_format" => "%H:%M:%S, %m/%d/%Y", "localtime" => "false")
        @d.configure(c)
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "13:27:01, 09/06/2016", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 UTC"), t)
    end

    test 'returns EventTime object, parsed as configured time_format in utc without timezone, utc: true' do
      t = with_timezone("UTC-02") do
        c = config_extract_section("time_key" => "t", "time_type" => "string", "time_format" => "%H:%M:%S, %m/%d/%Y", "utc" => "true")
        @d.configure(c)
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "13:27:01, 09/06/2016", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 UTC"), t)
    end

    test 'returns EventTime object, parsed as configured time_format in configured timezone' do
      t = with_timezone("UTC-02") do
        c = config_extract_section("time_key" => "t", "time_type" => "string", "time_format" => "%H:%M:%S, %m/%d/%Y", "timezone" => "+09:00")
        @d.configure(c)
        @d.start
        @d.after_start
        @d.extract_time_from_record({"t" => "13:27:01, 09/06/2016", "message" => "yay!"})
      end
      assert_equal_event_time(event_time("2016-09-06 13:27:01 +0900"), t)
    end
  end
end
