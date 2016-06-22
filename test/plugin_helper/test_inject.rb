require_relative '../helper'
require 'fluent/plugin_helper/inject'
require 'fluent/event'
require 'time'

class InjectHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :inject
  end

  def config_inject_section(hash = {})
    config_element('ROOT', '', {}, [config_element('inject', '', hash)])
  end

  setup do
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

  test 'do nothing in default' do
    @d.configure(config_inject_section())
    @d.start
    assert_nil @d.instance_eval{ @_inject_host_key }
    assert_nil @d.instance_eval{ @_inject_host_name }
    assert_nil @d.instance_eval{ @_inject_tag_key }
    assert_nil @d.instance_eval{ @_inject_time_key }
    assert_nil @d.instance_eval{ @_inject_time_formatter }

    time = event_time()
    record = {"key1" => "value1", "key2" => 2}
    assert_equal record, @d.inject_record('tag', time, record)
    assert_equal record.object_id, @d.inject_record('tag', time, record).object_id

    es0 = Fluent::OneEventStream.new(time, {"key1" => "v", "key2" => 0})

    es1 = Fluent::ArrayEventStream.new([ [time, {"key1" => "a", "key2" => 1}], [time, {"key1" => "b", "key2" => 2}] ])

    es2 = Fluent::MultiEventStream.new
    es2.add(event_time(), {"key1" => "a", "key2" => 1})
    es2.add(event_time(), {"key1" => "b", "key2" => 2})

    es3 = Fluent::MessagePackEventStream.new(es2.to_msgpack_stream)

    [es0, es1, es2, es3].each do |es|
      assert_equal es, @d.inject_event_stream('tag', es), "failed for #{es.class}"
      assert_equal es.object_id, @d.inject_event_stream('tag', es).object_id, "failed for #{es.class}"
    end
  end

  test 'can be configured as specified' do
    @d.configure(config_inject_section(
        "hostname_key" => "hostname",
        "hostname" => "myhost.local",
        "tag_key" => "tag",
        "time_key" => "time",
        "time_type" => "string",
        "time_format" => "%Y-%m-%d %H:%M:%S.%N",
        "timezone" => "-0700",
    ))

    assert_equal "hostname", @d.instance_eval{ @_inject_hostname_key }
    assert_equal "myhost.local", @d.instance_eval{ @_inject_hostname }
    assert_equal "tag", @d.instance_eval{ @_inject_tag_key }
    assert_equal "time", @d.instance_eval{ @_inject_time_key }
    assert_equal :string, @d.instance_eval{ @inject_config.time_type }
    assert_not_nil @d.instance_eval{ @_inject_time_formatter }
  end

  sub_test_case 'using inject_record' do
    test 'injects hostname automatically detected' do
      detected_hostname = `hostname`.chomp
      @d.configure(config_inject_section("hostname_key" => "host"))
      logs = @d.log.out.logs
      assert{ logs.first.include?('[info]: using hostname for specified field host_key="host" host_name="SATOSHI-no-MacBook-Air.local"') }
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"host" => detected_hostname}), @d.inject_record('tag', time, record)
    end

    test 'injects hostname as specified value' do
      @d.configure(config_inject_section("hostname_key" => "host", "hostname" => "myhost.yay.local"))
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"host" => "myhost.yay.local"}), @d.inject_record('tag', time, record)
    end

    test 'injects tag into specified key' do
      @d.configure(config_inject_section("tag_key" => "mytag"))
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"mytag" => "tag.test"}), @d.inject_record('tag.test', time, record)
    end

    test 'injects time as floating point value into specified key as default' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0900").to_i # 1466464211 in unix time
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)
      float_time = 1466464211.320101 # microsecond precision in float

      @d.configure(config_inject_section("time_key" => "timedata"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => float_time}), @d.inject_record('tag', time, record)
    end

    test 'injects time as unix time into specified key' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0900").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)
      int_time = 1466464211

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "unixtime"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => int_time}), @d.inject_record('tag', time, record)
    end

    test 'injects time as formatted string in localtime if timezone not specified' do
      local_timezone = Time.now.strftime('%z')
      time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 08:10:11 #{local_timezone}"}), @d.inject_record('tag', time, record)
    end

    test 'injects time as formatted string with nanosecond in localtime if timezone not specified' do
      local_timezone = Time.now.strftime('%z')
      time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%N %z"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 08:10:11.320101224 #{local_timezone}"}), @d.inject_record('tag', time, record)
    end

    test 'injects time as formatted string with millisecond in localtime if timezone not specified' do
      local_timezone = Time.now.strftime('%z')
      time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%3N %z"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 08:10:11.320 #{local_timezone}"}), @d.inject_record('tag', time, record)
    end

    test 'injects time as formatted string in specified timezone' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0000").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z", "timezone" => "-0800"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 00:10:11 -0800"}), @d.inject_record('tag', time, record)
    end

    test 'injects hostname, tag and time' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0900").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section(
          "hostname_key" => "hostnamedata",
          "hostname" => "myname.local",
          "tag_key" => "tagdata",
          "time_key" => "timedata",
          "time_type" => "string",
          "time_format" => "%Y_%m_%d %H:%M:%S.%N %z",
          "timezone" => "+0000",
      ))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      injected = {"hostnamedata" => "myname.local", "tagdata" => "tag", "timedata" => "2016_06_20 23:10:11.320101224 +0000"}
      assert_equal record.merge(injected), @d.inject_record('tag', time, record)
    end
  end
  sub_test_case 'using inject_event_stream' do
    local_timezone = Time.now.strftime('%z')
    time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
    time_subsecond = 320_101_224
    time = Fluent::EventTime.new(time_in_unix, time_subsecond)
    int_time = 1466464211
    float_time = 1466464211.320101

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects hostname automatically detected' do |data|
      detected_hostname = `hostname`.chomp
      @d.configure(config_inject_section("hostname_key" => "host"))
      logs = @d.log.out.logs
      assert{ logs.first.include?("[info]: using hostname for specified field host_key=\"host\" host_name=\"#{detected_hostname}\"") }
      @d.start

      injected = {"host" => detected_hostname}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects hostname as specified value' do |data|
      @d.configure(config_inject_section("hostname_key" => "host", "hostname" => "myhost.yay.local"))
      @d.start

      injected = {"host" => "myhost.yay.local"}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects tag into specified key' do |data|
      @d.configure(config_inject_section("tag_key" => "mytag"))
      @d.start

      injected = {"mytag" => "tag"}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as floating point value into specified key as default' do |data|
      @d.configure(config_inject_section("time_key" => "timedata"))
      @d.start

      injected = {"timedata" => float_time}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as unix time into specified key' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "unixtime"))
      @d.start

      injected = {"timedata" => int_time}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string in localtime if timezone not specified' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z"))
      @d.start

      injected = {"timedata" => "2016_06_21 08:10:11 #{local_timezone}"}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string with nanosecond in localtime if timezone not specified' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%N %z"))
      @d.start

      injected = {"timedata" => "2016_06_21 08:10:11.320101224 #{local_timezone}"}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string with millisecond in localtime if timezone not specified' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%3N %z"))
      @d.start

      injected = {"timedata" => "2016_06_21 08:10:11.320 #{local_timezone}"}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string in specified timezone' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z", "timezone" => "-0800"))
      @d.start

      injected = {"timedata" => Time.at(int_time).localtime("-08:00").strftime("%Y_%m_%d %H:%M:%S -0800")}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects hostname, tag and time' do |data|
      @d.configure(config_inject_section(
          "hostname_key" => "hostnamedata",
          "hostname" => "myname.local",
          "tag_key" => "tagdata",
          "time_key" => "timedata",
          "time_type" => "string",
          "time_format" => "%Y_%m_%d %H:%M:%S.%N %z",
          "timezone" => "+0000",
      ))
      @d.start

      injected = {"hostnamedata" => "myname.local", "tagdata" => "tag", "timedata" => "2016_06_20 23:10:11.320101224 +0000"}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_event_stream('tag', data)
    end
  end
end
