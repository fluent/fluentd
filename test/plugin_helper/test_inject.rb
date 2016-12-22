require_relative '../helper'
require 'fluent/plugin_helper/inject'
require 'fluent/plugin/output'
require 'fluent/event'
require 'time'

class InjectHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :inject
  end

  class Dummy2 < Fluent::Plugin::TestBase
    helpers :inject
    config_section :inject do
      config_set_default :hostname_key, 'host'
    end
  end

  class Dummy3 < Fluent::Plugin::Output
    helpers :inject
    def write(chunk)
      # dummy
    end
  end

  def config_inject_section(hash = {})
    config_element('ROOT', '', {}, [config_element('inject', '', hash)])
  end

  setup do
    Fluent::Test.setup
    @d = Dummy.new
  end

  teardown do
    if @d
      @d.stop unless @d.stopped?
      @d.before_shutdown unless @d.before_shutdown?
      @d.shutdown unless @d.shutdown?
      @d.after_shutdown unless @d.after_shutdown?
      @d.close unless @d.closed?
      @d.terminate unless @d.terminated?
    end
  end

  test 'can override default parameters, but not overwrite whole definition' do
    d = Dummy.new
    d.configure(config_element())
    assert_nil d.inject_config

    d = Dummy2.new
    d.configure(config_element('ROOT', '', {}, [config_element('inject')]))
    assert d.inject_config
    assert_equal 'host', d.inject_config.hostname_key
  end

  test 'do nothing in default' do
    @d.configure(config_inject_section())
    @d.start
    assert_nil @d.instance_eval{ @_inject_hostname_key }
    assert_nil @d.instance_eval{ @_inject_hostname }
    assert_nil @d.instance_eval{ @_inject_worker_id_key }
    assert_nil @d.instance_eval{ @_inject_worker_id }
    assert_nil @d.instance_eval{ @_inject_tag_key }
    assert_nil @d.instance_eval{ @_inject_time_key }
    assert_nil @d.instance_eval{ @_inject_time_formatter }

    time = event_time()
    record = {"key1" => "value1", "key2" => 2}
    assert_equal record, @d.inject_values_to_record('tag', time, record)
    assert_equal record.object_id, @d.inject_values_to_record('tag', time, record).object_id

    es0 = Fluent::OneEventStream.new(time, {"key1" => "v", "key2" => 0})

    es1 = Fluent::ArrayEventStream.new([ [time, {"key1" => "a", "key2" => 1}], [time, {"key1" => "b", "key2" => 2}] ])

    es2 = Fluent::MultiEventStream.new
    es2.add(event_time(), {"key1" => "a", "key2" => 1})
    es2.add(event_time(), {"key1" => "b", "key2" => 2})

    es3 = Fluent::MessagePackEventStream.new(es2.to_msgpack_stream)

    [es0, es1, es2, es3].each do |es|
      assert_equal es, @d.inject_values_to_event_stream('tag', es), "failed for #{es.class}"
      assert_equal es.object_id, @d.inject_values_to_event_stream('tag', es).object_id, "failed for #{es.class}"
    end
  end

  test 'can be configured as specified' do
    with_worker_config(workers: 1, worker_id: 0) do
      @d.configure(config_inject_section(
          "hostname_key" => "hostname",
          "hostname" => "myhost.local",
          "worker_id_key" => "worker_id",
          "tag_key" => "tag",
          "time_key" => "time",
          "time_type" => "string",
          "time_format" => "%Y-%m-%d %H:%M:%S.%N",
          "timezone" => "-0700",
      ))
    end

    assert_equal "hostname", @d.instance_eval{ @_inject_hostname_key }
    assert_equal "myhost.local", @d.instance_eval{ @_inject_hostname }
    assert_equal "worker_id", @d.instance_eval{ @_inject_worker_id_key }
    assert_equal 0, @d.instance_eval{ @_inject_worker_id }
    assert_equal "tag", @d.instance_eval{ @_inject_tag_key }
    assert_equal "time", @d.instance_eval{ @_inject_time_key }
    assert_equal :string, @d.instance_eval{ @inject_config.time_type }
    assert_not_nil @d.instance_eval{ @_inject_time_formatter }
  end

  test 'raise an error when injected hostname is used in buffer chunk key too' do
    @d = Dummy3.new
    conf = config_element('ROOT', '', {}, [
      config_element('inject', '', {'hostname_key' => 'h'}),
      config_element('buffer', 'tag,h'),
    ])
    assert_raise Fluent::ConfigError.new("the key specified by 'hostname_key' in <inject> cannot be used in buffering chunk key.") do
      @d.configure(conf)
    end
  end

  sub_test_case 'using inject_values_to_record' do
    test 'injects hostname automatically detected' do
      detected_hostname = `hostname`.chomp
      @d.configure(config_inject_section("hostname_key" => "host"))
      logs = @d.log.out.logs
      assert{ logs.any?{|l| l.include?("[info]: using hostname for specified field host_key=\"host\" host_name=\"#{detected_hostname}\"") } }
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"host" => detected_hostname}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects hostname as specified value' do
      @d.configure(config_inject_section("hostname_key" => "host", "hostname" => "myhost.yay.local"))
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"host" => "myhost.yay.local"}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects worker id' do
      with_worker_config(workers: 3, worker_id: 2) do
        @d.configure(config_inject_section("worker_id_key" => "workerid"))
      end
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"workerid" => 2}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects tag into specified key' do
      @d.configure(config_inject_section("tag_key" => "mytag"))
      @d.start

      time = event_time()
      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"mytag" => "tag.test"}), @d.inject_values_to_record('tag.test', time, record)
    end

    test 'injects time as floating point value into specified key as default' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0900").to_i # 1466464211 in unix time
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)
      float_time = 1466464211.320101 # microsecond precision in float

      @d.configure(config_inject_section("time_key" => "timedata"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => float_time}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects time as unix time into specified key' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0900").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)
      int_time = 1466464211

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "unixtime"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => int_time}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects time as formatted string in localtime if timezone not specified' do
      local_timezone = Time.now.strftime('%z')
      time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 08:10:11 #{local_timezone}"}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects time as formatted string with nanosecond in localtime if timezone not specified' do
      local_timezone = Time.now.strftime('%z')
      time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%N %z"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 08:10:11.320101224 #{local_timezone}"}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects time as formatted string with millisecond in localtime if timezone not specified' do
      local_timezone = Time.now.strftime('%z')
      time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%3N %z"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 08:10:11.320 #{local_timezone}"}), @d.inject_values_to_record('tag', time, record)
    end

    test 'injects time as formatted string in specified timezone' do
      time_in_unix = Time.parse("2016-06-21 08:10:11 +0000").to_i
      time_subsecond = 320_101_224
      time = Fluent::EventTime.new(time_in_unix, time_subsecond)

      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z", "timezone" => "-0800"))
      @d.start

      record = {"key1" => "value1", "key2" => 2}
      assert_equal record.merge({"timedata" => "2016_06_21 00:10:11 -0800"}), @d.inject_values_to_record('tag', time, record)
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
      assert_equal record.merge(injected), @d.inject_values_to_record('tag', time, record)
    end
  end

  sub_test_case 'using inject_values_to_event_stream' do
    local_timezone = Time.now.strftime('%z')
    time_in_unix = Time.parse("2016-06-21 08:10:11 #{local_timezone}").to_i
    time_subsecond = 320_101_224
    time_in_rational = Rational(time_in_unix * 1_000_000_000 + time_subsecond, 1_000_000_000)
    time_in_localtime = Time.at(time_in_rational).localtime
    time_in_utc = Time.at(time_in_rational).utc
    time = Fluent::EventTime.new(time_in_unix, time_subsecond)
    time_float = time.to_r.truncate(+6).to_f

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects hostname automatically detected' do |data|
      detected_hostname = `hostname`.chomp
      @d.configure(config_inject_section("hostname_key" => "host"))
      logs = @d.log.out.logs
      assert{ logs.any?{|l| l.include?("[info]: using hostname for specified field host_key=\"host\" host_name=\"#{detected_hostname}\"") } }
      @d.start

      injected = {"host" => detected_hostname}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
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
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
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
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as floating point value into specified key as default' do |data|
      @d.configure(config_inject_section("time_key" => "timedata"))
      @d.start

      injected = {"timedata" => time_float }
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as unix time into specified key' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "unixtime"))
      @d.start

      injected = {"timedata" => time_in_localtime.to_i}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string in localtime if timezone not specified' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z"))
      @d.start

      injected = {"timedata" => time_in_localtime.strftime("%Y_%m_%d %H:%M:%S %z")}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string with nanosecond in localtime if timezone not specified' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%N %z"))
      @d.start

      injected = {"timedata" => time_in_localtime.strftime("%Y_%m_%d %H:%M:%S.%N %z")}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string with millisecond in localtime if timezone not specified' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S.%3N %z"))
      @d.start

      injected = {"timedata" => time_in_localtime.strftime("%Y_%m_%d %H:%M:%S.%3N %z")}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end

    data(
      "OneEventStream" => Fluent::OneEventStream.new(time, {"key1" => "value1", "key2" => 0}),
      "ArrayEventStream" => Fluent::ArrayEventStream.new([ [time, {"key1" => "value1", "key2" => 1}], [time, {"key1" => "value2", "key2" => 2}] ]),
    )
    test 'injects time as formatted string in specified timezone' do |data|
      @d.configure(config_inject_section("time_key" => "timedata", "time_type" => "string", "time_format" => "%Y_%m_%d %H:%M:%S %z", "timezone" => "-0800"))
      @d.start

      injected = {"timedata" => Time.at(time_in_unix).localtime("-08:00").strftime("%Y_%m_%d %H:%M:%S -0800")}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
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

      injected = {"hostnamedata" => "myname.local", "tagdata" => "tag", "timedata" => time_in_utc.strftime("%Y_%m_%d %H:%M:%S.%N %z")}
      expected_es = Fluent::MultiEventStream.new
      data.each do |t, r|
        expected_es.add(t, r.merge(injected))
      end
      assert_equal expected_es, @d.inject_values_to_event_stream('tag', data)
    end
  end

  sub_test_case 'time formatting with modified timezone' do
    setup do
      @time = event_time("2014-09-27 00:00:00 +00:00").to_i
    end

    def format(conf)
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
      assert_equal record.merge(injected), @d.inject_values_to_record('tag', time, record)


      d = create_driver({'include_time_key' => true}.merge(conf))
      formatted = d.instance.format("tag", @time, {})
      # Drop the leading "time:" and the trailing "\n".
      formatted[5..-2]
    end

    def test_nothing_specified_about_time_formatting
      with_timezone("UTC-01") do
        # 'localtime' is true by default.
        @d.configure(config_inject_section("time_key" => "t", "time_type" => "string"))
        @d.start
        record = @d.inject_values_to_record('tag', @time, {"message" => "yay"})

        assert_equal("2014-09-27T01:00:00+01:00", record['t'])
      end
    end

    def test_utc
      with_timezone("UTC-01") do
        # 'utc' takes precedence over 'localtime'.
        @d.configure(config_inject_section("time_key" => "t", "time_type" => "string", "utc" => "true"))
        @d.start
        record = @d.inject_values_to_record('tag', @time, {"message" => "yay"})

        assert_equal("2014-09-27T00:00:00Z", record['t'])
      end
    end

    def test_timezone
      with_timezone("UTC-01") do
        # 'timezone' takes precedence over 'localtime'.
        @d.configure(config_inject_section("time_key" => "t", "time_type" => "string", "timezone" => "+02"))
        @d.start
        record = @d.inject_values_to_record('tag', @time, {"message" => "yay"})

        assert_equal("2014-09-27T02:00:00+02:00", record['t'])
      end
    end

    def test_utc_timezone
      with_timezone("UTC-01") do
        # 'timezone' takes precedence over 'utc'.
        @d.configure(config_inject_section("time_key" => "t", "time_type" => "string", "timezone" => "Asia/Tokyo", "utc" => "true"))
        @d.start
        record = @d.inject_values_to_record('tag', @time, {"message" => "yay"})

        assert_equal("2014-09-27T09:00:00+09:00", record['t'])
      end
    end
  end
end
