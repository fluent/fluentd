require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_exec_filter'
require 'fileutils'

class ExecFilterOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    command cat
    in_keys time_in,tag,k1
    out_keys time_out,tag,k2
    tag_key tag
    in_time_key time_in
    out_time_key time_out
    time_format %Y-%m-%d %H:%M:%S
    localtime
    num_children 3
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::ExecFilterOutput).configure(conf)
  end

  def sed_unbuffered_support?
    @sed_unbuffered_support ||= lambda {
      system("echo xxx | sed --unbuffered -l -e 's/x/y/g' >#{IO::NULL} 2>&1")
      $?.success?
    }.call
  end

  def sed_unbuffered_option
    sed_unbuffered_support? ? '--unbuffered' : ''
  end

  def test_configure
    d = create_driver

    assert_false d.instance.parser.estimate_current_event

    assert_equal ["time_in","tag","k1"], d.instance.formatter.keys
    assert_equal ["time_out","tag","k2"], d.instance.parser.keys
    assert_equal "tag", d.instance.inject_config.tag_key
    assert_equal "tag", d.instance.extract_config.tag_key
    assert_equal "time_in", d.instance.inject_config.time_key
    assert_equal "time_out", d.instance.extract_config.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.inject_config.time_format
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.extract_config.time_format
    assert_equal true, d.instance.inject_config.localtime
    assert_equal 3, d.instance.num_children

    d = create_driver %[
      command sed -l -e s/foo/bar/
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
      num_children 3
    ]
    assert_equal "sed -l -e s/foo/bar/", d.instance.command

    d = create_driver(CONFIG + %[
      remove_prefix before
      add_prefix after
    ])
    assert_equal "before", d.instance.remove_prefix
    assert_equal "after" , d.instance.add_prefix
  end

  def test_emit_1
    d = create_driver

    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'test', expect_emits: 2, timeout: 10) do
      # sleep 0.1 until d.instance.children && !d.instance.children.empty? && d.instance.children.all?{|c| c.finished == false }
      d.feed(time, {"k1"=>1})
      d.feed(time, {"k1"=>2})
    end

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["test", time, {"k2"=>"1"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["test", time, {"k2"=>"2"}], events[1]
  end

  def test_emit_2
    d = create_driver %[
      command cat
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
      num_children 3
    ]

    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'test', expect_emits: 2, timeout: 10) do
      d.feed(time, {"k1"=>1})
      d.feed(time, {"k1"=>2})
    end

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["xxx", time, {"k2"=>"1"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["xxx", time, {"k2"=>"2"}], events[1]
  end

  def test_emit_3
    d = create_driver %[
      command grep --line-buffered -v poo
      in_keys time,val1
      out_keys time,val2
      tag xxx
      time_key time
      num_children 3
    ]

    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'test', expect_emits: 1, timeout: 10) do
      d.feed(time, {"val1"=>"sed-ed value poo"})
      d.feed(time, {"val1"=>"sed-ed value foo"})
    end

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value foo"}], events[0]

    d = create_driver %[
      command sed #{sed_unbuffered_option} -l -e s/foo/bar/
      in_keys time,val1
      out_keys time,val2
      tag xxx
      time_key time
      num_children 3
    ]

    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'test', expect_emits: 1, timeout: 10) do
      d.feed(time, {"val1"=>"sed-ed value poo"})
      d.feed(time, {"val1"=>"sed-ed value foo"})
    end

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value poo"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value bar"}], events[1]
  end

  def test_emit_4
    d = create_driver(%[
      command sed #{sed_unbuffered_option} -l -e s/foo/bar/
      in_keys tag,time,val1
      remove_prefix input
      out_keys tag,time,val2
      add_prefix output
      tag_key tag
      time_key time
      num_children 3
    ])

    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'input.test', expect_emits: 2, timeout: 10) do
      d.feed(time, {"val1"=>"sed-ed value foo"})
      d.feed(time, {"val1"=>"sed-ed value poo"})
    end

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["output.test", time, {"val2"=>"sed-ed value bar"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["output.test", time, {"val2"=>"sed-ed value poo"}], events[1]
  end

  def test_json_1
    d = create_driver(%[
      command cat
      in_keys message
      out_format json
      time_key time
      tag_key tag
    ])

    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'input.test', expect_emits: 1, timeout: 10) do
      d.feed(time, {"message"=>%[{"time":#{time},"tag":"t1","k1":"v1"}]})
    end

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["t1", time, {"k1"=>"v1"}], events[0]
  end

  def test_json_with_float_time
    d = create_driver(%[
      command cat
      in_keys message
      out_format json
      time_key time
      tag_key tag
    ])

    time = event_time("2011-01-02 13:14:15.123")

    d.run(default_tag: 'input.test', expect_emits: 1, timeout: 10) do
      d.feed(time + 10, {"message"=>%[{"time":#{time.sec}.#{time.nsec},"tag":"t1","k1":"v1"}]})
    end

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["t1", time, {"k1"=>"v1"}], events[0]
  end

  def test_json_with_time_format
    d = create_driver(%[
      command cat
      in_keys message
      out_format json
      time_key time
      time_format %d/%b/%Y %H:%M:%S.%N %z
      tag_key tag
    ])

    time_str = "28/Feb/2013 12:00:00.123456789 +0900"
    time = event_time(time_str, format: "%d/%b/%Y %H:%M:%S.%N %z")

    d.run(default_tag: 'input.test', expect_emits: 1, timeout: 10) do
      d.feed(time + 10, {"message"=>%[{"time":"#{time_str}","tag":"t1","k1":"v1"}]})
    end

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["t1", time, {"k1"=>"v1"}], events[0]
  end
end

