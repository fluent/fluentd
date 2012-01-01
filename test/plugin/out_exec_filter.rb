require 'fluent/test'
require 'fileutils'

class ExecFilterOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    command cat
    in_keys time,tag,k1
    out_keys time,tag,k2
    tag_key tag
    time_key time
    time_format %Y-%m-%d %H:%M:%S
    localtime
  ]

  def create_driver(conf = CONFIG, tag = 'test')
    Fluent::Test::OutputTestDriver.new(Fluent::ExecFilterOutput, tag).configure(conf)
  end

  def test_configure
    d = create_driver

    assert_equal ["time","tag","k1"], d.instance.in_keys
    assert_equal ["time","tag","k2"], d.instance.out_keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
    assert_equal true, d.instance.localtime

    d = create_driver %[
      command sed -l -e s/foo/bar/
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
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

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.emit({"k1"=>1}, time)
      d.emit({"k1"=>2}, time)
      sleep 0.5
    end

    emits = d.emits
    assert_equal 2, emits.length
    assert_equal ["test", time, {"k2"=>"1"}], emits[0]
    assert_equal ["test", time, {"k2"=>"2"}], emits[1]
  end

  def test_emit_2
    d = create_driver %[
      command cat
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
    ]

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.emit({"k1"=>1}, time)
      d.emit({"k1"=>2}, time)
      sleep 0.5
    end

    emits = d.emits
    assert_equal 2, emits.length
    assert_equal ["xxx", time, {"k2"=>"1"}], emits[0]
    assert_equal ["xxx", time, {"k2"=>"2"}], emits[1]
  end

  def test_emit_3
    d = create_driver %[
      command grep --line-buffered -v poo
      in_keys time,val1
      out_keys time,val2
      tag xxx
      time_key time
    ]

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.emit({"val1"=>"sed-ed value foo"}, time)
      d.emit({"val1"=>"sed-ed value poo"}, time)
      sleep 0.5
    end

    emits = d.emits
    assert_equal 1, emits.length
    assert_equal ["xxx", time, {"val2"=>"sed-ed value foo"}], emits[0]

    d = create_driver %[
      command sed -l -e s/foo/bar/
      in_keys time,val1
      out_keys time,val2
      tag xxx
      time_key time
    ]

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.emit({"val1"=>"sed-ed value foo"}, time)
      d.emit({"val1"=>"sed-ed value poo"}, time)
      sleep 0.5
    end

    emits = d.emits
    assert_equal 2, emits.length
    assert_equal ["xxx", time, {"val2"=>"sed-ed value bar"}], emits[0]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value poo"}], emits[1]
  end

  def test_emit_4
    d = create_driver(%[
      command sed -l -e s/foo/bar/
      in_keys tag,time,val1
      remove_prefix input
      out_keys tag,time,val2
      add_prefix output
      tag_key tag
      time_key time
    ], 'input.test')

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      d.emit({"val1"=>"sed-ed value foo"}, time)
      d.emit({"val1"=>"sed-ed value poo"}, time)
      sleep 0.5
    end

    emits = d.emits
    assert_equal 2, emits.length
    assert_equal ["output.test", time, {"val2"=>"sed-ed value bar"}], emits[0]
    assert_equal ["output.test", time, {"val2"=>"sed-ed value poo"}], emits[1]
  end
end

