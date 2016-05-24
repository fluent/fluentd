require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_exec'
require 'fileutils'

class ExecOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR, secure: true)
    if File.exist?(TMP_DIR)
      # ensure files are closed for Windows, on which deleted files
      # are still visible from filesystem
      GC.start(full_mark: true, immediate_sweep: true)
      FileUtils.remove_entry_secure(TMP_DIR)
    end
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/out_exec#{ENV['TEST_ENV_NUMBER']}"

  CONFIG = %[
    buffer_path #{TMP_DIR}/buffer
    command cat >#{TMP_DIR}/out
    localtime
  ]
  TSV_CONFIG = %[
    keys "time,tag,k1"
    tag_key "tag"
    time_key "time"
    time_format %Y-%m-%d %H:%M:%S
  ]

  def create_driver(conf = TSV_CONFIG)
    config = CONFIG + conf
    Fluent::Test::Driver::Output.new(Fluent::Plugin::ExecOutput).configure(config)
  end

  def create_test_case
    time = Time.parse("2011-01-02 13:14:15").to_i
    tests = [{"k1"=>"v1","kx"=>"vx"}, {"k1"=>"v2","kx"=>"vx"}]
    return time, tests
  end

  def test_configure
    d = create_driver

    assert_equal ["time","tag","k1"], d.instance.keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
    assert_equal true, d.instance.localtime
  end

  def test_configure_with_compat_buffer_parameters
    conf = TSV_CONFIG + %[
      buffer_type memory
      time_slice_format %Y%m%d%H
      num_threads 5
      buffer_chunk_limit 50m
      buffer_queue_limit 128
      flush_at_shutdown yes
    ]
    d = create_driver(conf)
    assert_equal 3600, d.instance.buffer_config.timekey
    assert_equal 5, d.instance.buffer_config.flush_thread_count
    assert_equal 50*1024*1024, d.instance.buffer.chunk_limit_size
    assert_equal 128, d.instance.buffer.queue_length_limit
    assert d.instance.buffer_config.flush_at_shutdown
  end

  def test_format
    d = create_driver
    time, tests = create_test_case

    d.run(default_tag: 'test') do
      d.feed(time, tests[0])
      d.feed(time, tests[1])
    end

    assert_equal %[2011-01-02 13:14:15\ttest\tv1\n], d.formatted[0]
    assert_equal %[2011-01-02 13:14:15\ttest\tv2\n], d.formatted[1]
  end

  def test_format_json
    d = create_driver("format json")
    time, tests = create_test_case

    d.run(default_tag: 'test') do
      d.feed(time, tests[0])
      d.feed(time, tests[1])
    end

    assert_equal Yajl.dump(tests[0]) + "\n", d.formatted[0]
    assert_equal Yajl.dump(tests[1]) + "\n", d.formatted[1]
  end

  def test_format_msgpack
    d = create_driver("format msgpack")
    time, tests = create_test_case

    d.run(default_tag: 'test') do
      d.feed(time, tests[0])
      d.feed(time, tests[1])
    end

    assert_equal tests[0].to_msgpack, d.formatted[0]
    assert_equal tests[1].to_msgpack, d.formatted[1]
  end

  def test_format_time
    config = %[
      keys "time,tag,k1"
      tag_key "tag"
      time_key "time"
      time_format %Y-%m-%d %H:%M:%S.%3N
    ]
    d = create_driver(config)

    time = event_time("2011-01-02 13:14:15.123")
    tests = [{"k1"=>"v1","kx"=>"vx"}, {"k1"=>"v2","kx"=>"vx"}]

    d.run(default_tag: 'test') do
      d.feed(time, tests[0])
      d.feed(time, tests[1])
    end

    assert_equal %[2011-01-02 13:14:15.123\ttest\tv1\n], d.formatted[0]
    assert_equal %[2011-01-02 13:14:15.123\ttest\tv2\n], d.formatted[1]
  end

  def test_write
    d = create_driver
    time, tests = create_test_case

    d.run(default_tag: 'test', flush: true) do
      d.feed(time, tests[0])
      d.feed(time, tests[1])
    end

    expect_path = "#{TMP_DIR}/out"

    waiting(10, plugin: d.instance) do
      sleep(0.1) until File.exist?(expect_path)
    end

    assert_equal true, File.exist?(expect_path)

    data = File.read(expect_path)
    expect_data =
      %[2011-01-02 13:14:15\ttest\tv1\n] +
      %[2011-01-02 13:14:15\ttest\tv2\n]
    assert_equal expect_data, data
  end
end

