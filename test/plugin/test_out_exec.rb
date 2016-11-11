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

  def create_driver(config)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::ExecOutput).configure(config)
  end

  def create_test_data
    time = event_time("2011-01-02 13:14:15.123")
    records = [{"k1"=>"v1","kx"=>"vx"}, {"k1"=>"v2","kx"=>"vx"}]
    return time, records
  end

  DEFAULT_CONFIG_ONLY_WITH_KEYS = %[
    command cat >#{TMP_DIR}/out
    <format>
      keys ["k1", "kx"]
    </format>
  ]

  test 'configure in default' do
    d = create_driver DEFAULT_CONFIG_ONLY_WITH_KEYS
    assert{ d.instance.formatter.is_a? Fluent::Plugin::TSVFormatter }
    assert_equal ["k1", "kx"], d.instance.formatter.keys
    assert_nil d.instance.inject_config
  end

  TSV_CONFIG = %[
    command cat >#{TMP_DIR}/out
    <inject>
      tag_key tag
      time_key time
      time_format %Y-%m-%d %H:%M:%S
      localtime yes
    </inject>
    <format>
      @type tsv
      keys time, tag, k1
    </format>
  ]
  TSV_CONFIG_WITH_SUBSEC = %[
    command cat >#{TMP_DIR}/out
    <inject>
      tag_key tag
      time_key time
      time_format %Y-%m-%d %H:%M:%S.%3N
      localtime yes
    </inject>
    <format>
      @type tsv
      keys time, tag, k1
    </format>
  ]
  TSV_CONFIG_WITH_BUFFER = TSV_CONFIG + %[
    <buffer time>
      @type memory
      timekey 3600
      flush_thread_count 5
      chunk_limit_size 50m
      total_limit_size #{50 * 1024 * 1024 * 128}
      flush_at_shutdown yes
    </buffer>
  ]
  JSON_CONFIG = %[
    command cat >#{TMP_DIR}/out
    <format>
      @type json
    </format>
  ]
  MSGPACK_CONFIG = %[
    command cat >#{TMP_DIR}/out
    <format>
      @type msgpack
    </format>
  ]

  CONFIG_COMPAT = %[
    buffer_path #{TMP_DIR}/buffer
    command cat >#{TMP_DIR}/out
    localtime
  ]
  TSV_CONFIG_COMPAT = %[
    keys "time,tag,k1"
    tag_key "tag"
    time_key "time"
    time_format %Y-%m-%d %H:%M:%S
  ]
  BUFFER_CONFIG_COMPAT = %[
    buffer_type memory
    time_slice_format %Y%m%d%H
    num_threads 5
    buffer_chunk_limit 50m
    buffer_queue_limit 128
    flush_at_shutdown yes
  ]
  TSV_CONFIG_WITH_SUBSEC_COMPAT = %[
    keys "time,tag,k1"
    tag_key "tag"
    time_key "time"
    time_format %Y-%m-%d %H:%M:%S.%3N
  ]

  data(
    'with sections' => TSV_CONFIG,
    'traditional' => CONFIG_COMPAT + TSV_CONFIG_COMPAT,
  )
  test 'configure for tsv' do |conf|
    d = create_driver(conf)

    assert_equal ["time","tag","k1"], d.instance.formatter.keys
    assert_equal "tag", d.instance.inject_config.tag_key
    assert_equal "time", d.instance.inject_config.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.inject_config.time_format
    assert_equal true, d.instance.inject_config.localtime
  end

  data(
    'with sections' => TSV_CONFIG_WITH_BUFFER,
    'traditional' => CONFIG_COMPAT + TSV_CONFIG_COMPAT + BUFFER_CONFIG_COMPAT,
  )
  test 'configure_with_compat_buffer_parameters' do |conf|
    d = create_driver(conf)
    assert_equal 3600, d.instance.buffer_config.timekey
    assert_equal 5, d.instance.buffer_config.flush_thread_count
    assert_equal 50*1024*1024, d.instance.buffer.chunk_limit_size
    assert_equal 50*1024*1024*128, d.instance.buffer.total_limit_size
    assert d.instance.buffer_config.flush_at_shutdown
  end

  data(
    'with sections' => TSV_CONFIG,
    'traditional' => CONFIG_COMPAT + TSV_CONFIG_COMPAT,
  )
  test 'format' do |conf|
    d = create_driver(conf)
    time, records = create_test_data

    d.run(default_tag: 'test') do
      d.feed(time, records[0])
      d.feed(time, records[1])
    end

    assert_equal %[2011-01-02 13:14:15\ttest\tv1\n], d.formatted[0]
    assert_equal %[2011-01-02 13:14:15\ttest\tv2\n], d.formatted[1]
  end

  data(
    'with sections' => JSON_CONFIG,
    'traditional' => CONFIG_COMPAT + "format json",
  )
  test 'format_json' do |conf|
    d = create_driver(conf)
    time, records = create_test_data

    d.run(default_tag: 'test') do
      d.feed(time, records[0])
      d.feed(time, records[1])
    end

    assert_equal Yajl.dump(records[0]) + "\n", d.formatted[0]
    assert_equal Yajl.dump(records[1]) + "\n", d.formatted[1]
  end

  data(
    'with sections' => MSGPACK_CONFIG,
    'traditional' => CONFIG_COMPAT + "format msgpack"
  )
  test 'format_msgpack' do |conf|
    d = create_driver(conf)
    time, records = create_test_data

    d.run(default_tag: 'test') do
      d.feed(time, records[0])
      d.feed(time, records[1])
    end

    assert_equal records[0].to_msgpack, d.formatted[0]
    assert_equal records[1].to_msgpack, d.formatted[1]
  end

  data(
    'with sections' => TSV_CONFIG_WITH_SUBSEC,
    'traditional' => CONFIG_COMPAT + TSV_CONFIG_WITH_SUBSEC_COMPAT,
  )
  test 'format subsecond time' do |conf|
    d = create_driver(conf)
    time, records = create_test_data

    d.run(default_tag: 'test') do
      d.feed(time, records[0])
      d.feed(time, records[1])
    end

    assert_equal %[2011-01-02 13:14:15.123\ttest\tv1\n], d.formatted[0]
    assert_equal %[2011-01-02 13:14:15.123\ttest\tv2\n], d.formatted[1]
  end

  data(
    'with sections' => TSV_CONFIG,
    'traditional' => CONFIG_COMPAT + TSV_CONFIG_COMPAT,
  )
  test 'write' do |conf|
    d = create_driver(conf)
    time, records = create_test_data

    d.run(default_tag: 'test', flush: true) do
      d.feed(time, records[0])
      d.feed(time, records[1])
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

  sub_test_case 'when executed process dies unexpectedly' do
    setup do
      @gen_config = ->(num){ <<EOC
    command ruby -e "ARGV.first.to_i == 0 ? open(ARGV[1]){|f| STDOUT.write f.read} : (sleep 1 ; exit ARGV.first.to_i)" #{num} >#{TMP_DIR}/fail_out
    <inject>
      tag_key tag
      time_key time
      time_format %Y-%m-%d %H:%M:%S
      localtime yes
    </inject>
    <format>
      @type tsv
      keys time, tag, k1
    </format>
EOC
      }
    end

    test 'flushed chunk will be committed after child process successfully exits' do
      d = create_driver(@gen_config.call(0))
      time, records = create_test_data

      expect_path = "#{TMP_DIR}/fail_out"

      d.end_if{ File.exist?(expect_path) }
      d.run(default_tag: 'test', flush: true, wait_flush_completion: false, shutdown: false) do
        d.feed(time, records[0])
        d.feed(time, records[1])
      end

      assert{ File.exist?(expect_path) }

      data = File.read(expect_path)
      expect_data =
        %[2011-01-02 13:14:15\ttest\tv1\n] +
        %[2011-01-02 13:14:15\ttest\tv2\n]
      assert_equal expect_data, data

      assert{ d.instance.buffer.queue.empty? }
      assert{ d.instance.dequeued_chunks.empty? }

      d.instance_shutdown
    end

    test 'flushed chunk will be taken back after child process unexpectedly exits' do
      d = create_driver(@gen_config.call(3))
      time, records = create_test_data

      expect_path = "#{TMP_DIR}/fail_out"

      d.end_if{ d.instance.log.out.logs.any?{|line| line.include?("command exits with error code") } }
      d.run(default_tag: 'test', flush: true, wait_flush_completion: false, shutdown: false) do
        d.feed(time, records[0])
        d.feed(time, records[1])
      end

      assert{ d.instance.dequeued_chunks.empty? } # because it's already taken back
      assert{ d.instance.buffer.queue.size == 1 }

      logs = d.instance.log.out.logs
      assert{ logs.any?{|line| line.include?("command exits with error code") && line.include?("status=3") } }

      assert{ File.exist?(expect_path) && File.size(expect_path) == 0 }

      d.instance_shutdown
    end
  end
end
