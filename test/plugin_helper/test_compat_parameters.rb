require_relative '../helper'
require 'fluent/plugin_helper/compat_parameters'
require 'fluent/plugin/base'

class CompatParameterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @i = nil
  end

  teardown do
    if @i
      @i.stop unless @i.stopped?
      @i.before_shutdown unless @i.before_shutdown?
      @i.shutdown unless @i.shutdown?
      @i.after_shutdown unless @i.after_shutdown?
      @i.close unless @i.closed?
      @i.terminate unless @i.terminated?
    end
  end

  class Dummy0 < Fluent::Plugin::Output
    helpers :compat_parameters
    def compat_parameters_default_chunk_key
      ''
    end
    def write(chunk)
      # dummy
    end
  end
  class Dummy1 < Fluent::Plugin::Output
    helpers :compat_parameters
    def compat_parameters_default_chunk_key
      'time'
    end
    def write(chunk)
      # dummy
    end
  end
  class Dummy2 < Fluent::Plugin::Output
    helpers :compat_parameters
    # for test to assume default key time by 'time_slice_format'
    def write(chunk)
      # dummy
    end
  end
  class Dummy3 < Fluent::Plugin::Output
    helpers :compat_parameters
    def compat_parameters_default_chunk_key
      'tag'
    end
    def write(chunk)
      # dummy
    end
  end

  sub_test_case 'plugins which does not have default chunk key' do
    setup do
      @p = Dummy0
    end

    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'num_threads' => 8,
        'flush_interval' => '10s',
        'buffer_chunk_limit' => '8m',
        'buffer_queue_limit' => '1024',
        'flush_at_shutdown' => 'yes',
      }
      conf = config_element('ROOT', '', hash)
      @i = @p.new
      @i.configure(conf)

      assert_equal 'memory', @i.buffer_config[:@type]
      assert_equal [], @i.buffer_config.chunk_keys
      assert_equal 8, @i.buffer_config.flush_thread_count
      assert_equal 10, @i.buffer_config.flush_interval
      assert @i.buffer_config.flush_at_shutdown

      assert_equal 8*1024*1024, @i.buffer.chunk_limit_size
      assert_equal 1024, @i.buffer.queue_length_limit
    end
  end

  sub_test_case 'plugins which has default chunk key: time' do
    setup do
      @p = Dummy1
    end

    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'buffer_type' => 'file',
        'buffer_path' => '/tmp/mybuffer',
        'disable_retry_limit' => 'yes',
        'max_retry_wait' => '1h',
        'buffer_queue_full_action' => 'block',
      }
      conf = config_element('ROOT', '', hash)
      @i = @p.new
      @i.configure(conf)

      assert_equal 'file', @i.buffer_config[:@type]
      assert_equal 24*60*60, @i.buffer_config.timekey
      assert @i.buffer_config.retry_forever
      assert_equal 60*60, @i.buffer_config.retry_max_interval
      assert_equal :block, @i.buffer_config.overflow_action

      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys

      assert_equal '/tmp/mybuffer/buffer.*.log', @i.buffer.path
    end
  end

  sub_test_case 'plugins which does not have default chunk key' do
    setup do
      @p = Dummy2
    end

    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'buffer_type' => 'file',
        'buffer_path' => '/tmp/mybuffer',
        'time_slice_format' => '%Y%m%d%H',
        'time_slice_wait' => '10',
        'retry_limit' => '1024',
        'buffer_queue_full_action' => 'drop_oldest_chunk',
      }
      conf = config_element('ROOT', '', hash)
      @i = @p.new
      @i.configure(conf)

      assert_equal 'file', @i.buffer_config[:@type]
      assert_equal 60*60, @i.buffer_config.timekey
      assert_equal 10, @i.buffer_config.timekey_wait
      assert_equal 1024, @i.buffer_config.retry_max_times
      assert_equal :drop_oldest_chunk, @i.buffer_config.overflow_action

      assert @i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys

      assert_equal '/tmp/mybuffer/buffer.*.log', @i.buffer.path
    end
  end

  sub_test_case 'plugins which has default chunk key: tag' do
    setup do
      @p = Dummy3
    end

    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'buffer_type' => 'memory',
        'num_threads' => '10',
        'flush_interval' => '10s',
        'try_flush_interval' => '0.1',
        'queued_chunk_flush_interval' => '0.5',
      }
      conf = config_element('ROOT', '', hash)
      @i = @p.new
      @i.configure(conf)

      assert_equal 'memory', @i.buffer_config[:@type]
      assert_equal 10, @i.buffer_config.flush_thread_count
      assert_equal 10, @i.buffer_config.flush_interval
      assert_equal 0.1, @i.buffer_config.flush_thread_interval
      assert_equal 0.5, @i.buffer_config.flush_thread_burst_interval

      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys
    end
  end
end
