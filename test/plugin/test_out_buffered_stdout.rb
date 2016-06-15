require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_buffered_stdout'

class BufferedStdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BufferedStdoutOutput).configure(conf)
  end

  test 'default configure' do
    d = create_driver
    assert_equal 'json', d.instance.output_type
    assert_equal 10 * 1024, d.instance.buffer_config.chunk_limit_size
    assert d.instance.buffer_config.flush_at_shutdown
    assert_equal ['tag'], d.instance.buffer_config.chunk_keys
    assert d.instance.chunk_key_tag
    assert !d.instance.chunk_key_time
    assert_equal [], d.instance.chunk_keys
  end

  test 'configure with output_type' do
    d = create_driver(CONFIG + "\noutput_type json")
    assert_equal 'json', d.instance.output_type

    d = create_driver(CONFIG + "\noutput_type hash")
    assert_equal 'hash', d.instance.output_type

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG + "\noutput_type foo")
    end
  end

  sub_test_case "emit json" do
    data('oj' => 'oj', 'yajl' => 'yajl')
    test '#write(synchronous)' do |data|
      d = create_driver(CONFIG + "\noutput_type json\njson_parser #{data}")
      time = event_time()

      out = capture_log do
        d.run(default_tag: 'test', flush: true) do
          d.feed(time, {'test' => 'test'})
        end
      end
      assert_equal "#{Time.at(time).localtime} test: {\"test\":\"test\"}\n", out
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    test '#try_write(asynchronous)' do |data|
      d = create_driver(CONFIG + "\noutput_type json\njson_parser #{data}")
      time = event_time()
      d.instance.delayed = true

      out = capture_log do
        d.run(default_tag: 'test', flush: true, shutdown: false) do
          d.feed(time, {'test' => 'test'})
        end
      end

      assert_equal "#{Time.at(time).localtime} test: {\"test\":\"test\"}\n", out
    end
  end

  sub_test_case 'emit hash' do
    test '#write(synchronous)' do
      d = create_driver(CONFIG + "\noutput_type hash")
      time = event_time()

      out = capture_log do
        d.run(default_tag: 'test', flush: true) do
          d.feed(time, {'test' => 'test'})
        end
      end

      assert_equal "#{Time.at(time).localtime} test: {\"test\"=>\"test\"}\n", out
    end

    test '#try_write(asynchronous)' do
      d = create_driver(CONFIG + "\noutput_type hash")
      time = event_time()
      d.instance.delayed = true

      out = capture_log do
        d.run(default_tag: 'test', flush: true, shutdown: false) do
          d.feed(time, {'test' => 'test'})
        end
      end

      assert_equal "#{Time.at(time).localtime} test: {\"test\"=>\"test\"}\n", out
    end
  end

  # Capture the log output of the block given
  def capture_log(&block)
    tmp = $log
    $log = StringIO.new
    yield
    return $log.string
  ensure
    $log = tmp
  end
end
