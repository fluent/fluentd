require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_stdout'

class StdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]
  TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%9N %z'

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::StdoutOutput).configure(conf)
  end

  sub_test_case 'non-buffered' do
    test 'configure' do
      d = create_driver
      assert_equal 1, d.instance.formatter_configs.size # init: true
      assert_kind_of Fluent::Plugin::StdoutFormatter, d.instance.formatter
      assert_equal 'json', d.instance.formatter.output_type
    end

    test 'configure output_type' do
      d = create_driver(CONFIG + "\noutput_type json")
      assert_kind_of Fluent::Plugin::StdoutFormatter, d.instance.formatter
      assert_equal 'json', d.instance.formatter.output_type

      d = create_driver(CONFIG + "\noutput_type hash")
      assert_kind_of Fluent::Plugin::StdoutFormatter, d.instance.formatter
      assert_equal 'hash', d.instance.formatter.output_type

      assert_raise(Fluent::ConfigError) do
        d = create_driver(CONFIG + "\noutput_type foo")
      end
    end

    test 'emit with default configuration' do
      d = create_driver
      time = event_time()
      out = capture_log do
        d.run(default_tag: 'test') do
          d.feed(time, {'test' => 'test1'})
        end
      end
      assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\":\"test1\"}\n", out
    end

    data('oj' => 'oj', 'yajl' => 'yajl')
    test 'emit in json format' do |data|
      d = create_driver(CONFIG + "\noutput_type json\njson_parser #{data}")
      time = event_time()
      out = capture_log do
        d.run(default_tag: 'test') do
          d.feed(time, {'test' => 'test1'})
        end
      end
      assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\":\"test1\"}\n", out

      if data == 'yajl'
        # NOTE: Float::NAN is not jsonable
        assert_raise(Yajl::EncodeError) { d.feed('test', time, {'test' => Float::NAN}) }
      else
        out = capture_log { d.feed('test', time, {'test' => Float::NAN}) }
        assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\":NaN}\n", out
      end
    end

    test 'emit in hash format' do
      d = create_driver(CONFIG + "\noutput_type hash")
      time = event_time()
      out = capture_log do
        d.run(default_tag: 'test') do
          d.feed(time, {'test' => 'test2'})
        end
      end
      assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\"=>\"test2\"}\n", out

      # NOTE: Float::NAN is not jsonable, but hash string can output it.
      out = capture_log { d.feed('test', time, {'test' => Float::NAN}) }
      assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\"=>NaN}\n", out
    end
  end

  sub_test_case 'buffered' do
    test 'configure' do
      d = create_driver(config_element("ROOT", "", {}, [config_element("buffer")]))
      assert_equal 1, d.instance.formatter_configs.size
      assert_kind_of Fluent::Plugin::StdoutFormatter, d.instance.formatter
      assert_equal 'json', d.instance.formatter.output_type
      assert_equal 10 * 1024, d.instance.buffer_config.chunk_limit_size
      assert d.instance.buffer_config.flush_at_shutdown
      assert_equal ['tag'], d.instance.buffer_config.chunk_keys
      assert d.instance.chunk_key_tag
      assert !d.instance.chunk_key_time
      assert_equal [], d.instance.chunk_keys
    end

    test 'configure with output_type' do
      d = create_driver(config_element("ROOT", "", {"output_type" => "json"}, [config_element("buffer")]))
      assert_kind_of Fluent::Plugin::StdoutFormatter, d.instance.formatter
      assert_equal 'json', d.instance.formatter.output_type

      d = create_driver(config_element("ROOT", "", {"output_type" => "hash"}, [config_element("buffer")]))
      assert_kind_of Fluent::Plugin::StdoutFormatter, d.instance.formatter
      assert_equal 'hash', d.instance.formatter.output_type

      assert_raise(Fluent::ConfigError) do
        create_driver(config_element("ROOT", "", {"output_type" => "foo"}, [config_element("buffer")]))
      end
    end

    sub_test_case "emit with default config" do
      test '#write(synchronous)' do
        d = create_driver(config_element("ROOT", "", {}, [config_element("buffer")]))
        time = event_time()

        out = capture_log do
          d.run(default_tag: 'test', flush: true) do
            d.feed(time, {'test' => 'test'})
          end
        end
        assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\":\"test\"}\n", out
      end
    end

    sub_test_case "emit json" do
      data('oj' => 'oj', 'yajl' => 'yajl')
      test '#write(synchronous)' do |data|
        d = create_driver(config_element("ROOT", "", {"output_type" => "json", "json_parser" => data}, [config_element("buffer")]))
        time = event_time()

        out = capture_log do
          d.run(default_tag: 'test', flush: true) do
            d.feed(time, {'test' => 'test'})
          end
        end
        assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\":\"test\"}\n", out
      end
    end

    sub_test_case 'emit hash' do
      test '#write(synchronous)' do
        d = create_driver(config_element("ROOT", "", {"output_type" => "hash"}, [config_element("buffer")]))
        time = event_time()

        out = capture_log do
          d.run(default_tag: 'test', flush: true) do
            d.feed(time, {'test' => 'test'})
          end
        end

        assert_equal "#{Time.at(time).localtime.strftime(TIME_FORMAT)} test: {\"test\"=>\"test\"}\n", out
      end
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

