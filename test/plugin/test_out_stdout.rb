require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_stdout'

class StdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::StdoutOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'json', d.instance.output_type
  end

  def test_configure_output_type
    d = create_driver(CONFIG + "\noutput_type json")
    assert_equal 'json', d.instance.output_type

    d = create_driver(CONFIG + "\noutput_type hash")
    assert_equal 'hash', d.instance.output_type

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG + "\noutput_type foo")
    end
  end

  data('oj' => 'oj', 'yajl' => 'yajl')
  def test_emit_json(data)
    d = create_driver(CONFIG + "\noutput_type json\njson_parser #{data}")
    time = event_time()
    out = capture_log do
      d.run(default_tag: 'test') do
        d.feed(time, {'test' => 'test1'})
      end
    end
    assert_equal "#{Time.at(time).localtime} test: {\"test\":\"test1\"}\n", out

    if data == 'yajl'
      # NOTE: Float::NAN is not jsonable
      assert_raise(Yajl::EncodeError) { d.feed('test', time, {'test' => Float::NAN}) }
    else
      out = capture_log { d.feed('test', time, {'test' => Float::NAN}) }
      assert_equal "#{Time.at(time).localtime} test: {\"test\":NaN}\n", out
    end
  end

  def test_emit_hash
    d = create_driver(CONFIG + "\noutput_type hash")
    time = event_time()
    out = capture_log do
      d.run(default_tag: 'test') do
        d.feed(time, {'test' => 'test2'})
      end
    end
    assert_equal "#{Time.at(time).localtime} test: {\"test\"=>\"test2\"}\n", out

    # NOTE: Float::NAN is not jsonable, but hash string can output it.
    out = capture_log { d.feed('test', time, {'test' => Float::NAN}) }
    assert_equal "#{Time.at(time).localtime} test: {\"test\"=>NaN}\n", out
  end

  private

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

