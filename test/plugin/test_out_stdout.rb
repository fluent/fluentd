require_relative '../helper'
require 'fluent/test'
require 'timecop'

class StdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    Timecop.freeze
  end

  def teardown
    Timecop.return
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::StdoutOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'json', d.instance.formatter.output_type
  end

  def test_configure_output_type
    d = create_driver(CONFIG + "\noutput_type json")
    assert_equal 'json', d.instance.formatter.output_type

    d = create_driver(CONFIG + "\noutput_type hash")
    assert_equal 'hash', d.instance.formatter.output_type

    d = create_driver(CONFIG + "\noutput_type ltsv")
    assert_equal 'ltsv', d.instance.formatter.output_type

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG + "\noutput_type foo")
    end
  end

  def test_output_type_json
    d = create_driver(CONFIG + "\noutput_type json")
    time = Time.now
    out = capture_log { d.emit({'test' => 'test'}, time) }
    assert_equal "#{time.localtime} test: {\"test\":\"test\"}\n", out

    # NOTE: Float::NAN is not jsonable
    assert_raise(Yajl::EncodeError) { d.emit({'test' => Float::NAN}, time) }
  end

  def test_output_type_hash
    d = create_driver(CONFIG + "\noutput_type hash")
    time = Time.now
    out = capture_log { d.emit({'test' => 'test'}, time) }
    assert_equal "#{time.localtime} test: {\"test\"=>\"test\"}\n", out

    # NOTE: Float::NAN is not jsonable, but hash string can output it.
    out = capture_log { d.emit({'test' => Float::NAN}, time) }
    assert_equal "#{time.localtime} test: {\"test\"=>NaN}\n", out
  end

  # Use include_time_key to output the message's time
  def test_include_time_key
    d = create_driver(CONFIG + "\noutput_type json\ninclude_time_key true\nutc")
    time = Time.now
    message_time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    out = capture_log { d.emit({'test' => 'test'}, message_time) }
    assert_equal "#{time.localtime} test: {\"test\":\"test\",\"time\":\"2011-01-02T13:14:15Z\"}\n", out
  end

  # out_stdout formatter itself can also be replaced
  def test_format_json
    d = create_driver(CONFIG + "\nformat json")
    time = Time.now
    out = capture_log { d.emit({'test' => 'test'}, time) }
    assert_equal "{\"test\":\"test\"}\n", out
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

