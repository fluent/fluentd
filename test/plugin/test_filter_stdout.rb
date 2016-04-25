require_relative '../helper'
require 'fluent/plugin/filter_stdout'
require 'timecop'
require 'flexmock/test_unit'

class StdoutFilterTest < Test::Unit::TestCase
  include Fluent
  include FlexMock::TestCase

  def setup
    Fluent::Test.setup
    Timecop.freeze
  end

  def teardown
    super # FlexMock::TestCase requires this
    # http://flexmock.rubyforge.org/FlexMock/TestCase.html
    Timecop.return
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Test::FilterTestDriver.new(StdoutFilter, 'filter.test').configure(conf)
  end

  def emit(d, msg, time)
    d.run {
      d.emit(msg, time)
    }.filtered_as_array[0][2]
  end

  def test_through_record
    d = create_driver
    time = Time.now
    filtered = emit(d, {'test' => 'test'}, Fluent::EventTime.from_time(time))
    assert_equal({'test' => 'test'}, filtered)
  end

  def test_configure_default
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
    out = capture_log(d) { emit(d, {'test' => 'test'}, Fluent::EventTime.from_time(time)) }
    assert_equal "#{time.localtime} filter.test: {\"test\":\"test\"}\n", out

    # NOTE: Float::NAN is not jsonable
    d = create_driver(CONFIG + "\noutput_type json")
    flexmock(d.instance.router).should_receive(:emit_error_event)
    emit(d, {'test' => Float::NAN}, time)
  end

  def test_output_type_hash
    d = create_driver(CONFIG + "\noutput_type hash")
    time = Time.now
    out = capture_log(d) { emit(d, {'test' => 'test'}, Fluent::EventTime.from_time(time)) }
    assert_equal "#{time.localtime} filter.test: {\"test\"=>\"test\"}\n", out

    # NOTE: Float::NAN is not jsonable, but hash string can output it.
    d = create_driver(CONFIG + "\noutput_type hash")
    out = capture_log(d) { emit(d, {'test' => Float::NAN}, Fluent::EventTime.from_time(time)) }
    assert_equal "#{time.localtime} filter.test: {\"test\"=>NaN}\n", out
  end

  # Use include_time_key to output the message's time
  def test_include_time_key
    d = create_driver(CONFIG + "\noutput_type json\ninclude_time_key true\nutc")
    time = Time.now
    message_time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")
    out = capture_log(d) { emit(d, {'test' => 'test'}, message_time) }
    assert_equal "#{time.localtime} filter.test: {\"test\":\"test\",\"time\":\"2011-01-02T13:14:15Z\"}\n", out
  end

  # out_stdout formatter itself can also be replaced
  def test_format_json
    d = create_driver(CONFIG + "\nformat json")
    time = Time.now
    out = capture_log(d) { emit(d, {'test' => 'test'}, Fluent::EventTime.from_time(time)) }
    assert_equal "{\"test\":\"test\"}\n", out
  end

  private

  # Capture the log output of the block given
  def capture_log(d, &block)
    tmp = d.instance.log.out
    d.instance.log.out = StringIO.new
    yield
    return d.instance.log.out.string
  ensure
    d.instance.log.out = tmp
  end
end

