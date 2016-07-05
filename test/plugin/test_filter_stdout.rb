require_relative '../helper'
require 'fluent/test/driver/filter'
require 'fluent/plugin/filter_stdout'
require 'timecop'
require 'flexmock/test_unit'

class StdoutFilterTest < Test::Unit::TestCase
  include FlexMock::TestCase

  def setup
    Fluent::Test.setup
    @old_tz = ENV["TZ"]
    ENV["TZ"] = "UTC"
    Timecop.freeze
  end

  def teardown
    super # FlexMock::TestCase requires this
    # http://flexmock.rubyforge.org/FlexMock/TestCase.html
    Timecop.return
    ENV["TZ"] = @old_tz
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::StdoutFilter).configure(conf)
  end

  def filter(d, time, record)
    d.run {
      d.feed("filter.test", time, record)
    }
    d.filtered_records
  end

  def test_through_record
    d = create_driver
    filtered = filter(d, event_time, {'test' => 'test'})
    assert_equal([{'test' => 'test'}], filtered)
  end

  sub_test_case "flat style parameters" do
    sub_test_case "configure" do
      def test_configure_default
        d = create_driver
        d.run {}
        assert_equal 'json', d.instance.formatter.output_type
      end

      data(json: "json",
           hash: "hash",
           ltsv: "ltsv")
      def test_output_type(data)
        d = create_driver(CONFIG + "\noutput_type #{data}")
        d.run {}
        assert_equal data, d.instance.formatter.output_type
      end

      def test_invalid_output_type
        assert_raise(Fluent::ConfigError) do
          d = create_driver(CONFIG + "\noutput_type foo")
          d.run {}
        end
      end
    end

    def test_output_type_json
      d = create_driver(CONFIG + "\noutput_type json")
      etime = event_time
      time = Time.at(etime.sec)
      out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
      assert_equal "#{time.localtime} filter.test: {\"test\":\"test\"}\n", out

      # NOTE: Float::NAN is not jsonable
      d = create_driver(CONFIG + "\noutput_type json")
      flexmock(d.instance.router).should_receive(:emit_error_event)
      filter(d, etime, {'test' => Float::NAN})
    end

    def test_output_type_hash
      d = create_driver(CONFIG + "\noutput_type hash")
      etime = event_time
      time = Time.at(etime.sec)
      out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
      assert_equal "#{time.localtime} filter.test: {\"test\"=>\"test\"}\n", out

      # NOTE: Float::NAN is not jsonable, but hash string can output it.
      d = create_driver(CONFIG + "\noutput_type hash")
      out = capture_log(d) { filter(d, etime, {'test' => Float::NAN}) }
      assert_equal "#{time.localtime} filter.test: {\"test\"=>NaN}\n", out
    end

    # Use include_time_key to output the message's time
    def test_include_time_key
      d = create_driver(CONFIG + "\noutput_type json\ninclude_time_key true\nutc")
      etime = event_time
      time = Time.at(etime.sec)
      message_time = event_time("2011-01-02 13:14:15 UTC")
      out = capture_log(d) { filter(d, message_time, {'test' => 'test'}) }
      assert_equal "#{time.localtime} filter.test: {\"test\":\"test\",\"time\":\"2011-01-02T13:14:15+00:00\"}\n", out
    end

    # out_stdout formatter itself can also be replaced
    def test_format_json
      d = create_driver(CONFIG + "\nformat json")
      out = capture_log(d) { filter(d, event_time, {'test' => 'test'}) }
      assert_equal "{\"test\":\"test\"}\n", out
    end
  end
end

