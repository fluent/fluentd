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

  CONFIG = config_element('ROOT')

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
        d = create_driver(CONFIG + config_element("", "", { "output_type" => data }))
        d.run {}
        assert_equal data, d.instance.formatter.output_type
      end

      def test_invalid_output_type
        assert_raise(Fluent::ConfigError) do
          d = create_driver(CONFIG + config_element("", "", { "output_type" => "foo" }))
          d.run {}
        end
      end
    end

    def test_output_type_json
      d = create_driver(CONFIG + config_element("", "", { "output_type" => "json" }))
      etime = event_time("2016-10-07 21:09:31.012345678 UTC")
      out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
      assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\":\"test\"}\n", out

      # NOTE: Float::NAN is not jsonable
      d = create_driver(CONFIG + config_element("", "", { "output_type" => "json" }))
      flexmock(d.instance.router).should_receive(:emit_error_event)
      filter(d, etime, {'test' => Float::NAN})
    end

    def test_output_type_hash
      d = create_driver(CONFIG + config_element("", "", { "output_type" => "hash" }))
      etime = event_time("2016-10-07 21:09:31.012345678 UTC")
      out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
      assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\"=>\"test\"}\n", out

      # NOTE: Float::NAN is not jsonable, but hash string can output it.
      d = create_driver(CONFIG + config_element("", "", { "output_type" => "hash" }))
      out = capture_log(d) { filter(d, etime, {'test' => Float::NAN}) }
      assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\"=>NaN}\n", out
    end

    # Use include_time_key to output the message's time
    def test_include_time_key
      config = config_element("", "", {
                                "output_type" => "json",
                                "include_time_key" => true,
                                "localtime" => false
                              })
      d = create_driver(config)
      etime = event_time("2016-10-07 21:09:31.012345678 UTC")
      out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
      assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\":\"test\",\"time\":\"2016-10-07T21:09:31Z\"}\n", out
    end

    # out_stdout formatter itself can also be replaced
    def test_format_json
      d = create_driver(CONFIG + config_element("", "", { "format" => "json" }))
      out = capture_log(d) { filter(d, event_time, {'test' => 'test'}) }
      assert_equal "{\"test\":\"test\"}\n", out
    end
  end

  sub_test_case "with <format> sub section" do
    sub_test_case "configure" do
      def test_default
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout"})
        d = create_driver(conf)
        d.run {}
        assert_equal("json", d.instance.formatter.output_type)
      end

      data(json: "json",
           hash: "hash",
           ltsv: "ltsv")
      def test_output_type(data)
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout", "output_type" => data })
        d = create_driver(conf)
        d.run {}
        assert_equal(data, d.instance.formatter.output_type)
      end

      def test_invalid_output_type
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout", "output_type" => "foo" })
        assert_raise(Fluent::ConfigError) do
          d = create_driver(conf)
          d.run {}
        end
      end
    end

    sub_test_case "output_type" do
      def test_json
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout", "output_type" => "json" })
        d = create_driver(conf)
        etime = event_time("2016-10-07 21:09:31.012345678 UTC")
        out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
        assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\":\"test\"}\n", out
      end

      def test_json_nan
        # NOTE: Float::NAN is not jsonable
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout", "output_type" => "json" })
        d = create_driver(conf)
        etime = event_time("2016-10-07 21:09:31.012345678 UTC")
        flexmock(d.instance.router).should_receive(:emit_error_event)
        filter(d, etime, {'test' => Float::NAN})
      end

      def test_hash
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout", "output_type" => "hash" })
        d = create_driver(conf)
        etime = event_time("2016-10-07 21:09:31.012345678 UTC")
        out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
        assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\"=>\"test\"}\n", out
      end

      def test_hash_nan
        # NOTE: Float::NAN is not jsonable, but hash string can output it.
        conf = config_element
        conf.elements << config_element("format", "", { "@type" => "stdout", "output_type" => "hash" })
        d = create_driver(conf)
        etime = event_time("2016-10-07 21:09:31.012345678 UTC")
        out = capture_log(d) { filter(d, etime, {'test' => Float::NAN}) }
        assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\"=>NaN}\n", out
      end

      # Use include_time_key to output the message's time
      def test_include_time_key
        conf = config_element
        conf.elements << config_element("format", "", {
                                          "@type" => "stdout",
                                          "output_type" => "json"
                                        })
        conf.elements << config_element("inject", "", {
                                          "time_key" => "time",
                                          "time_type" => "string",
                                          "localtime" => false
                                          })
        d = create_driver(conf)
        etime = event_time("2016-10-07 21:09:31.012345678 UTC")
        out = capture_log(d) { filter(d, etime, {'test' => 'test'}) }
        assert_equal "2016-10-07 21:09:31.012345678 +0000 filter.test: {\"test\":\"test\",\"time\":\"2016-10-07T21:09:31Z\"}\n", out
      end
    end
  end
end
