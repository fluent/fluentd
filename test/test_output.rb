require_relative 'helper'
require 'fluent/test'
require 'fluent/output'
require 'fluent/output_chain'
require 'fluent/plugin/buffer'
require 'timecop'
require 'flexmock/test_unit'

module FluentOutputTest
  include Fluent
  include FlexMock::TestCase

  class BufferedOutputTest < ::Test::Unit::TestCase
    include FluentOutputTest

    class << self
      def startup
        $LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), 'scripts'))
        require 'fluent/plugin/out_test'
        require 'fluent/plugin/out_test2'
      end

      def shutdown
        $LOAD_PATH.shift
      end
    end

    def setup
      Fluent::Test.setup
    end

    CONFIG = %[]

    def create_driver(conf=CONFIG)
      Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedOutput) do
        def write(chunk)
          chunk.read
        end
      end.configure(conf)
    end

    def test_configure
      # default
      d = create_driver
      assert_equal 'memory', d.instance.buffer_type
      assert_equal 60, d.instance.flush_interval
      assert_equal false, d.instance.disable_retry_limit
      assert_equal 17, d.instance.retry_limit
      assert_equal 1.0, d.instance.retry_wait
      assert_equal nil, d.instance.max_retry_wait
      assert_equal 1.0, d.instance.retry_wait
      assert_equal 1, d.instance.num_threads
      assert_equal 1, d.instance.queued_chunk_flush_interval

      # max_retry_wait
      d = create_driver(CONFIG + %[max_retry_wait 4])
      assert_equal 4, d.instance.max_retry_wait

      # disable_retry_limit
      d = create_driver(CONFIG + %[disable_retry_limit true])
      assert_equal true, d.instance.disable_retry_limit

      #### retry_state cares it
      # # retry_wait is converted to Float for calc_retry_wait
      # d = create_driver(CONFIG + %[retry_wait 1s])
      # assert_equal Float, d.instance.retry_wait.class
    end

    class FormatterInjectTestOutput < Fluent::Output
      def initialize
        super
        @formatter = nil
      end
    end
    def test_start
      i = FormatterInjectTestOutput.new
      i.configure(config_element('ROOT', '', {}, [config_element('inject', '', {'hostname_key' => "host"})]))
      assert_nothing_raised do
        i.start
      end
    end

    def create_mock_driver(conf=CONFIG)
      Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedOutput) do
        attr_accessor :submit_flush_threads

        def start_mock
          @started = false
          start
          # ensure OutputThread to start successfully
          submit_flush
          sleep 0.5
          while !@started
            submit_flush
            sleep 0.5
          end
        end

        def try_flush
          @started = true
          @submit_flush_threads ||= {}
          @submit_flush_threads[Thread.current] ||= 0
          @submit_flush_threads[Thread.current] += 1
        end

        def write(chunk)
          chunk.read
        end
      end.configure(conf)
    end

    def test_secondary
      d = Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedOutput) do
        def write(chunk)
          chunk.read
        end
      end

      mock(d.instance.log).warn("secondary type should be same with primary one",
                                { primary: d.instance.class.to_s, secondary: "Fluent::Plugin::Test2Output" })
      d.configure(CONFIG + %[
        <secondary>
          type test2
          name c0
        </secondary>
      ])

      assert_not_nil d.instance.instance_variable_get(:@secondary).router
    end

    def test_secondary_with_no_warn_log
      # ObjectBufferedOutput doesn't implemnt `custom_filter`
      d = Fluent::Test::BufferedOutputTestDriver.new(Fluent::ObjectBufferedOutput)

      mock(d.instance.log).warn("secondary type should be same with primary one",
                                { primary: d.instance.class.to_s, secondary: "Fluent::Plugin::Test2Output" }).never
      d.configure(CONFIG + %[
        <secondary>
          type test2
          name c0
        </secondary>
      ])

      assert_not_nil d.instance.instance_variable_get(:@secondary).router
    end

    test 'BufferQueueLimitError compatibility' do
      assert_equal Fluent::Plugin::Buffer::BufferOverflowError, Fluent::BufferQueueLimitError
    end
  end

  class ObjectBufferedOutputTest < ::Test::Unit::TestCase
    include FluentOutputTest

    def setup
      Fluent::Test.setup
    end

    CONFIG = %[]

    def create_driver(conf=CONFIG)
      Fluent::Test::OutputTestDriver.new(Fluent::ObjectBufferedOutput).configure(conf, true)
    end

    def test_configure
      # default
      d = create_driver
      assert_equal true, d.instance.time_as_integer
    end
  end

  class TimeSlicedOutputTest < ::Test::Unit::TestCase
    include FluentOutputTest
    include FlexMock::TestCase

    def setup
      Fluent::Test.setup
      FileUtils.rm_rf(TMP_DIR)
      FileUtils.mkdir_p(TMP_DIR)
    end

    TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/time_sliced_output")

    CONFIG = %[
      buffer_path #{TMP_DIR}/foo
      time_slice_format %Y%m%d%H
    ]

    class TimeSlicedOutputTestPlugin < Fluent::TimeSlicedOutput
      attr_reader :written_chunk_keys, :errors_in_write
      def initialize
        super
        @written_chunk_keys = []
        @errors_in_write = []
      end

      def configure(conf)
        super

        @formatter = Fluent::Plugin.new_formatter('out_file')
        @formatter.configure(conf)
      end

      def format(tag, time, record)
        @formatter.format(tag, time, record)
      end
      def write(chunk)
        @written_chunk_keys << chunk.key
        true
      rescue => e
        @errors_in_write << e
      end
    end

    def create_driver(conf=CONFIG)
      Fluent::Test::TimeSlicedOutputTestDriver.new(TimeSlicedOutputTestPlugin).configure(conf, true)
    end

    data(:none => '',
         :utc => "utc",
         :localtime => 'localtime',
         :timezone => 'timezone +0000')
    test 'configure with timezone related parameters' do |param|
      assert_nothing_raised {
        create_driver(CONFIG + param)
      }
    end

    sub_test_case "test emit" do
      setup do
        @time = Time.parse("2011-01-02 13:14:15 UTC")
        Timecop.freeze(@time)
      end

      teardown do
        Timecop.return
      end

      test "emit with invalid event" do
        d = create_driver
        d.instance.start
        d.instance.after_start
        assert_raise ArgumentError, "time must be a Fluent::EventTime (or Integer)" do
          d.instance.emit_events('test', OneEventStream.new('string', 10))
        end
      end

      test "plugin can get key of chunk in #write" do
        d = create_driver
        d.instance.start
        d.instance.after_start
        d.instance.emit_events('test', OneEventStream.new(event_time("2016-11-08 17:44:30 +0900"), {"message" => "yay"}))
        d.instance.force_flush
        waiting(10) do
          sleep 0.1 until d.instance.written_chunk_keys.size == 1
        end
        assert_equal [], d.instance.errors_in_write
        assert_equal ["2016110808"], d.instance.written_chunk_keys # default timezone is UTC
      end

      test "check formatted time compatibility with utc. Should Z, not +00:00" do
        d = create_driver(CONFIG + %[
          utc
          include_time_key
        ])
        time = Time.parse("2016-11-08 12:00:00 UTC").to_i
        d.emit({"a" => 1}, time)
        d.expect_format %[2016-11-08T12:00:00Z\ttest\t{"a":1,"time":"2016-11-08T12:00:00Z"}\n]
        d.run
      end
    end
  end
end
