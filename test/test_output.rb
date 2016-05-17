require_relative 'helper'
require 'fluent/test'
require 'fluent/output'
require 'fluent/output_chain'
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

    def test_calc_retry_wait
      omit "too internal test"

      # default
      d = create_driver
      d.instance.retry_limit.times {
        # "d.instance.instance_variable_get(:@num_errors) += 1" causes SyntaxError
        d.instance.instance_eval { @num_errors += 1 }
      }
      wait = d.instance.retry_wait * (2 ** (d.instance.retry_limit - 1))
      assert( d.instance.calc_retry_wait > wait - wait / 8.0 )

      # max_retry_wait
      d = create_driver(CONFIG + %[max_retry_wait 4])
      d.instance.retry_limit.times {
        d.instance.instance_eval { @num_errors += 1 }
      }
      assert_equal 4, d.instance.calc_retry_wait
    end

    def test_calc_retry_wait_with_integer_retry_wait
      omit "too internal test"

      d = create_driver(CONFIG + %[retry_wait 2s])
      d.instance.retry_limit.times {
        d.instance.instance_eval { @num_errors += 1 }
      }
      assert_equal true, d.instance.calc_retry_wait.finite?
    end

    def test_large_num_retries
      omit "too internal test"

      # Test that everything works properly after a very large number of
      # retries and we hit the expected max_retry_wait.
      exp_max_retry_wait = 300
      d = create_driver(CONFIG + %[
        disable_retry_limit true
        max_retry_wait #{exp_max_retry_wait}
      ])
      d.instance.instance_eval { @num_errors += 1000 }
      assert_equal exp_max_retry_wait, d.instance.calc_retry_wait
      d.instance.instance_eval { @num_errors += 1000 }
      assert_equal exp_max_retry_wait, d.instance.calc_retry_wait
      d.instance.instance_eval { @num_errors += 1000 }
      assert_equal exp_max_retry_wait, d.instance.calc_retry_wait
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

    def test_submit_flush_target
      omit "too internal test"

      # default
      d = create_mock_driver
      d.instance.start_mock
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.shutdown
      assert_equal 1, d.instance.submit_flush_threads.size

      # num_threads 4
      d = create_mock_driver(CONFIG + %[num_threads 4])
      d.instance.start
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 1, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 2, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 3, d.instance.instance_variable_get('@writer_current_position')
      d.instance.submit_flush
      assert_equal 0, d.instance.instance_variable_get('@writer_current_position')
      d.instance.shutdown
      assert (d.instance.submit_flush_threads.size > 1), "fails if only one thread works to submit flush"
    end

    def test_secondary
      d = create_driver(CONFIG + %[
        <secondary>
          type test
          name c0
        </secondary>
      ])
      assert_not_nil d.instance.instance_variable_get(:@secondary).router
    end

    sub_test_case "test_force_flush" do
      setup do
        time = Time.parse("2011-01-02 13:14:15 UTC")
        Timecop.freeze(time)
        @time = time.to_i
      end

      teardown do
        Timecop.return
      end

      test "force_flush works on retrying" do
        omit "too internal test"

        d = create_driver(CONFIG)
        d.instance.start
        buffer = d.instance.instance_variable_get(:@buffer)
        # imitate 10 failures
        d.instance.instance_variable_set(:@num_errors, 10)
        d.instance.instance_variable_set(:@next_retry_time, @time + d.instance.calc_retry_wait)
        # buffer should be popped (flushed) immediately
        flexmock(buffer).should_receive(:pop).once
        # force_flush
        buffer.emit("test", 'test', NullOutputChain.instance)
        d.instance.force_flush
        10.times { sleep 0.05 }
      end
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
      time_slice_format %Y%m%d
    ]

    class TimeSlicedOutputTestPlugin < Fluent::TimeSlicedOutput
      def format(tag, time, record)
        ''
      end
    end

    def create_driver(conf=CONFIG)
      Fluent::Test::TimeSlicedOutputTestDriver.new(TimeSlicedOutputTestPlugin).configure(conf, true)
    end

    sub_test_case "force_flush test" do
      setup do
        time = Time.parse("2011-01-02 13:14:15 UTC")
        Timecop.freeze(time)
        @es = OneEventStream.new(time.to_i, {"message" => "foo"})
      end

      teardown do
        Timecop.return
      end

      test "force_flush immediately flushes" do
        omit "too internal test"

        d = create_driver(CONFIG + %[
          time_format %Y%m%d%H%M%S
        ])
        d.instance.start
        # buffer should be popped (flushed) immediately
        flexmock(d.instance.instance_variable_get(:@buffer)).should_receive(:pop).once
        # force_flush
        d.instance.emit('test', @es, NullOutputChain.instance)
        d.instance.force_flush
        10.times { sleep 0.05 }
      end
    end

    sub_test_case "test emit" do
      setup do
        @time = Time.parse("2011-01-02 13:14:15 UTC")
        Timecop.freeze(@time)
      end

      teardown do
        Timecop.return
      end

      test "emit with valid event" do
        d = create_driver
        d.instance.start
        if d.instance.method(:emit).arity == 3
          d.instance.emit('test', OneEventStream.new(@time.to_i, {"message" => "foo"}), NullOutputChain.instance)
        else
          d.instance.emit('test', OneEventStream.new(@time.to_i, {"message" => "foo"}))
        end
        assert_equal 0, d.instance.log.logs.size
      end

      test "emit with invalid event" do
        d = create_driver
        d.instance.start
        assert_raise ArgumentError, "time must be a Fluent::EventTime (or Integer)" do
          d.instance.emit_events('test', OneEventStream.new('string', 10))
        end
      end
    end
  end
end
