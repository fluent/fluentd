require 'helper'
require 'fluent/test'
require 'fluent/output'

module FluentOutputTest
  include Fluent

  class BufferedOutputTest < ::Test::Unit::TestCase
    include FluentOutputTest

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
      assert_equal 17, d.instance.retry_limit
      assert_equal 1.0, d.instance.retry_wait
      assert_equal nil, d.instance.max_retry_wait
      assert_equal 1.0, d.instance.retry_wait
      assert_equal 1, d.instance.num_threads
      assert_equal 1, d.instance.queued_chunk_flush_interval

      # max_retry_wait
      d = create_driver(CONFIG + %[max_retry_wait 4])
      assert_equal 4, d.instance.max_retry_wait
    end

    def test_calc_retry_wait
      # default
      d = create_driver
      d.instance.retry_limit.times { d.instance.instance_variable_get(:@error_history) << Engine.now }
      wait = d.instance.retry_wait * (2 ** (d.instance.retry_limit - 1))
      assert( d.instance.calc_retry_wait > wait - wait / 8.0 )

      # max_retry_wait
      d = create_driver(CONFIG + %[max_retry_wait 4])
      d.instance.retry_limit.times { d.instance.instance_variable_get(:@error_history) << Engine.now }
      assert_equal 4, d.instance.calc_retry_wait
    end
  end
end
