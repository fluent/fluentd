require_relative 'helper'
require 'fluent/plugin/input'
require 'fluent/test/driver/input'
require 'fluent/plugin/output'
require 'fluent/test/driver/output'
require 'fluent/plugin/filter'
require 'fluent/test/driver/filter'
require 'fluent/plugin/multi_output'
require 'fluent/test/driver/multi_output'
require 'fluent/plugin/parser'
require 'fluent/test/driver/parser'
require 'fluent/plugin/formatter'
require 'fluent/test/driver/formatter'

require 'timecop'

class TestDriverTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  sub_test_case 'plugin test driver' do
    data(
      'input plugin test driver'  => [Fluent::Test::Driver::Input, Fluent::Plugin::Input],
      'multi_output plugin test driver' => [Fluent::Test::Driver::MultiOutput, Fluent::Plugin::MultiOutput],
      'parser plugin test driver'       => [Fluent::Test::Driver::Parser, Fluent::Plugin::Parser],
      'formatter plugin test driver'    => [Fluent::Test::Driver::Formatter, Fluent::Plugin::Formatter],
    )
    test 'returns the block value as the return value of #run' do |args|
      driver_class, plugin_class = args
      d = driver_class.new(Class.new(plugin_class))
      v = d.run do
        x = 1 + 2
        y = 2 + 4
        3 || x || y
      end
      assert_equal 3, v
    end

    data(
      'input plugin test driver'  => [Fluent::Test::Driver::Input, Fluent::Plugin::Input],
      'multi_output plugin test driver' => [Fluent::Test::Driver::MultiOutput, Fluent::Plugin::MultiOutput],
      'parser plugin test driver'       => [Fluent::Test::Driver::Parser, Fluent::Plugin::Parser],
      'formatter plugin test driver'    => [Fluent::Test::Driver::Formatter, Fluent::Plugin::Formatter],
    )
    test 'raises error for hard timeout' do |args|
      driver_class, plugin_class = args
      d = driver_class.new(Class.new(plugin_class))
      assert_raise Fluent::Test::Driver::TestTimedOut do
        d.run(timeout: 1) do
          sleep 5
        end
      end
    end

    data(
      'input plugin test driver'  => [Fluent::Test::Driver::Input, Fluent::Plugin::Input],
      'multi_output plugin test driver' => [Fluent::Test::Driver::MultiOutput, Fluent::Plugin::MultiOutput],
      'parser plugin test driver'       => [Fluent::Test::Driver::Parser, Fluent::Plugin::Parser],
      'formatter plugin test driver'    => [Fluent::Test::Driver::Formatter, Fluent::Plugin::Formatter],
    )
    test 'can stop with soft timeout for blocks never stops, even with Timecop' do |args|
      Timecop.freeze(Time.parse("2016-11-04 18:49:00"))
      begin
        driver_class, plugin_class = args
        d = driver_class.new(Class.new(plugin_class))
        assert_nothing_raised do
          before = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          d.end_if{ false }
          d.run(timeout: 5) do
            sleep 0.1 until d.stop?
          end
          after = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          assert{ after >= before + 1.0 }
        end
      ensure
        Timecop.return
      end
    end

    test 'raise errors raised in threads' do
      d = Fluent::Test::Driver::Input.new(Fluent::Plugin::Input) do
        helpers :thread
        def start
          super
          thread_create(:input_thread_for_test_driver_test) do
            sleep 0.5
            raise "yaaaaaaaaaay!"
          end
        end
      end
      assert_raise RuntimeError.new("yaaaaaaaaaay!") do
        d.end_if{ false }
        d.run(timeout: 3) do
          sleep 0.1 until d.stop?
        end
      end
    end
  end

  sub_test_case 'output plugin test driver' do
    test 'returns the block value as the return value of #run' do
      d = Fluent::Test::Driver::Output.new(Fluent::Plugin::Output) do
        def prefer_buffered_processing
          false
        end
        def process(tag, es)
          # drop
        end
      end
      v = d.run do
        x = 1 + 2
        y = 2 + 4
        3 || x || y
      end
      assert_equal 3, v
    end
  end

  sub_test_case 'filter plugin test driver' do
    test 'returns the block value as the return value of #run' do
      d = Fluent::Test::Driver::Filter.new(Fluent::Plugin::Filter) do
        def filter(tag, time, record)
          record
        end
      end
      v = d.run do
        x = 1 + 2
        y = 2 + 4
        3 || x || y
      end
      assert_equal 3, v
    end
  end
end
