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
  end

  sub_test_case 'output plugin test driver' do
    test 'returns the block value as the return value of #run' do
      d = Fluent::Test::Driver::Output.new(Class.new(Fluent::Plugin::Output)) do
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
      d = Fluent::Test::Driver::Filter.new(Class.new(Fluent::Plugin::Filter)) do
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
