require_relative 'helper'
require 'fluent/filter'

class FilterTest < Test::Unit::TestCase
  include Fluent

  setup do
    Fluent::Test.setup
    @time = Fluent::Engine.now
  end

  def create_driver(klass = Fluent::Filter, conf = '')
    Test::FilterTestDriver.new(klass).configure(conf, true)
  end

  def emit(klass, msgs, conf = '')
    d = create_driver(klass, conf)
    d.run {
      msgs.each {|msg|
        d.emit({'message' => msg}, @time)
      }
    }.filtered
  end

  sub_test_case 'configure' do
    test 'check default' do
      assert_nothing_raised { create_driver }
    end
  end

  sub_test_case 'filter' do
    test 'NotImplementedError' do
      not_implemented_filter = Class.new(Fluent::Filter)
      assert_raise(NotImplementedError) { emit(not_implemented_filter, ['foo']) }
    end

    test 'null filter' do
      null_filter = Class.new(Fluent::Filter) do |c|
        def filter(tag, time, record)
          nil
        end
      end
      es = emit(null_filter, ['foo'])
      assert_equal(0, es.instance_variable_get(:@record_array).size)
    end

    test 'pass filter' do
      pass_filter = Class.new(Fluent::Filter) do |c|
        def filter(tag, time, record)
          record
        end
      end
      es = emit(pass_filter, ['foo'])
      assert_equal(1, es.instance_variable_get(:@record_array).size)
    end
  end

  sub_test_case 'filter_stream' do
    test 'null filter' do
      null_filter = Class.new(Fluent::Filter) do |c|
        def filter_stream(tag, es)
          MultiEventStream.new
        end
      end
      es = emit(null_filter, ['foo'])
      assert_equal(0, es.instance_variable_get(:@record_array).size)
    end

    test 'pass filter' do
      pass_filter = Class.new(Fluent::Filter) do |c|
        def filter_stream(tag, es)
          es
        end
      end
      es = emit(pass_filter, ['foo'])
      assert_equal(1, es.instance_variable_get(:@record_array).size)
    end
  end
end
