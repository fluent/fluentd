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
    test 'check to implement `filter` method' do
      klass = Class.new(Fluent::Filter) do |c|
        def filter(tag, time, record); end
      end

      assert_nothing_raised do
        klass.new
      end
    end

    test 'check to implement `filter_with_time` method' do
      klass = Class.new(Fluent::Filter) do |c|
        def filter_with_time(tag, time, record); end
      end

      assert_nothing_raised do
        klass.new
      end
    end

    test 'DO NOT check when implement `filter_stream`' do
      klass = Class.new(Fluent::Filter) do |c|
        def filter_stream(tag, es); end
      end

      assert_nothing_raised do
        klass.new
      end
    end

    test 'NotImplementedError' do
      klass = Class.new(Fluent::Filter)

      assert_raise NotImplementedError do
        klass.new
      end
    end

    test 'duplicated method implementation' do
      klass = Class.new(Fluent::Filter) do |c|
        def filter(tag, time, record); end
        def filter_with_time(tag, time, record); end
      end

      assert_raise do
        klass.new
      end
    end
  end

  sub_test_case 'filter' do
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
        def filter(tag, time, record); record; end
      end
      es = emit(null_filter, ['foo'])
      assert_equal(0, es.instance_variable_get(:@record_array).size)
    end

    test 'pass filter' do
      pass_filter = Class.new(Fluent::Filter) do |c|
        def filter_stream(tag, es)
          es
        end
        def filter(tag, time, record); record; end
      end
      es = emit(pass_filter, ['foo'])
      assert_equal(1, es.instance_variable_get(:@record_array).size)
    end
  end
end
