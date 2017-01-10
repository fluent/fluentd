require_relative '../helper'
require 'fluent/plugin/filter'
require 'fluent/event'
require 'flexmock/test_unit'

module FluentPluginFilterTest
  class DummyPlugin < Fluent::Plugin::Filter
  end
  class NumDoublePlugin < Fluent::Plugin::Filter
    def filter(tag, time, record)
      r = record.dup
      r["num"] = r["num"].to_i * 2
      r
    end
  end
  class IgnoreForNumPlugin < Fluent::Plugin::Filter
    def filter(tag, time, record)
      if record["num"].is_a? Numeric
        nil
      else
        record
      end
    end
  end
  class RaiseForNumPlugin < Fluent::Plugin::Filter
    def filter(tag, time, record)
      if record["num"].is_a? Numeric
        raise "Value of num is Number!"
      end
      record
    end
  end
  class NumDoublePluginWithTime < Fluent::Plugin::Filter
    def filter_with_time(tag, time, record)
      r = record.dup
      r["num"] = r["num"].to_i * 2
      [time, r]
    end
  end
  class IgnoreForNumPluginWithTime < Fluent::Plugin::Filter
    def filter_with_time(tag, time, record)
      if record["num"].is_a? Numeric
        nil
      else
        [time, record]
      end
    end
  end
  class InvalidPlugin < Fluent::Plugin::Filter
    # Because of implemnting `filter_with_time` and `filter` methods
    def filter_with_time(tag, time, record); end
    def filter(tag, time, record); end
  end
end

class FilterPluginTest < Test::Unit::TestCase
  DummyRouter = Struct.new(:emits) do
    def emit_error_event(tag, time, record, error)
      self.emits << [tag, time, record, error]
    end
  end

  setup do
    @p = nil
  end

  teardown do
    if @p
      @p.stop unless @p.stopped?
      @p.before_shutdown unless @p.before_shutdown?
      @p.shutdown unless @p.shutdown?
      @p.after_shutdown unless @p.after_shutdown?
      @p.close unless @p.closed?
      @p.terminate unless @p.terminated?
    end
  end

  sub_test_case 'for basic dummy plugin' do
    setup do
      Fluent::Test.setup
    end

    test 'plugin does not define #filter raises error' do
      assert_raise NotImplementedError do
        FluentPluginFilterTest::DummyPlugin.new
      end
    end
  end

  sub_test_case 'normal filter plugin' do
    setup do
      Fluent::Test.setup
      @p = FluentPluginFilterTest::NumDoublePlugin.new
    end

    test 'has healthy lifecycle' do
      assert !@p.configured?
      @p.configure(config_element)
      assert @p.configured?

      assert !@p.started?
      @p.start
      assert @p.start

      assert !@p.stopped?
      @p.stop
      assert @p.stopped?

      assert !@p.before_shutdown?
      @p.before_shutdown
      assert @p.before_shutdown?

      assert !@p.shutdown?
      @p.shutdown
      assert @p.shutdown?

      assert !@p.after_shutdown?
      @p.after_shutdown
      assert @p.after_shutdown?

      assert !@p.closed?
      @p.close
      assert @p.closed?

      assert !@p.terminated?
      @p.terminate
      assert @p.terminated?
    end

    test 'has plugin_id automatically generated' do
      assert @p.respond_to?(:plugin_id_configured?)
      assert @p.respond_to?(:plugin_id)

      @p.configure(config_element)

      assert !@p.plugin_id_configured?
      assert @p.plugin_id
      assert{ @p.plugin_id != 'mytest' }
    end

    test 'has plugin_id manually configured' do
      @p.configure(config_element('ROOT', '', {'@id' => 'mytest'}))
      assert @p.plugin_id_configured?
      assert_equal 'mytest', @p.plugin_id
    end

    test 'has plugin logger' do
      assert @p.respond_to?(:log)
      assert @p.log

      # default logger
      original_logger = @p.log

      @p.configure(config_element('ROOT', '', {'@log_level' => 'debug'}))

      assert{ @p.log.object_id != original_logger.object_id }
      assert_equal Fluent::Log::LEVEL_DEBUG, @p.log.level
    end

    test 'can load plugin helpers' do
      assert_nothing_raised do
        class FluentPluginFilterTest::DummyPlugin2 < Fluent::Plugin::Filter
          helpers :storage
        end
      end
    end

    test 'are available with multi worker configuration in default' do
      assert @p.multi_workers_ready?
    end

    test 'filters events correctly' do
      test_es = [
        [event_time('2016-04-19 13:01:00 -0700'), {"num" => "1", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:03 -0700'), {"num" => "2", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:05 -0700'), {"num" => "3", "message" => "Hello filters!"}],
      ]
      @p.configure(config_element)
      es = @p.filter_stream('testing', test_es)
      assert es.is_a? Fluent::EventStream

      ary = []
      es.each do |time, r|
        ary << [time, r]
      end

      assert_equal 3, ary.size

      assert_equal event_time('2016-04-19 13:01:00 -0700'), ary[0][0]
      assert_equal "Hello filters!", ary[0][1]["message"]
      assert_equal 2, ary[0][1]["num"]

      assert_equal event_time('2016-04-19 13:01:03 -0700'), ary[1][0]
      assert_equal 4, ary[1][1]["num"]

      assert_equal event_time('2016-04-19 13:01:05 -0700'), ary[2][0]
      assert_equal 6, ary[2][1]["num"]
    end
  end

  sub_test_case 'filter plugin returns nil for some records' do
    setup do
      Fluent::Test.setup
      @p = FluentPluginFilterTest::IgnoreForNumPlugin.new
    end

    test 'filter_stream ignores records which #filter return nil' do
      test_es = [
        [event_time('2016-04-19 13:01:00 -0700'), {"num" => "1", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:03 -0700'), {"num" => 2, "message" => "Ignored, yay!"}],
        [event_time('2016-04-19 13:01:05 -0700'), {"num" => "3", "message" => "Hello filters!"}],
      ]
      @p.configure(config_element)
      es = @p.filter_stream('testing', test_es)
      assert es.is_a? Fluent::EventStream

      ary = []
      es.each do |time, r|
        ary << [time, r]
      end

      assert_equal 2, ary.size

      assert_equal event_time('2016-04-19 13:01:00 -0700'), ary[0][0]
      assert_equal "Hello filters!", ary[0][1]["message"]
      assert_equal "1", ary[0][1]["num"]

      assert_equal event_time('2016-04-19 13:01:05 -0700'), ary[1][0]
      assert_equal "3", ary[1][1]["num"]
    end
  end

  sub_test_case 'filter plugin raises error' do
    setup do
      Fluent::Test.setup
      @p = FluentPluginFilterTest::RaiseForNumPlugin.new
    end

    test 'has router and can emit events to error streams' do
      assert @p.has_router?
      @p.configure(config_element)
      assert @p.router

      @p.router = DummyRouter.new([])

      test_es = [
        [event_time('2016-04-19 13:01:00 -0700'), {"num" => "1", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:03 -0700'), {"num" => 2, "message" => "Hello error router!"}],
        [event_time('2016-04-19 13:01:05 -0700'), {"num" => "3", "message" => "Hello filters!"}],
      ]
      es = @p.filter_stream('testing', test_es)
      assert es.is_a? Fluent::EventStream

      ary = []
      es.each do |time, r|
        ary << [time, r]
      end

      assert_equal 2, ary.size

      assert_equal event_time('2016-04-19 13:01:00 -0700'), ary[0][0]
      assert_equal "Hello filters!", ary[0][1]["message"]
      assert_equal "1", ary[0][1]["num"]

      assert_equal event_time('2016-04-19 13:01:05 -0700'), ary[1][0]
      assert_equal "3", ary[1][1]["num"]

      assert_equal 1, @p.router.emits.size

      error_emits = @p.router.emits

      assert_equal "testing", error_emits[0][0]
      assert_equal event_time('2016-04-19 13:01:03 -0700'), error_emits[0][1]
      assert_equal({"num" => 2, "message" => "Hello error router!"}, error_emits[0][2])
      assert{ error_emits[0][3].is_a? RuntimeError }
      assert_equal "Value of num is Number!", error_emits[0][3].message
    end
  end

  sub_test_case 'filter plugins that is implmented `filter_with_time`' do
    setup do
      Fluent::Test.setup
      @p = FluentPluginFilterTest::NumDoublePluginWithTime.new
    end

    test 'filters events correctly' do
      test_es = [
        [event_time('2016-04-19 13:01:00 -0700'), {"num" => "1", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:03 -0700'), {"num" => "2", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:05 -0700'), {"num" => "3", "message" => "Hello filters!"}],
      ]
      es = @p.filter_stream('testing', test_es)
      assert es.is_a? Fluent::EventStream

      ary = []
      es.each do |time, r|
        ary << [time, r]
      end

      assert_equal 3, ary.size

      assert_equal event_time('2016-04-19 13:01:00 -0700'), ary[0][0]
      assert_equal "Hello filters!", ary[0][1]["message"]
      assert_equal 2, ary[0][1]["num"]

      assert_equal event_time('2016-04-19 13:01:03 -0700'), ary[1][0]
      assert_equal 4, ary[1][1]["num"]

      assert_equal event_time('2016-04-19 13:01:05 -0700'), ary[2][0]
      assert_equal 6, ary[2][1]["num"]
    end
  end

  sub_test_case 'filter plugin that is implmented `filter_with_time` and returns nil for some records' do
    setup do
      Fluent::Test.setup
      @p = FluentPluginFilterTest::IgnoreForNumPluginWithTime.new
    end

    test 'filter_stream ignores records which #filter_with_time return nil' do
      test_es = [
        [event_time('2016-04-19 13:01:00 -0700'), {"num" => "1", "message" => "Hello filters!"}],
        [event_time('2016-04-19 13:01:03 -0700'), {"num" => 2, "message" => "Ignored, yay!"}],
        [event_time('2016-04-19 13:01:05 -0700'), {"num" => "3", "message" => "Hello filters!"}],
      ]
      @p.configure(config_element)
      es = @p.filter_stream('testing', test_es)
      assert es.is_a? Fluent::EventStream

      ary = []
      es.each do |time, r|
        ary << [time, r]
      end

      assert_equal 2, ary.size

      assert_equal event_time('2016-04-19 13:01:00 -0700'), ary[0][0]
      assert_equal "Hello filters!", ary[0][1]["message"]
      assert_equal "1", ary[0][1]["num"]

      assert_equal event_time('2016-04-19 13:01:05 -0700'), ary[1][0]
      assert_equal "3", ary[1][1]["num"]
    end
  end

  sub_test_case 'filter plugins that is implmented both `filter_with_time` and `filter`' do
    setup do
      Fluent::Test.setup
    end

    test 'raises DuplicatedImplementError' do
      assert_raise do
        FluentPluginFilterTest::InvalidPlugin.new
      end
    end
  end
end
