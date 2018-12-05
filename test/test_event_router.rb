require_relative 'helper'
require 'fluent/event_router'
require_relative 'test_plugin_classes'

class EventRouterTest < ::Test::Unit::TestCase
  include Fluent
  include FluentTest

  teardown do
    @output = nil
    @filter = nil
    @compat_filter = nil
    @error_output = nil
    @emit_handler = nil
    @default_collector = nil
  end

  def output
    @output ||= FluentTestOutput.new
  end

  def filter
    @filter ||= FluentTestFilter.new
  end

  def compat_filter
    @compat_filter ||= FluentCompatTestFilter.new
  end

  def error_output
    @error_output ||= FluentTestErrorOutput.new
  end

  def emit_handler
    @emit_handler ||= TestEmitErrorHandler.new
  end

  def default_collector
    @default_collector ||= FluentTestOutput.new
  end

  def event(record, time = Engine.now)
    OneEventStream.new(time, record)
  end

  DEFAULT_EVENT_NUM = 5

  def events(num = DEFAULT_EVENT_NUM)
    es = MultiEventStream.new
    num.times { |i|
      es.add(Engine.now, 'key' => "value#{i}")
    }
    es
  end

  sub_test_case EventRouter::MatchCache do
    setup do
      @match_cache = EventRouter::MatchCache.new
    end

    test "call block when non-cached key" do
      assert_raise(RuntimeError.new('Test!')) {
        @match_cache.get('test') { raise 'Test!' }
      }
    end

    test "don't call block when cached key" do
      @match_cache.get('test') { "I'm cached" }
      assert_nothing_raised {
        @match_cache.get('test') { raise 'Test!' }
      }
      assert_equal "I'm cached", @match_cache.get('test') { raise 'Test!' }
    end

    test "call block when keys are expired" do
      cache_size = EventRouter::MatchCache::MATCH_CACHE_SIZE
      cache_size.times { |i|
        @match_cache.get("test#{i}") { "I'm cached #{i}" }
      }
      assert_nothing_raised {
        cache_size.times { |i|
          @match_cache.get("test#{i}") { raise "Why called?" }
        }
      }
      # expire old keys
      cache_size.times { |i|
        @match_cache.get("new_test#{i}") { "I'm young #{i}" }
      }
      num_called = 0
      cache_size.times { |i|
        @match_cache.get("test#{i}") { num_called += 1 }
      }
      assert_equal cache_size, num_called
    end
  end

  sub_test_case EventRouter::Pipeline do
    setup do
      @pipeline = EventRouter::Pipeline.new
      @es = event('key' => 'value')
    end

    test 'set one output' do
      @pipeline.set_output(output)
      @pipeline.emit_events('test', @es)
      assert_equal 1, output.events.size
      assert_equal 'value', output.events['test'].first['key']
    end

    sub_test_case 'with filter' do
      setup do
        @pipeline.set_output(output)
      end

      data('Filter plugin' => 'filter',
           'Compat::Filter plugin' => 'compat_filter')
      test 'set one filer' do |filter_type|
        @pipeline.add_filter(filter_type == 'filter' ? filter : compat_filter)
        @pipeline.emit_events('test', @es)
        assert_equal 1, output.events.size
        assert_equal 'value', output.events['test'].first['key']
        assert_equal 0, output.events['test'].first['__test__']
      end

      data('Filter plugin' => 'filter',
           'Compat::Filter plugin' => 'compat_filter')
      test 'set one filer with multi events' do |filter_type|
        @pipeline.add_filter(filter_type == 'filter' ? filter : compat_filter)
        @pipeline.emit_events('test', events)
        assert_equal 1, output.events.size
        assert_equal 5, output.events['test'].size
        DEFAULT_EVENT_NUM.times { |i|
          assert_equal "value#{i}", output.events['test'][i]['key']
          assert_equal i, output.events['test'][i]['__test__']
        }
      end
    end
  end

  sub_test_case EventRouter do
    teardown do
      @event_router = nil
    end

    def event_router
      @event_router ||= EventRouter.new(default_collector, emit_handler)
    end

    sub_test_case 'default collector' do
      test 'call default collector when no output' do
        assert_rr do
          mock(default_collector).emit_events('test', is_a(OneEventStream))
          event_router.emit('test', Engine.now, 'k' => 'v')
        end
      end

      test "call default collector when only filter" do
        event_router.add_rule('test', filter)
        assert_rr do
          # After apply Filter, EventStream becomes MultiEventStream by default
          mock(default_collector).emit_events('test', is_a(MultiEventStream))
          event_router.emit('test', Engine.now, 'k' => 'v')
        end
        assert_equal 1, filter.num
      end

      test "call default collector when no matched with output" do
        event_router.add_rule('test', output)
        assert_rr do
          mock(default_collector).emit_events('dummy', is_a(OneEventStream))
          event_router.emit('dummy', Engine.now, 'k' => 'v')
        end
      end

      test "don't call default collector when tag matched" do
        event_router.add_rule('test', output)
        assert_rr do
          dont_allow(default_collector).emit_events('test', is_a(OneEventStream))
          event_router.emit('test', Engine.now, 'k' => 'v')
        end
        # check emit handler doesn't catch rr error
        assert_empty emit_handler.events
      end
    end

    sub_test_case 'filter' do
      test 'filter should be called when tag matched' do
        filter = Class.new(FluentTestFilter) { |x|
          def filter_stream(_tag, es); end
        }.new

        event_router.add_rule('test', filter)

        assert_rr do
          mock(filter).filter_stream('test', is_a(OneEventStream)) { events }
          event_router.emit('test', Engine.now, 'k' => 'v')
        end
      end

      test 'filter should not be called when tag mismatched' do
        event_router.add_rule('test', filter)

        assert_rr do
          dont_allow(filter).filter_stream('test', is_a(OneEventStream)) { events }
          event_router.emit('foo', Engine.now, 'k' => 'v')
        end
      end

      test 'filter changes records' do
        event_router.add_rule('test', filter)
        event_router.add_rule('test', output)
        event_router.emit('test', Engine.now, 'k' => 'v')

        assert_equal 1, filter.num
        assert_equal 1, output.events['test'].size
        assert_equal 0, output.events['test'].first['__test__']
        assert_equal 'v', output.events['test'].first['k']
      end

      test 'filter can be chained' do
        other_filter = FluentTestFilter.new('__hoge__')
        event_router.add_rule('test', filter)
        event_router.add_rule('test', other_filter)
        event_router.add_rule('test', output)
        event_router.emit('test', Engine.now, 'k' => 'v')

        assert_equal 1, filter.num
        assert_equal 1, other_filter.num
        assert_equal 1, output.events['test'].size
        assert_equal 0, output.events['test'].first['__test__']
        assert_equal 0, output.events['test'].first['__hoge__']
        assert_equal 'v', output.events['test'].first['k']
      end
    end

    sub_test_case 'optimized filter' do
      setup do
        @record = { 'k' => 'v' }
        @now = Engine.now
      end

      test 'call optimized filter when the filter plugin implements #filter without #filter_stream' do
        event_router.add_rule('test', filter)

        assert_rr do
          mock(filter).filter('test', @now, @record) { @record }
          event_router.emit('test', @now, @record)
        end
      end

      test 'call optimized filter when the filter plugin implements #filter_with_time without #filter_stream' do
        filter = Class.new(FluentTestFilter) {
          undef_method :filter
          def filter_with_time(tag, time, record); end
        }.new

        event_router.add_rule('test', filter)

        assert_rr do
          mock(filter).filter_with_time('test', @now, @record) { [time, @record] }
          event_router.emit('test', @now, @record)
        end
      end

      test "don't call optimized filter when filter plugins implement #filter_stream" do
        filter = Class.new(FluentTestFilter) {
          undef_method :filter
          def filter_stream(tag, time, record); end
        }.new

        event_router.add_rule('test', filter)

        assert_rr do
          mock(filter).filter_stream('test', is_a(OneEventStream)) { OneEventStream.new(@now, @record) }
          event_router.emit('test', @now, @record)
        end
      end

      test 'call optimized filter when filter plugins have #filter_with_time instead of #filter' do
        filter_with_time = Class.new(FluentTestFilter) {
          undef_method :filter
          def filter_with_time(tag, time, record); end
        }.new

        event_router.add_rule('test', filter_with_time)
        event_router.add_rule('test', filter)

        assert_rr do
          mock(filter_with_time).filter_with_time('test', @now, @record) { [@now + 1, @record] }
          mock(filter).filter('test', @now + 1, @record) { @record }
          event_router.emit('test', @now, @record)
        end
      end

      test "don't call optimized filter even if just a filter of some filters implements #filter_stream method" do
        filter_stream = Class.new(FluentTestFilter) {
          def filter_stream(tag, record); end
        }.new

        filter_with_time = Class.new(FluentTestFilter) {
          undef_method :filter
          def filter_with_time(tag, time, record); end
        }.new

        filters = [filter_stream, filter_with_time, filter]
        filters.each { |f| event_router.add_rule('test', f) }

        e = OneEventStream.new(@now, @record)
        assert_rr do
          mock($log).info("disable filter chain optimization because #{[filter_stream].map(&:class)} uses `#filter_stream` method.")
          mock(filter_stream).filter_stream('test', is_a(OneEventStream)) { e }
          mock(filter).filter_stream('test', is_a(OneEventStream)) { e }
          mock(filter_with_time).filter_stream('test', is_a(OneEventStream)) { e }
          event_router.emit('test', @now, @record)
        end
      end
    end

    sub_test_case 'emit_error_handler' do
      test 'call handle_emits_error when emit failed' do
        event_router.add_rule('test', error_output)

        event_router.emit('test', Engine.now, 'k' => 'v')
        assert_rr do
          mock(emit_handler).handle_emits_error('test', is_a(OneEventStream), is_a(RuntimeError))
          event_router.emit('test', Engine.now, 'k' => 'v')
        end
      end
    end
  end
end
