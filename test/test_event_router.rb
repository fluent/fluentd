require 'fluent/event_router'
require_relative 'test_plugin_classes'

class EventRouterTest < ::Test::Unit::TestCase
  include Fluent
  include FluentTest

  teardown do
    @output = nil
    @filter = nil
  end

  def output
    @output ||= FluentTestOutput.new
  end

  def filter
    @filter ||= FluentTestFilter.new
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
      @pipeline.emit('test', @es, nil)
      assert_equal 1, output.events.size
      assert_equal 'value', output.events['test'].first['key']
    end

    sub_test_case 'with filter' do
      setup do
        @pipeline.set_output(output)
      end

      test 'set one filer' do
        @pipeline.add_filter(filter)
        @pipeline.emit('test', @es, nil)
        assert_equal 1, output.events.size
        assert_equal 'value', output.events['test'].first['key']
        assert_equal 0, output.events['test'].first['__test__']
      end

      test 'set one filer with multi events' do
        @pipeline.add_filter(filter)
        @pipeline.emit('test', events, nil)
        assert_equal 1, output.events.size
        assert_equal 5, output.events['test'].size
        DEFAULT_EVENT_NUM.times { |i|
          assert_equal "value#{i}", output.events['test'][i]['key']
          assert_equal i, output.events['test'][i]['__test__']
        }
      end
    end
  end
end

