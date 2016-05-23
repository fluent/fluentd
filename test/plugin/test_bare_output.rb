require_relative '../helper'
require 'fluent/plugin/bare_output'
require 'fluent/event'

module FluentPluginBareOutputTest
  class DummyPlugin < Fluent::Plugin::BareOutput
    attr_reader :store
    def initialize
      super
      @store = []
    end
    def process(tag, es)
      es.each do |time, record|
        @store << [tag, time, record]
      end
    end
  end
end

class BareOutputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @p = FluentPluginBareOutputTest::DummyPlugin.new
  end

  test 'has healthy lifecycle' do
    assert !@p.configured?
    @p.configure(config_element())
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

    @p.configure(config_element())

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
      class FluentPluginBareOutputTest::DummyPlugin2 < Fluent::Plugin::BareOutput
        helpers :storage
      end
    end
  end

  test 'can get input event stream to write' do
    @p.configure(config_element('ROOT'))
    @p.start

    es1 = Fluent::OneEventStream.new(event_time('2016-05-21 18:37:31 +0900'), {'k1' => 'v1'})
    es2 = Fluent::ArrayEventStream.new([
        [event_time('2016-05-21 18:38:33 +0900'), {'k2' => 'v2'}],
        [event_time('2016-05-21 18:39:10 +0900'), {'k3' => 'v3'}],
      ])
    @p.emit_events('mytest1', es1)
    @p.emit_events('mytest2', es2)

    all_events = [
      ['mytest1', event_time('2016-05-21 18:37:31 +0900'), {'k1' => 'v1'}],
      ['mytest2', event_time('2016-05-21 18:38:33 +0900'), {'k2' => 'v2'}],
      ['mytest2', event_time('2016-05-21 18:39:10 +0900'), {'k3' => 'v3'}],
    ]

    assert_equal all_events, @p.store
  end
end
