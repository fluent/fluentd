require_relative 'helper'
require 'fluent/fluent_log_event_router'
require 'fluent/root_agent'
require 'fluent/system_config'

class FluentLogEventRouterTest < ::Test::Unit::TestCase
  # @param config [String]
  def build_config(config)
    Fluent::Config.parse(config, 'fluent_log_event', '', syntax: :v1)
  end

  sub_test_case 'NullFluentLogEventRouter does nothing' do
    test 'emittable? returns false but others does nothing' do
      null_event_router = Fluent::NullFluentLogEventRouter.new
      null_event_router.start
      null_event_router.stop
      null_event_router.graceful_stop
      null_event_router.emit_event(nil)
      assert_false null_event_router.emittable?
    end
  end

  sub_test_case '#build' do
    test 'NullFluentLogEventRouter if root_agent have not internal logger' do
      root_agent = Fluent::RootAgent.new(log: $log, system_config: Fluent::SystemConfig.new)
      root_agent.configure(build_config(''))

      d = Fluent::FluentLogEventRouter.build(root_agent)
      assert_equal Fluent::NullFluentLogEventRouter, d.class
    end

    test 'FluentLogEventRouter if <match fluent.*> exists in config' do
      root_agent = Fluent::RootAgent.new(log: $log, system_config: Fluent::SystemConfig.new)
      root_agent.configure(build_config(<<-CONFIG))
        <match fluent.*>
          @type null
        </match>
      CONFIG

      d = Fluent::FluentLogEventRouter.build(root_agent)
      assert_equal Fluent::FluentLogEventRouter, d.class
    end

    test 'FluentLogEventRouter if <label @FLUENT_LOG> exists in config' do
      root_agent = Fluent::RootAgent.new(log: $log, system_config: Fluent::SystemConfig.new)
      root_agent.configure(build_config(<<-CONFIG))
       <label @FLUENT_LOG>
         <match *>
           @type null
         </match>
        </label>
      CONFIG

      d = Fluent::FluentLogEventRouter.build(root_agent)
      assert_equal Fluent::FluentLogEventRouter, d.class
    end
  end

  test 'when calling graceful_stop, it flushes all events' do
    event_router = []
    stub(event_router).emit do |tag, time, record|
      event_router.push([tag, time, record])
    end

    d = Fluent::FluentLogEventRouter.new(event_router)

    t = Time.now
    msg = ['tag', t, { 'key' => 'value' }]
    d.emit_event(msg)
    d.graceful_stop
    d.emit_event(msg)
    d.start

    d.graceful_stop # to call join
    assert_equal 2, event_router.size
    assert_equal msg, event_router[0]
    assert_equal msg, event_router[1]
  end

  test 'when calling stop, it ignores existing events' do
    event_router = []
    stub(event_router).emit do |tag, time, record|
      event_router.push([tag, time, record])
    end

    d = Fluent::FluentLogEventRouter.new(event_router)

    t = Time.now
    msg = ['tag', t, { 'key' => 'value' }]
    d.emit_event(msg)
    d.stop
    d.emit_event(msg)
    d.start

    d.stop # to call join
    assert_equal 1, event_router.size
    assert_equal msg, event_router[0]
  end
end
