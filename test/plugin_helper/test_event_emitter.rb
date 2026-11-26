require_relative '../helper'
require 'fluent/plugin_helper/event_emitter'
require 'fluent/plugin/base'
require 'fluent/event_router'
require 'flexmock/test_unit'

class EventEmitterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  class Dummy0 < Fluent::Plugin::TestBase
  end

  class Dummy < Fluent::Plugin::TestBase
    helpers :event_emitter
  end

  test 'can be instantiated to be able to emit with router' do
    d0 = Dummy0.new
    assert d0.respond_to?(:has_router?)
    assert !d0.has_router?
    assert !d0.respond_to?(:router)

    d1 = Dummy.new
    assert d1.respond_to?(:has_router?)
    assert d1.has_router?
    assert d1.respond_to?(:router)
    d1.stop; d1.shutdown; d1.close; d1.terminate
  end

  test 'can be configured with valid router' do
    d1 = Dummy.new
    assert d1.has_router?
    assert_nil d1.router

    assert_nothing_raised do
      d1.configure(config_element())
    end

    assert d1.router

    d1.shutdown

    assert d1.router

    d1.close

    assert_nil d1.router

    d1.terminate
  end

  test 'should not have event_emitter_router' do
    d0 = Dummy0.new
    assert !d0.respond_to?(:event_emitter_router)
  end

  test 'should have event_emitter_router' do
    d = Dummy.new
    assert d.respond_to?(:event_emitter_router)
  end

  test 'get router' do
    router_mock = flexmock('mytest')
    label_mock = flexmock('mylabel')
    label_mock.should_receive(:event_router).twice.and_return(router_mock)
    Fluent::Engine.root_agent.labels['@mytest'] = label_mock

    d = Dummy.new
    d.configure(config_element('ROOT', '', {'@label' => '@mytest'}))
    router = d.event_emitter_router("@mytest")
    assert_equal router_mock, router
  end

  test 'get root router' do
    d = Dummy.new
    router = d.event_emitter_router("@ROOT")
    assert_equal Fluent::Engine.root_agent.event_router, router
  end

  test '#router should return the root router by default' do
    stub(Fluent::Engine.root_agent).event_router { "root_event_router" }
    stub(Fluent::Engine.root_agent).source_only_router { "source_only_router" }

    d = Dummy.new
    d.configure(Fluent::Config::Element.new('source', '', {}, []))
    assert_equal "root_event_router", d.router
  end

  test '#router should return source_only_router during source-only' do
    stub(Fluent::Engine.root_agent).event_router { "root_event_router" }
    stub(Fluent::Engine.root_agent).source_only_router { "source_only_router" }

    d = Dummy.new
    d.configure(Fluent::Config::Element.new('source', '', {}, []))
    d.event_emitter_apply_source_only

    router_when_source_only = d.router

    d.event_emitter_cancel_source_only

    router_after_canceled = d.router

    assert_equal(
      ["source_only_router", "root_event_router"],
      [router_when_source_only, router_after_canceled]
    )
  end

  sub_test_case 'event_emitter_router during a graceful reload' do
    class RecordingRouter
      attr_reader :events
      def initialize; @events = []; end
      def emit_stream(tag, es); @events << tag; end
      def emit(tag, time, record); @events << tag; end
    end

    setup do
      require 'fluent/root_agent'
      @old_agent = Fluent::RootAgent.new(log: $log)
      @new_agent = Fluent::RootAgent.new(log: $log)
      @routers = {}
      [[:old, @old_agent], [:new, @new_agent]].each do |name, agent|
        agent.add_label('@L')
        @routers[name] = RecordingRouter.new
        @routers[:"#{name}_root"] = RecordingRouter.new
        label_router = @routers[name]
        root_router = @routers[:"#{name}_root"]
        stub(agent.find_label('@L')).event_router { label_router }
        stub(agent).event_router { root_router }
      end
      stub(Fluent::Engine).root_agent { @new_agent }
    end

    def plugin_in(router)
      d = Dummy.new
      d.context_router = router
      d
    end

    def old_root_plugin
      plugin_in(Fluent::EventRouter.new(nil, @old_agent))
    end

    def old_label_plugin
      plugin_in(Fluent::EventRouter.new(nil, @old_agent.find_label('@L')))
    end

    test 'RootAgent#root_agent and Label#root_agent answer the owning configuration' do
      assert_same @old_agent, @old_agent.root_agent
      assert_same @old_agent, @old_agent.find_label('@L').root_agent
    end

    data('plugin in <match> of the old configuration' => :old_root_plugin,
         'plugin in <label> of the old configuration' => :old_label_plugin)
    test 'sends events to the label of its own configuration while it runs' do |plugin|
      d = __send__(plugin)
      d.event_emitter_router('@L').emit_stream('t', nil)
      d.event_emitter_router('@ROOT').emit('t2', nil, nil)
      assert_equal [['t'], [], ['t2'], []],
                   [@routers[:old].events, @routers[:new].events, @routers[:old_root].events, @routers[:new_root].events]
    end

    data('plugin in <match> of the old configuration' => :old_root_plugin,
         'plugin in <label> of the old configuration' => :old_label_plugin)
    test 'sends events to Engine.root_agent after its own configuration starts to shut down' do |plugin|
      d = __send__(plugin)
      router_taken_before_shutdown = d.event_emitter_router('@L')
      @old_agent.instance_variable_set(:@shutdown_started, true)
      router_taken_before_shutdown.emit_stream('before', nil)
      d.event_emitter_router('@L').emit_stream('after', nil)
      assert_equal [[], ['before', 'after']], [@routers[:old].events, @routers[:new].events]
    end

    test 'returns the EventRouter of the label itself outside a reload' do
      d = plugin_in(Fluent::EventRouter.new(nil, @new_agent))
      assert_same @routers[:new], d.event_emitter_router('@L')
      assert_same @routers[:new_root], d.event_emitter_router('@ROOT')
    end

    test 'uses Engine.root_agent on each call when the owner is not known' do
      d = plugin_in(nil)
      assert_same @routers[:new], d.event_emitter_router('@L')
      stub(Fluent::Engine).root_agent { @old_agent }
      assert_same @routers[:old], d.event_emitter_router('@L')
    end

    test 'raises for an unknown label at the time of the lookup' do
      assert_raise(ArgumentError) { old_root_plugin.event_emitter_router('@NOPE') }
    end
  end
end
