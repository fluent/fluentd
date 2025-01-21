require_relative '../helper'
require 'fluent/plugin_helper/event_emitter'
require 'fluent/plugin/base'
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
end
