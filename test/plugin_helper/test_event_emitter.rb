require_relative '../helper'
require 'fluent/plugin_helper/event_emitter'
require 'fluent/plugin/base'

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
end
