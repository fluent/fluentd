require_relative '../helper'
require 'fluent/plugin_helper/event_loop'
require 'fluent/plugin/base'

class EventLoopTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :event_loop
    def configure(conf)
      super
      @_event_loop_run_timeout = 0.1
    end
  end

  test 'can be instantiated to be able to create event loop' do
    d1 = Dummy.new
    assert d1.respond_to?(:event_loop_attach)
    assert d1.respond_to?(:event_loop_running?)
    assert d1.respond_to?(:_event_loop)
    assert d1._event_loop
    assert !d1.event_loop_running?
  end

  test 'can be configured' do
    d1 = Dummy.new
    assert_nothing_raised do
      d1.configure(config_element())
    end
    assert d1.plugin_id
    assert d1.log
  end

  test 'can run event loop by start, stop by shutdown/close and clear by terminate' do
    d1 = Dummy.new
    d1.configure(config_element())
    assert !d1.event_loop_running?

    d1.start
    d1.event_loop_wait_until_start

    assert d1.event_loop_running?
    assert_equal 1, d1._event_loop.watchers.size

    d1.shutdown
    d1.close

    assert !d1.event_loop_running?

    d1.terminate

    assert_nil d1._event_loop
  end
end
