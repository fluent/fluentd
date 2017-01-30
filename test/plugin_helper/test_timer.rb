require_relative '../helper'
require 'fluent/plugin_helper/timer'
require 'fluent/plugin/base'

class TimerTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :timer
  end

  test 'can be instantiated under state that timer is not running' do
    d1 = Dummy.new
    assert d1.respond_to?(:timer_running?)
    assert !d1.timer_running?
  end

  test 'can be configured' do
    d1 = Dummy.new
    assert_nothing_raised do
      d1.configure(config_element())
    end
    assert d1.plugin_id
    assert d1.log
  end

  test 'can start timers by start' do
    d1 = Dummy.new
    d1.configure(config_element())
    assert !d1.timer_running?
    d1.start
    assert d1.timer_running?

    counter = 0
    d1.timer_execute(:test, 1) do
      counter += 1
    end

    sleep 5

    d1.stop
    assert !d1.timer_running?

    assert{ counter >= 3 && counter <= 6 }

    d1.shutdown; d1.close; d1.terminate
  end

  test 'can run many timers' do
    d1 = Dummy.new
    d1.configure(config_element())
    d1.start

    counter1 = 0
    counter2 = 0

    d1.timer_execute(:t1, 1) do
      counter1 += 1
    end
    d1.timer_execute(:t2, 2) do
      counter2 += 1
    end

    sleep 6

    d1.stop

    assert{ counter1 >= 4 && counter1 <= 7 }
    assert{ counter2 >= 2 && counter2 <= 4 }

    d1.shutdown; d1.close; d1.terminate
  end

  test 'aborts timer which raises exceptions' do
    d1 = Dummy.new
    d1.configure(config_element())
    d1.start

    counter1 = 0
    counter2 = 0

    d1.timer_execute(:t1, 1) do
      counter1 += 1
    end
    d1.timer_execute(:t2, 1) do
      raise "abort!!!!!!" if counter2 > 1
      counter2 += 1
    end

    sleep 5

    d1.stop

    assert{ counter1 >= 3 && counter1 <= 6 }
    assert{ counter2 == 2 }
    msg = "Unexpected error raised. Stopping the timer. title=:t2"
    assert{ d1.log.out.logs.any?{|line| line.include?("[error]:") && line.include?(msg) && line.include?("abort!!!!!!") } }
    assert{ d1.log.out.logs.any?{|line| line.include?("[error]:") && line.include?("Timer detached. title=:t2") } }

    d1.shutdown; d1.close; d1.terminate
  end

  test 'can run at once' do
    d1 = Dummy.new
    d1.configure(config_element())
    assert !d1.timer_running?
    d1.start
    assert d1.timer_running?

    waiting_assertion = true
    waiting_timer = true
    counter = 0
    d1.timer_execute(:test, 1, repeat: false) do
      sleep(0.1) while waiting_assertion
      counter += 1
      waiting_timer = false
    end

    watchers = d1._event_loop.watchers.reject {|w| w.is_a?(Fluent::PluginHelper::EventLoop::DefaultWatcher) }
    assert_equal(1, watchers.size)
    assert(watchers.first.attached?)

    waiting_assertion = false
    sleep(0.1) while waiting_timer

    assert_equal(1, counter)
    waiting(4){ sleep 0.1 while watchers.first.attached? }
    assert_false(watchers.first.attached?)
    watchers = d1._event_loop.watchers.reject {|w| w.is_a?(Fluent::PluginHelper::EventLoop::DefaultWatcher) }
    assert_equal(0, watchers.size)

    d1.shutdown; d1.close; d1.terminate
  end
end
