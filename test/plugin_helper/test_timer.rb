require_relative '../helper'
require 'fluent/plugin_helper/timer'
require 'fluent/plugin/base'

class TimerTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::Base
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
    d1.timer_execute(:test, 0.1) do
      counter += 1
    end

    sleep 0.55

    d1.stop
    assert !d1.timer_running?

    assert{ counter >= 4 && counter <= 6 }

    d1.shutdown; d1.close; d1.terminate
  end

  test 'can run many timers' do
    d1 = Dummy.new
    d1.configure(config_element())
    d1.start

    counter1 = 0
    counter2 = 0

    d1.timer_execute(:t1, 0.1) do
      counter1 += 1
    end
    d1.timer_execute(:t2, 0.25) do
      counter2 += 1
    end

    sleep 0.55

    d1.stop

    assert{ counter1 >= 4 && counter1 <= 6 }
    assert{ counter2 == 2 }

    d1.shutdown; d1.close; d1.terminate
  end

  test 'aborts timer which raises exceptions' do
    d1 = Dummy.new
    d1.configure(config_element())
    d1.start

    counter1 = 0
    counter2 = 0

    d1.timer_execute(:t1, 0.1) do
      counter1 += 1
    end
    d1.timer_execute(:t2, 0.1) do
      raise "abort!!!!!!" if counter2 > 2
      counter2 += 1
    end

    sleep 0.55

    d1.stop

    assert{ counter1 >= 4 && counter1 <= 6 }
    assert{ counter2 == 3 }
    msg = "[error]: Unexpected error raised. Stopping the timer. title=:t2 error=#<RuntimeError: abort!!!!!!> error_class=RuntimeError"
    assert{ d1.log.out.logs.any?{|line| line.include?(msg) } }
    msg = "[error]: Timer detached. title=:t2"
    assert{ d1.log.out.logs.any?{|line| line.include?(msg) } }

    d1.shutdown; d1.close; d1.terminate
  end
end
