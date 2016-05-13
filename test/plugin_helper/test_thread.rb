require_relative '../helper'
require 'fluent/plugin_helper/thread'
require 'fluent/plugin/base'
require 'timeout'

class ThreadTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :thread
    def configure(conf)
      super
      @_thread_wait_seconds = 0.1
      self
    end
  end

  test 'can be instantiated to be able to create threads' do
    d1 = Dummy.new
    assert d1.respond_to?(:thread_current_running?)
    assert d1.respond_to?(:thread_create)
    assert d1.respond_to?(:_threads)
    assert !d1.thread_current_running?
    assert d1._threads.empty?
  end

  test 'can be configured' do
    d1 = Dummy.new
    assert_nothing_raised do
      d1.configure(config_element())
    end
    assert d1.plugin_id
    assert d1.log
  end

  test 'can create thread after prepared' do
    d1 = Dummy.new
    d1.configure(config_element())
    d1.start

    m1 = Mutex.new
    m2 = Mutex.new

    m1.lock
    thread_run = false

    Timeout.timeout(10) do
      t = d1.thread_create(:test1) do
        m2.lock

        assert !d1._threads.empty? # this must be true always
        assert d1.thread_current_running?

        thread_run = true
        m2.unlock
        m1.lock
      end
      Thread.pass until m2.locked? || thread_run

      m2.lock; m2.unlock
      assert_equal 1, d1._threads.size

      assert_equal :test1, t[:_fluentd_plugin_helper_thread_title]
      assert t[:_fluentd_plugin_helper_thread_running]
      assert !d1._threads.empty?

      m1.unlock

      while t[:_fluentd_plugin_helper_thread_running]
        Thread.pass
      end
    end

    assert d1._threads.empty?

    d1.stop; d1.shutdown; d1.close; d1.terminate
  end

  test 'can wait until all threads start' do
    d1 = Dummy.new.configure(config_element()).start
    ary = []
    d1.thread_create(:t1) do
      ary << 1
    end
    d1.thread_create(:t2) do
      ary << 2
    end
    d1.thread_create(:t3) do
      ary << 3
    end
    Timeout.timeout(10) do
      d1.thread_wait_until_start
    end
    assert_equal [1,2,3], ary.sort

    d1.stop; d1.shutdown; d1.close; d1.terminate
  end

  test 'can stop threads which is watching thread_current_running?, and then close it' do
    d1 = Dummy.new.configure(config_element()).start

    m1 = Mutex.new
    thread_in_run = false
    Timeout.timeout(10) do
      t = d1.thread_create(:test2) do
        thread_in_run = true
        m1.lock
        while d1.thread_current_running?
          Thread.pass
        end
        thread_in_run = false
        m1.unlock
      end
      Thread.pass until m1.locked?

      assert thread_in_run
      assert !d1._threads.empty?

      d1.stop
      Thread.pass while m1.locked?
      assert !t[:_fluentd_plugin_helper_thread_running]
      assert t.stop?
    end

    assert d1._threads.empty?

    d1.stop; d1.shutdown; d1.close; d1.terminate
  end

  test 'can terminate threads forcedly which is running forever' do
    d1 = Dummy.new.configure(config_element()).start

    m1 = Mutex.new
    thread_in_run = false
    Timeout.timeout(10) do
      t = d1.thread_create(:test2) do
        thread_in_run = true
        m1.lock
        while true
          Thread.pass
        end
        thread_in_run = false
      end
      Thread.pass until m1.locked?

      assert thread_in_run
      assert !d1._threads.empty?

      d1.stop
      assert !t[:_fluentd_plugin_helper_thread_running]
      assert t.alive?

      d1.shutdown
      assert t.alive?
      assert !d1._threads.empty?

      d1.close
      assert t.alive?
      assert !d1._threads.empty?

      d1.terminate
      assert t.stop?
      assert d1._threads.empty?
    end
  end
end
