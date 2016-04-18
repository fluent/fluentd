require_relative 'helper'
require 'fluent/plugin_helper'
require 'fluent/plugin/base'

class ConfigTest < Test::Unit::TestCase
  module FluentTest; end

  sub_test_case 'Fluent::Plugin::Base.helpers method works as shortcut to include helper modules' do
    class FluentTest::PluginTest1 < Fluent::Plugin::TestBase
      helpers :event_emitter
    end
    class FluentTest::PluginTest2 < Fluent::Plugin::TestBase
      helpers :thread
    end
    class FluentTest::PluginTest3 < Fluent::Plugin::TestBase
      helpers :event_loop
    end
    class FluentTest::PluginTest4 < Fluent::Plugin::TestBase
      helpers :timer
    end
    class FluentTest::PluginTest5 < Fluent::Plugin::TestBase
      helpers :child_process
    end
    class FluentTest::PluginTest6 < Fluent::Plugin::TestBase
      helpers :retry_state
    end
    class FluentTest::PluginTest0 < Fluent::Plugin::TestBase
      helpers :event_emitter, :thread, :event_loop, :timer, :child_process, :retry_state
    end

    test 'plugin can include helper event_emitter' do
      assert FluentTest::PluginTest1.include?(Fluent::PluginHelper::EventEmitter)
      p1 = FluentTest::PluginTest1.new
      assert p1.respond_to?(:has_router?)
      assert p1.has_router?
    end

    test 'plugin can include helper thread' do
      assert FluentTest::PluginTest2.include?(Fluent::PluginHelper::Thread)
      p2 = FluentTest::PluginTest2.new
      assert p2.respond_to?(:thread_current_running?)
      assert p2.respond_to?(:thread_create)
    end

    test 'plugin can include helper event_loop' do
      assert FluentTest::PluginTest3.include?(Fluent::PluginHelper::EventLoop)
      p3 = FluentTest::PluginTest3.new
      assert p3.respond_to?(:event_loop_attach)
      assert p3.respond_to?(:event_loop_running?)
    end

    test 'plugin can include helper timer' do
      assert FluentTest::PluginTest4.include?(Fluent::PluginHelper::Timer)
      p4 = FluentTest::PluginTest4.new
      assert p4.respond_to?(:timer_execute)
    end

    test 'plugin can include helper child_process' do
      assert FluentTest::PluginTest5.include?(Fluent::PluginHelper::ChildProcess)
      p5 = FluentTest::PluginTest5.new
      assert p5.respond_to?(:child_process_execute)
    end

    test 'plugin can 2 or more helpers at once' do
      assert FluentTest::PluginTest0.include?(Fluent::PluginHelper::EventEmitter)
      assert FluentTest::PluginTest0.include?(Fluent::PluginHelper::Thread)
      assert FluentTest::PluginTest0.include?(Fluent::PluginHelper::EventLoop)
      assert FluentTest::PluginTest0.include?(Fluent::PluginHelper::Timer)
      assert FluentTest::PluginTest0.include?(Fluent::PluginHelper::ChildProcess)

      p0 = FluentTest::PluginTest0.new
      assert p0.respond_to?(:child_process_execute)
      assert p0.respond_to?(:timer_execute)
      assert p0.respond_to?(:event_loop_attach)
      assert p0.respond_to?(:event_loop_running?)
      assert p0.respond_to?(:thread_current_running?)
      assert p0.respond_to?(:thread_create)
      assert p0.respond_to?(:has_router?)
    end
  end
end
