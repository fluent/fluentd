require_relative '../helper'
require 'fluent/plugin/input'
require 'flexmock/test_unit'

module FluentPluginInputTest
  class DummyPlugin < Fluent::Plugin::Input
  end
end

class InputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @p = FluentPluginInputTest::DummyPlugin.new
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
      class FluentPluginInputTest::DummyPlugin2 < Fluent::Plugin::Input
        helpers :storage
      end
    end
  end

  test 'are not available with multi workers configuration in default' do
    assert_false @p.multi_workers_ready?
  end

  test 'has router and can emit into it' do
    assert @p.has_router?

    @p.configure(config_element())
    assert @p.router

    DummyRouter = Struct.new(:emits) do
      def emit(tag, es)
        self.emits << [tag, es]
      end
    end
    @p.router = DummyRouter.new([])
    @p.router.emit('mytag', [])
    @p.router.emit('mytag.testing', ['it is not es, but no problem for tests'])

    assert_equal ['mytag', []], @p.router.emits[0]
    assert_equal ['mytag.testing', ['it is not es, but no problem for tests']], @p.router.emits[1]
  end

  test 'has router for specified label if configured' do
    @p.configure(config_element())
    original_router = @p.router

    router_mock = flexmock('mytest')
    router_mock.should_receive(:emit).once.with('mytag.testing', ['for mock'])
    label_mock = flexmock('mylabel')
    label_mock.should_receive(:event_router).once.and_return(router_mock)
    Fluent::Engine.root_agent.labels['@mytest'] = label_mock

    @p.configure(config_element('ROOT', '', {'@label' => '@mytest'}))
    assert{ @p.router.object_id != original_router.object_id }

    @p.router.emit('mytag.testing', ['for mock'])
  end
end
