require_relative 'helper'
require 'fluent/engine'
require 'fluent/config'
require 'fluent/input'
require 'fluent/system_config'

class EngineTest < ::Test::Unit::TestCase
  class DummyEngineTestOutput < Fluent::Plugin::Output
    Fluent::Plugin.register_output('dummy_engine_test', self)
    def write(chunk); end
  end

  class DummyEngineTest2Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('dummy_engine_test2', self)
    def write(chunk); end
  end

  class DummyEngineTestInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('dummy_engine_test', self)
    def multi_workers_ready?; true; end
  end

  class DummyEngineTest2Input < Fluent::Plugin::Input
    Fluent::Plugin.register_input('dummy_engine_test2', self)
    def multi_workers_ready?; true; end
  end

  class DummyEngineClassVarTestInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('dummy_engine_class_var_test', self)
    @@test = nil
    def multi_workers_ready?; true; end
  end

  sub_test_case '#reload_config' do
    test 'reload new configuration' do
      conf_data = <<-CONF
        <source>
          @type dummy_engine_test
        </source>
        <match>
          @type dummy_engine_test
        </match>
      CONF

      conf = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      system_config = Fluent::SystemConfig.create(conf)

      engine = Fluent::EngineClass.new
      engine.init(system_config)
      engine.configure(conf)

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]

      new_conf_data = <<-CONF
        <source>
          @type dummy_engine_test2
        </source>
        <match>
          @type dummy_engine_test2
        </match>
      CONF

      new_conf = Fluent::Config.parse(new_conf_data, '(test)', '(test_dir)', true)

      agent = Fluent::RootAgent.new(log: $log, system_config: system_config)
      stub(Fluent::RootAgent).new do
        stub(agent).start.once
        agent
      end

      engine.reload_config(new_conf)

      assert_kind_of DummyEngineTest2Input, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTest2Output, engine.root_agent.outputs[0]
    end

    test "doesn't start RootAgent when supervisor is true" do
      conf_data = <<-CONF
        <source>
          @type dummy_engine_test
        </source>
        <match>
          @type dummy_engine_test
        </match>
      CONF

      conf = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      system_config = Fluent::SystemConfig.create(conf)

      engine = Fluent::EngineClass.new
      engine.init(system_config)
      engine.configure(conf)

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]

      new_conf_data = <<-CONF
        <source>
          @type dummy_engine_test2
        </source>
        <match>
          @type dummy_engine_test2
        </match>
      CONF

      new_conf = Fluent::Config.parse(new_conf_data, '(test)', '(test_dir)', true)

      agent = Fluent::RootAgent.new(log: $log, system_config: system_config)
      stub(Fluent::RootAgent).new do
        stub(agent).start.never
        agent
      end

      engine.reload_config(new_conf, supervisor: true)

      assert_kind_of DummyEngineTest2Input, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTest2Output, engine.root_agent.outputs[0]
    end

    test 'raise an error when conf is invalid' do
      conf_data = <<-CONF
        <source>
          @type dummy_engine_test
        </source>
        <match>
          @type dummy_engine_test
        </match>
      CONF

      conf = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      system_config = Fluent::SystemConfig.create(conf)

      engine = Fluent::EngineClass.new
      engine.init(system_config)
      engine.configure(conf)

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]

      new_conf_data = <<-CONF
        <source>
          @type
        </source>
      CONF

      new_conf = Fluent::Config.parse(new_conf_data, '(test)', '(test_dir)', true)

      agent = Fluent::RootAgent.new(log: $log, system_config: system_config)
      stub(Fluent::RootAgent).new do
        stub(agent).start.never
        agent
      end

      assert_raise(Fluent::ConfigError.new("Missing '@type' parameter on <source> directive")) do
        engine.reload_config(new_conf)
      end

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]
    end

    test 'raise an error when unreloadable exists' do
      conf_data = <<-CONF
        <source>
          @type dummy_engine_test
        </source>
        <match>
          @type dummy_engine_test
        </match>
      CONF

      conf = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      system_config = Fluent::SystemConfig.create(conf)

      engine = Fluent::EngineClass.new
      engine.init(system_config)
      engine.configure(conf)

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]

      conf_data = <<-CONF
        <source>
          @type dummy_engine_class_var_test
        </source>
        <match>
          @type dummy_engine_test
        </match>
      CONF

      new_conf = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)

      assert_raise(Fluent::ConfigError.new('Unreloadable plugin: EngineTest::DummyEngineClassVarTestInput')) do
        engine.reload_config(new_conf)
      end

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]
    end
  end
end
