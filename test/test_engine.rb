require_relative 'helper'
require 'fluent/engine'
require 'fluent/config'
require 'fluent/input'
require 'fluent/system_config'
require 'fluent/plugin/output'
require 'fluent/event'

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

      e = assert_raise(Fluent::ConfigError) do
        engine.reload_config(new_conf)
      end
      assert e.message.match?('Unreloadable plugin: dummy_engine_class_var_test')

      assert_kind_of DummyEngineTestInput, engine.root_agent.inputs[0]
      assert_kind_of DummyEngineTestOutput, engine.root_agent.outputs[0]
    end
  end

  sub_test_case '#reload_config with a runtime label lookup' do
    class LabelAtRuntimeOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_output('label_at_runtime', self)
      helpers :event_emitter
      config_param :to, :string
      def process(tag, es)
        event_emitter_router(@to).emit_stream("#{tag}.out", es)
      end
    end

    class CountOutput < Fluent::Plugin::Output
      Fluent::Plugin.register_output('count_reload_events', self)
      def self.events; @events ||= []; end
      def self.reset_events; @events = []; end
      def process(tag, es)
        es.each { |_time, record| self.class.events << record["message"] }
      end
    end

    DIRECT_CONF = <<-CONF
      <match t>
        @type label_at_runtime
        to @L
      </match>
      <label @L>
        <match **>
          @type count_reload_events
        </match>
      </label>
    CONF

    # The configuration in the review of #5628: the old relabel in @L has
    # dropped its router once the old configuration runs after_shutdown.
    RELABEL_CONF = <<-CONF
      <match t>
        @type label_at_runtime
        to @L
      </match>
      <label @L>
        <match **>
          @type relabel
          @label @C
        </match>
      </label>
      <label @C>
        <match **>
          @type count_reload_events
        </match>
      </label>
    CONF

    # The same, with the runtime lookup done by a plugin inside a <label>.
    RELABEL_IN_LABEL_CONF = <<-CONF
      <match t>
        @type relabel
        @label @L2
      </match>
      <label @L2>
        <match **>
          @type label_at_runtime
          to @L
        </match>
      </label>
      <label @L>
        <match **>
          @type relabel
          @label @C
        </match>
      </label>
      <label @C>
        <match **>
          @type count_reload_events
        </match>
      </label>
    CONF

    def build_engine(conf_data)
      conf = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      system_config = Fluent::SystemConfig.create(conf)
      engine = Fluent::EngineClass.new
      engine.init(system_config)
      # event_emitter_router reads the global Fluent::Engine.root_agent
      stub(Fluent::Engine).root_agent { engine.root_agent }
      engine.configure(conf)
      engine.root_agent.start
      [engine, conf]
    end

    def find_runtime_output(engine)
      ([engine.root_agent] + engine.root_agent.labels.values)
        .flat_map(&:outputs)
        .find { |o| o.is_a?(LabelAtRuntimeOutput) }
    end

    def event(message)
      Fluent::OneEventStream.new(Fluent::EventTime.now, { "message" => message })
    end

    # Runs the block during configure of the new configuration, right before
    # the <match> of the new label named label_name is added.
    def during_add_match_of(label_name, &block)
      stub.proxy(Fluent::Label).new do |label|
        if label.context == label_name
          label.singleton_class.prepend(Module.new do
            define_method(:add_match) do |*args|
              block.call
              super(*args)
            end
          end)
        end
        label
      end
    end

    teardown do
      CountOutput.reset_events
    end

    test 'an event during reload does not leave the new label dropping that tag' do
      CountOutput.reset_events
      engine, conf = build_engine(DIRECT_CONF)
      old_output = find_runtime_output(engine)
      during_add_match_of("@L") { old_output.process("t", event("during")) }

      engine.reload_config(conf)
      5.times { |i| engine.root_agent.event_router.emit("t", Fluent::EventTime.now, { "message" => "after#{i}" }) }

      assert_equal ["during", "after0", "after1", "after2", "after3", "after4"], CountOutput.events
    end

    test 'events after a reload arrive when nothing is emitted during configure' do
      CountOutput.reset_events
      engine, conf = build_engine(DIRECT_CONF)
      engine.reload_config(conf)
      5.times { |i| engine.root_agent.event_router.emit("t", Fluent::EventTime.now, { "message" => "after#{i}" }) }
      assert_equal ["after0", "after1", "after2", "after3", "after4"], CountOutput.events
    end

    data('plugin in <match>' => RELABEL_CONF,
         'plugin in <label>' => RELABEL_IN_LABEL_CONF)
    test 'a lookup after the old configuration has shut down goes to the new configuration' do |conf_data|
      CountOutput.reset_events
      engine, conf = build_engine(conf_data)
      old_output = find_runtime_output(engine)

      engine.reload_config(conf)
      assert_nothing_raised { old_output.process("t", event("late")) }

      assert_equal ["late"], CountOutput.events
    end

    data('plugin in <match>' => RELABEL_CONF,
         'plugin in <label>' => RELABEL_IN_LABEL_CONF)
    test 'a router taken during reload and used after the old configuration has shut down goes to the new configuration' do |conf_data|
      CountOutput.reset_events
      engine, conf = build_engine(conf_data)
      old_output = find_runtime_output(engine)
      router = nil
      during_add_match_of("@C") { router = old_output.event_emitter_router("@L") }

      engine.reload_config(conf)
      assert_nothing_raised { router.emit_stream("t.out", event("late")) }

      assert_equal ["late"], CountOutput.events
    end
  end
end
