require 'spec_helper'

describe Fluentd::MessageBus do
  let(:root_bus) { Fluentd::LabeledMessageBus.new }
  let(:bus) { Fluentd::MessageBus.new(root_bus) }
  let(:agent) { Fluentd::Agent.new }
  let(:config) {
    e = Fluentd::Config::Element.new('ROOT', '', {}, [])
    Fluentd::ConfigContext.inject!(e)
    e.context.plugin = plugin
    e
  }
  let(:plugin) { PluginClass.new }

  class ::Fluentd::MessageBus
    attr_reader :matches
  end

  it do
    root_bus.default_collector.should be_a_kind_of(Fluentd::Collectors::NoMatchCollector)
  end

  it do
    bus.default_collector.should be_a_kind_of(Fluentd::LabeledMessageBus)
  end

  it 'configure_agent' do
    agent.should_receive(:configure).with(config)
    bus.send(:configure_agent, agent, config, LabeledMessageBus.new)
  end

  #it 'configure_agent StreamSource' do
  #  agent.should_receive(:configure).with(config)

  #  ibus = Fluentd::MessageBus.new(root_bus)
  #  Fluentd::MessageBus.should_receive(:new).and_return(ibus)

  #  agent.should_receive(:is_a?).with(Fluentd::StreamSource).and_return(true)
  #  ibus.should_receive(:configure).with(config)
  #  agent.should_receive(:setup_internal_bus).with(ibus)

  #  bus.send(:configure_agent, agent, config, LabeledMessageBus.new)
  #end

  it 'add_source' do
    plugin.should_receive(:new_input).with(:test).and_return(agent)
    bus.should_receive(:configure_agent).with(agent, config, bus)
    bus.add_source(:test, config)
  end

  it 'add_match' do
    plugin.should_receive(:new_output).with(:test).and_return(agent)
    bus.should_receive(:configure_agent).with(agent, config, bus)
    bus.add_match(:test, '**', config)
  end

  context 'match' do
    let(:output1) { Fluentd::Outputs::BasicOutput.new }
    let(:output2) { Fluentd::Outputs::BasicOutput.new }
    let(:output3) { Fluentd::Outputs::BasicOutput.new }

    def new_match_pattern(str)
      Fluentd::MatchPattern.create(str)
    end

    def new_match(pattern, agent)
      Fluentd::MessageBus::Match.new(new_match_pattern(pattern), agent)
    end

    it do
      bus.matches << new_match('**', output1)
      output1.should_receive(:open).and_return(1)
      bus.open('t').should == 1
    end

    it do
      bus.matches << new_match('**', output1)
      bus.matches << new_match('**', output2)
      output1.should_receive(:open).and_return(1)
      output2.should_not_receive(:open)
      bus.open('t').should == 1
    end

    it do
      bus.matches << new_match('t', output1)
      bus.matches << new_match('**', output2)
      output1.should_not_receive(:open)
      output2.should_receive(:open).and_return(2)
      bus.open('x').should == 2
    end
  end
end

