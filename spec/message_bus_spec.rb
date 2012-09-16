
describe Fluentd::MessageBus do
  let(:bus) { Fluentd::MessageBus.new }
  let(:agent) { Fluentd::Agent.new }
  let(:config) { Fluentd::Config::Element.new('ROOT', '', {}, []) }

  it { bus.default_collector.should be_a_kind_of(
    Fluentd::Collectors::NoMatchCollector) }

  it 'configure_agent' do
    agent.should_receive(:configure).with(config)
    bus.send(:configure_agent, agent, config)
  end

  it 'configure_agent StreamSource' do
    agent.should_receive(:configure).with(config)

    bus
    ibus = Fluentd::MessageBus.new
    Fluentd::MessageBus.should_receive(:new).and_return(ibus)

    agent.should_receive(:is_a?).with(Fluentd::StreamSource).and_return(true)
    ibus.should_receive(:configure).with(config)
    ibus.should_receive(:default_collector=).with(bus)
    agent.should_receive(:setup_internal_bus).with(ibus)

    bus.send(:configure_agent, agent, config)
  end

  it 'add_source' do
    Fluentd::Engine.setup!
    Fluentd::Plugin.should_receive(:new_input).with(:test).and_return(agent)
    bus.should_receive(:configure_agent).with(agent, config)
    bus.add_source(:test, config)
  end

  it 'add_output' do
    Fluentd::Engine.setup!
    Fluentd::Plugin.should_receive(:new_output).with(:test).and_return(agent)
    bus.should_receive(:configure_agent).with(agent, config)
    bus.add_output(:test, '**', config)
  end

  it 'add_filter' do
    Fluentd::Engine.setup!
    Fluentd::Plugin.should_receive(:new_filter).with(:test).and_return(agent)
    bus.should_receive(:configure_agent).with(agent, config)
    bus.add_filter(:test, '**', config)
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

    def new_filter_match(pattern, agent)
      Fluentd::MessageBus::FilterMatch.new(new_match_pattern(pattern), agent)
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

    it do
      bus.matches << new_filter_match('**', output1)
      bus.matches << new_match('**', output2)
      output1.should_receive(:open).and_return(1)
      output2.should_receive(:open).and_return(2)
      c = bus.open('t')
      c.should be_a_kind_of(Fluentd::Collectors::FilteringCollector::FilteringWriter)
      c.writers.should == [1,2]
    end

    it do
      bus.matches << new_filter_match('**', output1)
      bus.matches << new_filter_match('**', output2)
      bus.matches << new_match('**', output3)
      output1.should_receive(:open).and_return(1)
      output2.should_receive(:open).and_return(2)
      output3.should_receive(:open).and_return(3)
      c = bus.open('t')
      c.should be_a_kind_of(Fluentd::Collectors::FilteringCollector::FilteringWriter)
      c.writers.should == [1,2,3]
    end

    it do
      bus.matches << new_match('**', output1)
      bus.matches << new_filter_match('**', output2)
      output1.should_receive(:open).and_return(1)
      output2.should_not_receive(:open)
      bus.open('t').should == 1
    end
  end
end

