require 'fluentd/engine'
require 'fluentd/root_agent'
require 'fluentd/match_pattern'
require 'fluentd/logger'
require 'fluentd/agent'

require_relative 'spec_classes'

describe Fluentd::Agent do
  before :each do
    Fluentd::Engine.setup_test_environment!
  end

  let! :root_agent do
    Fluentd::RootAgent.new
  end

  let! :parent_agent do
    a = Fluentd::Agent.new
    a.parent_agent = root_agent
    a
  end

  let! :agent do
    a = Fluentd::Agent.new
    a.parent_agent = parent_agent
    a
  end

  let :now do
    Time.now.to_i
  end

  describe '#root_agent' do
    it 'inherits root_agent from the parent agent' do
      agent.root_agent.should == root_agent
    end
  end

  describe '#agents' do
    it 'contains child agents' do
      agent.agents.should == []
      parent_agent.agents.should == [agent]
    end
  end

  describe '#parent_agent' do
    it 'points the parent agent' do
      agent.parent_agent.should == parent_agent
      parent_agent.parent_agent.should == root_agent
    end
  end

  describe '#logger' do
    it 'wraps root logger inherited from the parent agent' do
      # logger is different
      agent.logger.should_not == root_agent.logger
      # inner logger should be same
      agent.logger.instance_eval { @logger }.should ==
        root_agent.logger.instance_eval { @logger }
    end

    it 'has different log level from the parent agent' do
      parent_agent.logger.level = Fluentd::Logger::TRACE
      agent.logger.level = Fluentd::Logger::WARN
      parent_agent.logger.level.should == Fluentd::Logger::TRACE
    end
  end

  # TODO event_router is moved to HasNestedMatch
  #describe '#event_router' do
  #  it 'routes error events to root_agent.error_collector' do
  #    root_agent.error_collector.should_receive(:emit).with("tag1", now, {})
  #    agent.event_router.add_rule(MatchPattern('tag1.**'), ErrorCollector.new)
  #    agent.event_router.emit("tag1", now, {})
  #  end
  #end

  describe '#logger' do
    %w[trace debug info warn error fatal].each do |level_name|
      it "routes logs to root_agent.log_collector with 'fluentd.#{level_name}' tag" do
        agent.logger.level = :trace
        root_agent.log_collector.should_receive(:emit) do |tag,time,record|
          tag.should == "fluentd.#{level_name}"
          record.should == {'message' => "test"}
        end
        agent.logger.__send__(level_name, 'test')
      end
    end

    it 'ignores logs if log level is less than setting' do
      agent.logger.level = :warn
      root_agent.log_collector.should_receive(:emit).once
      agent.logger.debug('test')
      agent.logger.warn('test')
    end
  end
end

