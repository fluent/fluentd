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

  describe '#parent_agent' do
    it { root_agent.parent_agent.should == nil }
  end

  describe '#root_agent' do
    it { root_agent.root_agent.should == root_agent }
  end

  # TODO
end

