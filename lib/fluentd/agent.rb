#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluentd

  require 'fluentd/configurable'
  require 'fluentd/engine'
  require 'fluentd/agent_logger'

  #
  # Agent is the base class of Label, input and output plugins.
  #
  # Agent has:
  #
  #   * A parent Agent, eventually reaches the RootAgent
  #   * Nested Agent instances, whose parent is this Agent
  #
  # Agent can:
  #
  #   * send events to other OutputAgents
  #   * forward events to other OutputAgents (through EventRouter)
  #
  # Agent can't receive events (because Agent is not Collector).
  # EventRouter and OutputAgents are Collector and can receive events.
  #
  # Next step: `fluentd/root_agent.rb`
  #
  class Agent
    include Configurable

    def initialize
      @root_agent = nil
      @parent_agent = nil
      @agents = []  # child agents

      # See 'fluentd/agent_logger.rb' about AgentLogger.
      @logger = Engine.logger ? AgentLogger.new(Engine.logger) : nil

      init_configurable  # initialize Configurable
    end

    attr_reader :parent_agent

    attr_accessor :logger
    alias_method :log, :logger
    alias_method :log=, :logger=

    # child agents
    attr_reader :agents

    # RootAgent
    attr_reader :root_agent

    config_param :log_level, :string, default: nil

    def configure(conf)
      # calls Configurable#configure
      super

      if @log_level
        @logger.level = @log_level
      end
    end

    def start
      # agent API called by Worker
    end

    def shutdown
      # agent API called by Worker
    end

    def close
      # agent API called by Worker
    end

    def _add_agent(agent)
      @agents << agent
      self
    end

    # called by PluginRegistry
    def parent_agent=(parent)
      self.root_agent = parent.root_agent
      parent._add_agent(self)
      @parent_agent = parent
    end

    def root_agent=(root_agent)
      @root_agent = root_agent
      @logger.root_agent = root_agent
    end
  end

end
