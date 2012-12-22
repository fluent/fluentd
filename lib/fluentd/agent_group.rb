#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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


  module AgentGroup
    # TODO
    #include Configurable

    def initialize
      @agents = []
      @agent_groups = []
      @default_process_groups = []
      super
    end

    attr_reader :agents, :agent_groups
    attr_reader :default_process_groups

    def configure(conf)
      # TODO default_process_group
    end

    def add_agent(agent)
      @agents << agent
      if agent.is_a?(AgentGroup)
        add_agent_group(agent)
      end
      self
    end

    def add_agent_group(group)
      @agent_groups << group
      self
    end

    def configure_agent(agent, conf, parent_bus)
      add_agent(agent)

      if agent.is_a?(StreamSource)
        bus = MessageBus.new(parent_bus)
        add_agent_group(bus)

        bus.configure(conf)
        bus.default_collector = parent_bus

        agent.setup_internal_bus(bus)
      end

      agent.configure(conf)
      agent = nil

    ensure
      agent.shutdown if agent  # TODO
    end

    # Processors call Agent#start
    #def start
    #  @agents.each {|agent| agent.start }
    #  @agent_groups.each {|group| group.start }
    #end

    # Processors call Agent#shutdown
    #def shutdown
    #  @agents.each {|agent| agent.shutdown }
    #  @agent_groups.each {|group| group.shutdown }
    #end
  end


end
