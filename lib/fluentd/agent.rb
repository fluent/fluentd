#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
  require 'fluentd/event_router'
  require 'fluentd/engine'
  require 'fluentd/agent_logger'
  require 'fluentd/match_pattern'
  require 'fluentd/collectors/null_collector'

  #
  # Agent is the base class of Label, input, output and filter plugins.
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
  # NextStep: 'fluentd/event_router.rb'
  # NextStep: 'fluentd/label.rb'
  #
  class Agent
    include Configurable

    def initialize
      @root_agent = nil
      @parent_agent = nil
      @agents = []  # child agents

      init_configurable  # initialize Configurable
      super

      @event_router = EventRouter.new(Collectors::NullCollector.new)
      @event_router.emit_error_handler = method(:handle_emit_error)

      # See 'fluentd/agent_logger.rb' about AgentLogger.
      @logger = AgentLogger.new(Engine.logger)
    end

    attr_reader :parent_agent

    attr_accessor :logger
    alias_method :log, :logger
    alias_method :log=, :logger=

    def collector
      @event_router
    end

    def default_collector=(collector)
      @event_router.default_collector = collector
    end

    # nested agents
    attr_reader :agents

    # RootAgent
    attr_reader :root_agent

    def configure(conf)
      super

      # initialize <match> and <filter> elements
      conf.elements.select {|e|
        e.name == 'match' || e.name == 'filter'
      }.each {|e|
        case e.name
        when 'match'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type'] || 'redirect'
          add_match(type, pattern, e)

        when 'filter'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type'] || 'redirect'
          add_filter(type, pattern, e)
        end
      }

      # <source> is initialized by Label because <source> can't
      # be nested.
    end

    def add_match(type, pattern, conf)
      log.info "adding match", pattern: pattern, type: type

      # PluginRegistry#new_output adds the created Output to @agents
      # (because Output is an Agent).
      output = Engine.plugins.new_output(self, type)
      output.configure(conf)

      @event_router.add_pattern(pattern, output)

      return output
    end

    def add_filter(type, pattern, conf)
      log.info "adding filter", pattern: pattern, type: type

      # PluginRegistry#new_filter adds the created Filter to @agents
      # (because Filter is an Agent).
      filter = Engine.plugins.new_filter(self, type)

      # overwrite default_collector to the offset, instead of the head
      filter.default_collector = @event_router.current_offset

      filter.configure(conf)

      @event_router.add_pattern(pattern, filter)

      return filter
    end

    def start
      # agent API called by Worker
    end

    def stop
      # agent API called by Worker
    end

    def shutdown
      # agent API called by Worker
    end

    def handle_emit_error(tag, time, record, error)
      log.warn "emit error", :error => e
      if @root_agent
        @root_agent.emit_error(tag, time, record)
      end
    end
    private :handle_emit_error

    def _add_agent(agent)
      @agents << agent
      self
    end

    # called by PluginRegistry
    def parent_agent=(parent)
      # take root_agent
      root_agent = parent.root_agent
      @root_agent = root_agent
      @logger.root_agent = root_agent
      parent._add_agent(self)
      @parent_agent = parent
    end
  end

end
