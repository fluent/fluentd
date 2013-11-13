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

      @event_router = EventRouter.new(Collectors::NullCollector.new, self)

      # See 'fluentd/agent_logger.rb' about AgentLogger.
      @logger = Engine.logger ? AgentLogger.new(Engine.logger) : nil
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

    config_param :log_level, :string, default: nil

    def configure(conf)
      super

      if @log_level
        @logger.level = @log_level
      end

      # initialize <match>, <copy> and <filter> elements
      # TODO Either of <copy> or <filter> will be removed.
      #      See https://github.com/fluent/fluentd/wiki/V11-filter-syntax
      conf.elements.select {|e|
        e.name == 'match' || e.name == 'copy' || e.name == 'filter'
      }.each {|e|
        case e.name
        when 'match'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type']
          add_match(type, pattern, e)

        when 'copy'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type']
          add_copy(type, pattern, e)

        when 'filter'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type']
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

      @event_router.add_rule(pattern, output, copy: false)
      return output
    end

    def add_copy(type, pattern, conf)
      log.info "adding copy", pattern: pattern, type: type

      # PluginRegistry#new_output adds the created Output to @agents
      # (because Output is an Agent).
      output = Engine.plugins.new_output(self, type)
      output.configure(conf)

      @event_router.add_rule(pattern, output, copy: true)
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

      @event_router.add_rule(pattern, filter)

      return filter
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

    # EmitErrorHandler interface. See 'fluentd/event_router.rb'
    def handle_emit_error(tag, time, record, error)
      log.warn "emit error", error: error
      log.debug_backtrace error.backtrace if error.respond_to?(:backtrace)
      if @root_agent
        @root_agent.error_collector.emit(tag, time, record)
      end
    end

    # EmitErrorHandler interface. See 'fluentd/event_router.rb'
    def handle_emits_error(tag, es, error)
      log.warn "emit error", error: error
      if @root_agent
        @root_agent.error_collector.emits(tag, es)
      end
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
