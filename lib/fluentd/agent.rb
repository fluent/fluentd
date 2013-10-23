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

  require_relative 'configurable'
  require_relative 'event_router'
  require_relative 'engine'
  require_relative 'match_pattern'
  require_relative 'collectors/null_collector'

  #
  # Agent is a base module of input and filter plugins.
  # Agent has:
  #
  #   * A parent Agent, eventually reaches the root agent
  #   * Nested Agent instances, whose parent is this Agent
  #   * A EventRouter provided through `collector` attribute to route events
  #
  class Agent
    include Configurable

    def initialize
      @agents = []
      init_configurable  # initialize Configurable
      super
      @event_router = EventRouter.new(Collectors::NullCollector.new)
    end

    def collector
      @event_router
    end

    def default_collector=(collector)
      @event_router.default_collector = collector
    end

    # nested agents
    attr_reader :agents

    attr_reader :root_agent

    def configure(conf)
      super

      conf.elements.select {|e|
        e.name == 'source' || e.name == 'match' || e.name == 'filter'
      }.each {|e|
        case e.name
        when 'source'
          type = e['type']
          raise ConfigError, "Missing 'type' parameter" unless type
          add_source(type, e)

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
    end

    def add_source(type, conf)
      log.info "adding source", :type=>type

      agent = Engine.plugins.new_input(self, type)
      agent.configure(conf)
      agent.default_collector = @event_router

      # <source> does not match pattern; don't register to EventRouter

      return agent
    end

    def add_match(type, pattern, conf)
      log.info "adding match", :pattern=>pattern, :type=>type

      agent = Engine.plugins.new_output(self, type)
      agent.configure(conf)
      agent.default_collector = @event_router

      @event_router.add_collector(pattern, agent)  # register to EventRouter

      return agent
    end

    def add_filter(type, pattern, conf)
      log.info "adding filter", :pattern=>pattern, :type=>type

      agent = Engine.plugins.new_filter(self, type)
      agent.configure(conf)
      agent.default_collector = @event_router.current_offset

      @event_router.add_collector(pattern, agent)  # register to EventRouter

      return agent
    end

    def start
      # plugin API
    end

    def stop
      # plugin API
    end

    def shutdown
      # plugin API
    end

    # internal use
    def parent_agent=(parent)
      @root_agent = parent.root_agent
      parent._add_agent(self)
      @parent_agent = parent
    end

    attr_reader :parent_agent

    def _add_agent(agent)
      @agents << agent
      self
    end
  end

end
