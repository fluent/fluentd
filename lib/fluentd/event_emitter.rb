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
  require_relative 'config_error'
  require_relative 'event_router'
  require_relative 'match_pattern'
  require_relative 'engine'
  require_relative 'collectors/label_redirect_collector'

  #
  # EventEmitter is a base module of input and filter plugins.
  # EventEmitter has the capability to generates events into fluentd's message router.
  #
  # EventEmitter has:
  #
  #   * a parent EventEmitter, which is the default_collector of this EventEmitter
  #   * nested EventEmitter instances, whose parent is this EventEmitter
  #
  #             +--------------+
  #             | EventEmitter |       --- parent EventEmitter
  #             |     ...      |           input, filter plugin or RootAgent
  #             +--------------+
  #                    ^
  #                    | default_collector
  #                    |
  #         +--------- | ---------+
  #        ||     EventEmitter    ||
  #        ||          |          ||   An agent instance
  #        ||    <EventRouter>    ||
  #        ||      /       \      ||
  #        ||   Agent     Agent   ||   --- <match> or <filter>
  #         +---------------------+        output or filter plugins
  #                ^       ^
  #                |       | default_collector
  #               /         \
  #  +-----------/---+   +---\-----------+
  #  | EventEmitter |   | EventEmitter |  --- nested <source>
  #  |      ...      |   |      ...      |      input plugins
  #  +---------------+   +---------------+
  #
  # When EventEmitter generates an event:
  #
  #   1. EventEmitter sends it to the internal EventRouter
  #   2. The EventRouter tries to match it with registered output (or filter) plugins
  #   If no plugins match the event,
  #   3. it sends the event to default_collector (= parent EventEmitter)
  #      The parent EventEmitter does the same thing; tries to match output (or filter)
  #      plugins in the the internal EventRouter, or sends it to the default_collector.
  #
  # The root node of this routing tree is RootAgent. RootAgent sends the event to
  # NoMatchNoticeCollector if no output (or filter) plugins match.
  #
  module EventEmitter
    include Configurable

    # call this method in subclass
    def init_event_emitter(root_agent, default_collector)
      @root_agent = root_agent
      @event_router = EventRouter.new(default_collector)
      nil
    end

    # message source API
    def collector
      @event_router
    end

    # routes events into a label instead of the internal EventRouter
    def label_redirect(label)
      @event_router.default_collector = Collectors::LabelRedirectCollector.new(@root_agent, label)
      nil
    end

    # override Agent#configure
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
      configure_agent(agent, conf)

      # <source> does not match pattern; don't register to EventRouter

      return agent
    end

    def add_match(type, pattern, conf)
      log.info "adding match", :pattern=>pattern, :type=>type

      agent = Engine.plugins.new_output(self, type)
      configure_agent(agent, conf)

      @event_router.add_collector(pattern, agent)  # register to EventRouter

      return agent
    end

    def add_filter(type, pattern, conf)
      log.info "adding filter", :pattern=>pattern, :type=>type

      agent = Engine.plugins.new_filter(self, type)
      configure_agent_with_offset(agent, conf)

      @event_router.add_collector(pattern, agent)  # register to EventRouter

      return agent
    end

    private

    def configure_agent_with_offset(agent, conf)
      configure_agent_impl(agent, conf, @event_router.current_offset)
    end

    def configure_agent(agent, conf)
      configure_agent_impl(agent, conf, @event_router)
    end

    def configure_agent_impl(agent, conf, default_collector)
      if agent.is_a?(EventEmitter)
        # this agent is a nested EventEmitter.
        # setup the default_collector of the agent here
        agent.init_event_emitter(@root_agent, default_collector)
      end

      agent.configure(conf)
    end
  end
end
