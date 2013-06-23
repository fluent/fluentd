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

  #
  # MessageSource module provides #collector method to an Agent.
  # Fluentd::Input includes MessageSource.
  #
  # MessageSource manages input, output and filter plugins.
  #
  # Input plugin (#add_source):
  #   MessageSource sets up input plugins to send events into the internal MessageRouter.
  #   MessageRouter routes the events into registered output and/or filter plugins.
  #
  # Output plugin (#add_match):
  #   MessageRouter creates output plugins and registers them into MessageRouter.
  #
  # Filter plugin (#add_filter):
  #   MessageRouter creates output plugins and registers them into MessageRouter.
  #   And also sets it up to send events into the internal MessageRouter with offset.
  #
  # MessageRouter routes events into into default_collector if no output plugins matched.
  # See also message_router.rb.
  #
  module MessageSource
    include Configurable

    # call this method in subclass
    def init_message_source(root_router, default_collector)
      @root_router = root_router

      @message_router = MessageRouter.new(default_collector)

      nil
    end

    # message source API
    def collector
      @message_router
    end

    # routes events into a label instead of the internal MessageRouter
    def label_redirect(label)
      @message_router.default_collector = Collectors::LabelCollector.new(@root_router, label)
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
      log.info "adding source"#, :type=>type

      agent = Fluentd.plugin.new_input(type)
      configure_agent(agent, conf)

      return agent
    end

    def add_match(type, pattern, conf)
      log.info "adding match"#, :pattern=>pattern, :type=>type

      agent = Fluentd.plugin.new_output(type)
      configure_agent(agent, conf)

      @message_router.add_collector(pattern, agent)  # register to MessageRouter

      return agent
    end

    def add_filter(type, pattern, conf)
      log.info "adding filter"#, :pattern=>pattern, :type=>type

      agent = Fluentd.plugin.new_filter(type)
      configure_agent_offset(agent, conf)

      @message_router.add_collector(pattern, agent)  # register to MessageRouter

      return agent
    end

    private

    def configure_agent_offset(agent, conf)
      configure_agent_impl(agent, conf, @message_router.current_offset)
    end

    def configure_agent(agent, conf)
      configure_agent_impl(agent, conf, @message_router)
    end

    def configure_agent_impl(agent, conf, default_collector)
      if agent.is_a?(MessageSource)
        # nested MessageSource.
        # inherit @root_router and default_collector (RootAgent#initialize is the root to set @root_router and default_collector)
        agent.init_message_source(@root_router, default_collector)
      end

      agent.configure(conf)
      add_agent(agent)
    end
  end
end
