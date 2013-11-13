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

  require 'fluentd/agent'
  require 'fluentd/collector'
  require 'fluentd/collectors/label_collector'

  #
  # Label is an Agent with nested <source> elements.
  #
  # Label can't receive events, where it's same as Agent.
  #
  # Nested <source> elements are Input plugins. They can send events. The events
  # match the nested <match> elements in the <source> first. But if no nested
  # <match> elements match, the events go to the EventRouter in this Label,
  # because default_collector of the <source> is overwriten at #add_source.
  #
  # See the Agent defined in 'fluentd/plugin/agent.rb' for EventRouter.
  #
  class Label < Agent
    def configure(conf)
      # Agent initializes <match> and <filter>
      super

      # initialize <source> elements
      conf.elements.select {|e|
        e.name == 'source'
      }.each {|e|
        case e.name
        when 'source'
          type = e['type']
          raise ConfigError, "Missing 'type' parameter" unless type
          add_source(type, e)
        end
      }

      # initialize <match> and <filter> are initialized by Agent
      # because they can be nested while <source> can't be nested.
    end

    def add_source(type, conf)
      log.info "adding source", type: type

      # PluginRegistry#new_input overwrites default_collector of
      # the created Input to @event_router of this Label
      # (@event_router is initialized at Agent#initialize).
      agent = Engine.plugins.new_input(self, type)
      agent.configure(conf)

      if self.is_a?(RootAgent)
        # TODO Label#configure and #add_source will be moved to RootAgent,
        #      or to_label option will be removed.
        if label = conf['to_label']
          agent.default_collector = Collectors::LabelCollector.new(root_agent, label)
        end
      end

      return agent
    end
  end

end
