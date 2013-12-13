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
  module Plugin

    require 'fluentd/agent'
    require 'fluentd/actor'
    require 'fluentd/engine'
    require 'fluentd/collectors/label_collector'

    class Input < Agent
      attr_accessor :event_router

      # provides #actor
      include Actor::AgentMixin

      config_param :to_label, :string, default: nil

      def configure(conf)
        super

        if @to_label
          # overwrites @event_router to point a label's event_router
          # instead of top-level event_router (RootAgent#event_router)
          label = root_agent.find_label(@to_label)
          unless label
            # TODO error or warning?
            raise ConfigError, "Label #{@to_label} does not exist"
          end
          self.event_router = label.event_router
        end
      end
    end

  end
end
