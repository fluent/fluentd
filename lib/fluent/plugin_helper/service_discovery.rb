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

require 'fluent/plugin_helper/timer'
require 'fluent/plugin_helper/service_discovery/manager'

module Fluent
  module PluginHelper
    module ServiceDiscovery
      include Fluent::PluginHelper::Timer

      def start
        unless @discovery_manager
          log.warn('There is no discovery_manager. skip start them')
          super
          return
        end

        @discovery_manager.start
        unless @discovery_manager.static_config?
          timer_execute(@_plugin_helper_service_discovery_title, @_plugin_helper_service_discovery_iterval) do
            @discovery_manager.run_once
          end
        end

        super
      end

      private

      # @param title [Symbol] the thread name. this value should be unique.
      # @param configurations [Hash] hash which must has discivery_service type and its configuration like `{ type: :static, conf: <Fluent::Config::Element> }`
      # @param load_balancer [Object] object which has two methods #rebalance and #select_service
      # @param custom_build_method [Proc]
      def create_service_discovery_manager(title, configurations:, load_balancer: nil, custom_build_method: nil, interval: 3)
        @_plugin_helper_service_discovery_title = title
        @_plugin_helper_service_discovery_iterval = interval

        @discovery_manager = Fluent::PluginHelper::ServiceDiscovery::Manager.new(
          log: log,
          load_balancer: load_balancer,
          custom_build_method: custom_build_method,
        )

        @discovery_manager.configure(configurations, parent: self)

        @discovery_manager
      end

      def discovery_manager
        @discovery_manager
      end
    end
  end
end
