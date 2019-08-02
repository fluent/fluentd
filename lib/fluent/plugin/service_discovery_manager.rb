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

module Fluent
  module Plugin
    class ServiceDiscoveryManager
      def initialize(load_balancer:, log:, static:, custom_build_method: nil)
        @log = log
        @load_balancer = load_balancer
        @custom_build_method = custom_build_method
        @services = {}
        @static = static

        @queue = Queue.new
      end

      def configure
        @static.each do |s|
          # TODO
          service = s.service
          @services[service.discovery_id] = build_node(service)
        end

        rebalance
      end

      def rebalance
        @load_balancer.rebuild_weight_array(services)
      end

      def select_node(&block)
        @load_balancer.select_healthy_node(&block)
      end

      def services
        @services.values
      end

      private

      def build_node(n)
        @custom_build_method ? @custom_build_method.call(n) : n
      end
    end
  end
end
