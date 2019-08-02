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
      def initialize(load_balancer:, services:, log:)
        @log = log
        @load_balancer = load_balancer
        @services = services

        rebalance
      end

      def rebalance
        @load_balancer.rebuild_weight_array(@services)
      end

      def select_node(&block)
        @load_balancer.select_healthy_node(&block)
      end

      def services
        @services.dup
      end
    end
  end
end
