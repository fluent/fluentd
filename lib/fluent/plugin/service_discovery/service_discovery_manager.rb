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

require 'fluent/plugin'
require 'fluent/plugin/service_discovery'
require 'fluent/plugin/service_discovery/round_robin_balancer'
require 'fluent/plugin/service_discovery/discovery_message'

module Fluent
  module Plugin
    class ServiceDiscovery
      class ServiceDiscoveryManager
        def initialize(log:, load_balancer: nil, custom_build_method: nil)
          @log = log
          @load_balancer = load_balancer || RoundRobinBalancer.new
          @custom_build_method = custom_build_method

          @discoveries = []
          @services = {}
          @queue = Queue.new
          @static_config = true
        end

        def configure(opts, parent: nil)
          opts.each do |opt|
            sd = Fluent::Plugin.new_sd(opt[:type], parent: parent)
            sd.configure(opt[:conf])

            sd.services.each do |s|
              @services[s.discovery_id] = build_service(s)
            end
            @discoveries << sd

            if @static_config && opt[:type] != :static
              @static_config = false
            end
          end

          rebalance
        end

        def static_config?
          @static_config
        end

        def start
          @discoveries.each do |d|
            d.start(@queue)
          end
        end

        def run_once
          # Don't care race in this loop intentionally
          s = @queue.size

          if s == 0
            return
          end

          s.times do
            msg = @queue.pop

            unless msg.is_a?(Fluent::Plugin::ServiceDiscovery::DiscoveryMessage)
              @log.warn("BUG: #{msg}")
              next
            end

            begin
              handle_message(msg)
            rescue => e
              @log.error(e)
            end
          end

          rebalance
        end

        def rebalance
          @load_balancer.rebalance(services)
        end

        def select_service(&block)
          @load_balancer.select_service(&block)
        end

        def services
          @services.values
        end

        private

        def handle_message(msg)
          service = msg.service

          case msg.type
          when Fluent::Plugin::ServiceDiscovery::SERVICE_IN
            if (n = build_service(service))
              @log.info("Service in: name=#{service.name} #{service.host}:#{service.port}")
              @services[service.discovery_id] = n
            else
              raise "failed to build service in name=#{service.name} #{service.host}:#{service.port}"
            end
          when Fluent::Plugin::ServiceDiscovery::SERVICE_OUT
            s = @services.delete(service.discovery_id)
            if s
              @log.info("Service out: name=#{service.name} #{service.host}:#{service.port}")
            else
              @log.warn("Not found service: name=#{service.name} #{service.host}:#{service.port}")
            end
          else
            @log.error("BUG: unknow message type: #{msg.type}")
          end
        end

        def build_service(n)
          @custom_build_method ? @custom_build_method.call(n) : n
        end
      end
    end
  end
end
