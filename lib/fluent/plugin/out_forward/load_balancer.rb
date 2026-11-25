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

require 'fluent/plugin/output'
require 'fluent/plugin/out_forward/error'

module Fluent::Plugin
  class ForwardOutput < Output
    class LoadBalancer
      def initialize(log)
        @log = log
        @weight_array = []
        @rand_seed = Random.new.seed
        @rr = 0
        @mutex = Mutex.new
        @rebalance_mutex = Mutex.new
        @nodes = []
        @availability = {}
      end

      def select_healthy_node(routes: nil)
        error = nil

        # Don't care about the change of @weight_array's size while looping since
        # it's only used for determining the number of loops and it is not so important.
        wlen = (routes || @weight_array).size
        wlen.times do
          node = @mutex.synchronize do
            weights = routes || @weight_array
            next if weights.empty?

            r = @rr % weights.size
            @rr = (r + 1) % weights.size
            weights[r]
          end
          break unless node
          next unless node.available?

          begin
            ret = yield node
            return ret, node
          rescue
            # for load balancing during detecting crashed servers
            error = $!  # use the latest error
          end
        end

        raise error if error
        raise NoNodesAvailable, "no nodes are available"
      end

      def availability_changed?(nodes)
        availability = @mutex.synchronize { @availability }
        nodes.any? { |n| availability[n] != n.available? }
      end

      def rebuild_weight_array(nodes)
        @rebalance_mutex.synchronize do
          @nodes = nodes
          rebuild_routes(nodes)
        end
      end

      def refresh_availability
        @rebalance_mutex.synchronize { rebuild_routes(@nodes) }
      end

      def select_standby(failed_nodes, tried_standbys, &block)
        nodes = @rebalance_mutex.synchronize { @nodes }
        availability = nodes.to_h do |node|
          [node, node.available? && !failed_nodes.key?(node) && !tried_standbys.key?(node)]
        end
        _, promoted = partition_routes(nodes, availability)
        select_healthy_node(routes: weight_array_for(promoted), &block)
      rescue NoNodesAvailable
        nil
      end

      private def rebuild_routes(nodes)
        availability = nodes.to_h { |n| [n, n.available?] }
        regular_nodes, promoted = partition_routes(nodes, availability)
        promoted.each { |n| @log.warn "using standby node #{n.host}:#{n.port}", weight: n.weight }
        routes = regular_nodes + promoted
        @log.warn('No nodes are available') if routes.empty?
        weight_array = weight_array_for(routes)
        @mutex.synchronize do
          @availability = availability
          @weight_array = weight_array
        end
      end

      private def partition_routes(nodes, availability)
        standby_nodes, regular_nodes = nodes.select { |e| e.weight > 0 }.partition {|n|
          n.standby?
        }

        lost_weight = regular_nodes.sum { |node| availability[node] ? 0 : node.weight }
        @log.debug("rebuilding weight array", lost_weight: lost_weight)

        promoted = []
        standby_nodes.each do |node|
          break if lost_weight <= 0
          next unless availability[node]

          promoted << node
          lost_weight -= node.weight
        end
        [regular_nodes, promoted]
      end

      private def weight_array_for(nodes)
        return [] if nodes.empty?

        weight_array = []
        gcd = nodes.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
        nodes.each {|n|
          (n.weight / gcd).times {
            weight_array << n
          }
        }

        # for load balancing during detecting crashed servers
        coe = (nodes.size * 6) / weight_array.size
        weight_array *= coe if coe > 1

        r = Random.new(@rand_seed)
        weight_array.sort_by! { r.rand }
      end

      alias select_service select_healthy_node
      alias rebalance rebuild_weight_array
    end
  end
end
