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
        @availability = {}
        # The weight array is rebuilt from several threads: the event loop
        # (heartbeat timer and service discovery), the ack thread after a
        # failed ack and, with heartbeat_type none, the flush threads. Each
        # rebuild reads the node list and publishes its array while holding
        # this lock, so an older rebuild cannot publish its array after a
        # newer one (#5627).
        @rebuild_mutex = Mutex.new
        @nodes_provider = nil
      end

      # Returns the current node list. When it is set, every rebuild reads the
      # node list from it under the rebuild lock instead of using the list the
      # caller read before.
      attr_writer :nodes_provider

      def select_healthy_node
        error = nil

        # Don't care about the change of @weight_array's size while looping since
        # it's only used for determining the number of loops and it is not so important.
        wlen = @weight_array.size
        wlen.times do
          node = @mutex.synchronize do
            r = @rr % @weight_array.size
            @rr = (r + 1) % @weight_array.size
            @weight_array[r]
          end
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

      def rebuild_weight_array(nodes = nil)
        @rebuild_mutex.synchronize do
          nodes = @nodes_provider.call if @nodes_provider
          rebuild_weight_array_unlocked(nodes)
        end
      end

      alias select_service select_healthy_node
      alias rebalance rebuild_weight_array

      private

      def rebuild_weight_array_unlocked(nodes)
        availability = nodes.to_h { |n| [n, n.available?] }
        standby_nodes, regular_nodes = nodes.select { |e| e.weight > 0 }.partition {|n|
          n.standby?
        }

        lost_weight = 0
        regular_nodes.each {|n|
          unless availability[n]
            lost_weight += n.weight
          end
        }
        @log.debug("rebuilding weight array", lost_weight: lost_weight)

        if lost_weight > 0
          standby_nodes.each {|n|
            if availability[n]
              regular_nodes << n
              @log.warn "using standby node #{n.host}:#{n.port}", weight: n.weight
              lost_weight -= n.weight
              break if lost_weight <= 0
            end
          }
        end

        weight_array = []
        if regular_nodes.empty?
          @log.warn('No nodes are available')
          @mutex.synchronize do
            @availability = availability
            @weight_array = weight_array
          end
          return @weight_array
        end

        gcd = regular_nodes.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
        regular_nodes.each {|n|
          (n.weight / gcd).times {
            weight_array << n
          }
        }

        # for load balancing during detecting crashed servers
        coe = (regular_nodes.size * 6) / weight_array.size
        weight_array *= coe if coe > 1

        r = Random.new(@rand_seed)
        weight_array.sort_by! { r.rand }

        @mutex.synchronize do
          @availability = availability
          @weight_array = weight_array
        end
      end
    end
  end
end
