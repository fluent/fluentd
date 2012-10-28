#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
  module ProcessManager

    class MultiprocessManager
      def initialize
        @hm = HeartbeatManager.new
        @hm.cloexec_mode = :both

        @sm = SocketManager.new
        @sm.cloexec_mode = :both

        @processors = {}
        @ipxs = []
      end

      def configure(conf)
        # TODO
        @child_kill_interval = 2
        @child_graceful_kill_limit = 10
        @child_heartbeat_limit = 10
      end

      def restart(immediate, agent_group, conf)
        configure(conf)

        # send stop signals
        @processors.values.each {|pc| pc.stop(immediate) }

        # shutdown running processors
        @processors.values.each {|pc| pc.join }
        @processors.clear

        # close exchange servers
        @ipxs.each {|es| es.close }

        # initialize new processors
        @processors = { }

        # assign agents to processors
        assign_processor_group(agent_group, @processors, ['default'], conf)

        # listen exchange servers
        @processors.values.each {|pc|
          @ipxs[pc.processor_id] = pc.init_exchange
        }

        # start processors
        @processors.values.each {|pc| pc.start }

        nil
      end

      def stop(immediate)
        @processors.values.each {|pc|
          pc.stop(immediate)
        }
      end

      def join
        @processors.values.each {|pc|
          pc.join
        }
      end

      def run
        @sm.start

        begin
          while true
            @hm.receive(0.5)

            has_running = maintain_processors
            break unless has_running
          end

        ensure
          @sm.shutdown
        end
      end

      def fork_processor(pc, &block)
        hmc = @hm.new_client(pc.processor_id)
        smc = @sm.new_client

        pid = fork do
          @sm.close
          @hm.close

          begin
            block.call(hmc, smc)
          rescue
            # TODO log
            puts $!
            $!.backtrace.each {|bt| puts bt }
            raise
          ensure
            exit! 127
          end
        end

        smc.close
        hmc.close

        pid
      end

      private

      def maintain_processors
        has_running = false

        @processors.values.each {|pc|
          pc.last_heartbeat_time = @hm.last_heartbeat_time(pc.processor_id) || 0  # use 0 if the child is dead

          if pc.try_join(@child_kill_interval, @child_graceful_kill_limit, @child_heartbeat_limit)
            has_running = true
          end
        }

        return has_running
      end

      def assign_processor_group(agent_group, processors, default_groups, conf)
        groups = agent_group.default_process_groups
        groups = default_groups if groups.empty?

        agent_group.agents.each {|agent|
          assign_processor(agent, processors, groups, conf)
        }

        agent_group.agent_groups.each {|nested_agent_group|
          assign_processor_group(nested_agent_group, processors, groups, conf)
        }
      end

      def assign_processor(agent, processors, default_groups, conf)
        groups = agent.process_groups
        groups = default_groups if groups.empty?
        pcs = groups.map {|name|
          processors[name] ||= ChildProcess.new(self, conf, processors.size)
        }
        if pcs.size == 1
          assign_single_processor(pcs[0], agent)
        else
          assign_multiple_processors(pcs, agent)
        end
      end

      def assign_single_processor(pc, agent)
        if agent.is_a?(Collector) && !agent.is_a?(CollectorProxy)
          agent.extend(CollectorProxy)
        end
        pc.assign_agent(agent)
        if agent.is_a?(CollectorProxy)
          agent.setup_collector_proxy!(pc)
        end
      end

      def assign_multiple_processors(pcs, agent)
        if agent.is_a?(Collector) && !agent.is_a?(DistributeCollectorProxy)
          agent.extend(DistributeCollectorProxy)
        end
        pcs.each {|pc|
          pc.assign_agent(agent)
        }
        if agent.is_a?(CollectorProxy)
          agent.setup_collector_proxy!(pcs)
        end
      end

      module CollectorProxy
        def setup_collector_proxy!(processor)
          # this processor runs this agent
          @processor = processor
        end

        def open(tag)
          if @processor.local_process?
            return super
          end

          @processor.open_remote_self(self, tag)
        end
      end

      module DistributeCollectorProxy
        def setup_collector_proxy(processors)
          # these processors run this agent
          @processors = processors
          @processor_rr = 0
        end

        def open(tag)
          if @processors.find_if {|pc| pc.local_process? }
            return super
          end

          rr = @processor_rr = (@processor_rr + 1) % @processors.size
          pc = @processors[rr]

          pc.open_remote_self(self, tag)
        end
      end
    end

  end
end
