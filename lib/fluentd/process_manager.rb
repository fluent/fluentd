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

  class ProcessManager
    def initialize
      @sm = SocketManager.new
      @sm.cloexec_mode = :server

      @processors = {}
      @exchange_servers = []
    end

    def restart(immediate, agent_group, conf)
      @processor_factory = Processors.new_processor_factory(conf)

      agents = agent_group.collect_agents

      # send stop signals
      @processors.values.each {|pc| pc.stop(immediate) }
      @exchange_servers.each {|es| es.stop }

      # shutdown running processors
      @processors.values.each {|pc| pc.join(immediate) }
      @processors.clear

      # shutdown exchange servers
      @exchange_servers.each {|es| es.shutdown }

      # initialize new processors
      @processors = { }

      # assign agents to processors
      assign_processor_group(agent_group, @processors, ['default'])

      # listen exchange servers
      @processors.values.each {|pc|
        @exchange_servers[pc.processor_id] = pc.setup!
      }

      @processors.values.each {|pc|
        pc.start
      }

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
      @sm.shutdown
    end

    def run
      @sm.start

      while true
        sleep 0.5  # TODO

        has_running_process = false
        @processors.values.each {|pc|
          # TODO heartbeat
          if pc.try_join(2, 10)  # TODO kill_interval, graceful_kill_limit
            has_running_process = true
          end
        }

        unless has_running_process
          break
        end
      end

    rescue
      puts $!
      $!.backtrace.each {|bt| puts bt }
    end

    def fork_processor(pc, &block)
      # open clients for all other remote servers
      exchange_clients = []
      begin
        @exchange_servers.each_with_index {|es,pcid|
          if es && pcid != pc.processor_id
            exchange_clients[pcid] = es.new_client
          end
        }

        smc = @sm.new_client
        pid = fork do
          begin
            # close other servers
            @exchange_servers.each_with_index {|es,pcid|
              if es && pcid != pc.processor_id
                es.close
              end
            }
            block.call(exchange_clients, smc)
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
      ensure
        exchange_clients.each {|ec|
          ec.close if ec
        }
      end

      pid
    end

    private
    def assign_processor_group(agent_group, processors, default_groups)
      agent_group.agents.each {|agent|
        assign_processor(agent, processors, default_groups)
      }

      groups = agent_group.default_process_groups
      groups = default_groups if groups.empty?

      agent_group.agent_groups.each {|nested_agent_group|
        assign_processor_group(nested_agent_group, processors, default_groups)
      }
    end

    def assign_processor(agent, processors, default_groups)
      groups = agent.process_groups
      groups = default_groups if groups.empty?
      pcs = groups.map {|name|
        processors[name] ||= @processor_factory.new(self, processors.size)
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

