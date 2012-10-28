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

    class SingleprocessManager
      def initialize
        @hm = HeartbeatManager.new
        @hm.cloexec_mode = :both

        @sm = SocketManager.new
        @sm.cloexec_mode = :both
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
        @processor.stop(immediate) if @processor

        # shutdown running processor
        @processor.join if @processor

        @processor = ChildProcess.new(self, conf, 0)

        # assign agents to processor
        assign_processor_group(agent_group, @processor)

        # start processor
        @processor.start

        nil
      end

      def stop(immediate)
        @processor.stop(immediate)
      end

      def join
        @processor.join
      end

      def run
        @sm.start

        begin
          while true
            @hm.receive(0.5)

            @processor.last_heartbeat_time = @hm.last_heartbeat_time(0)
            running = @processor.try_join(@child_kill_interval, @child_graceful_kill_limit, @child_heartbeat_limit)
            break unless running
          end

        ensure
          @sm.shutdown
        end
      end

      def fork_processor(pc, &block)
        hmc = @hm.new_client(0)
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

      def assign_processor_group(agent_group, processor)
        agent_group.agents.each {|agent|
          processor.assign_agent(agent)
        }

        agent_group.agent_groups.each {|nested_agent_group|
          assign_processor_group(nested_agent_group, processor)
        }
      end
    end
  end
end
