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
        @sm = SocketManager.new
        @sm.cloexec_mode = :server
        @processor = ChildProcess.new(self, 0)
      end

      def restart(immediate, agent_group, conf)
        # send stop signals
        @processor.stop(immediate)

        # shutdown running processor
        @processor.join

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
            running = @processor.try_join(2, 10)  # TODO kill_interval, graceful_kill_limit
            break unless running
          end

        ensure
          @sm.shutdown
        end
      end

      def fork_processor(pc, &block)
        smc = @sm.new_client
        pid = fork do
          begin
            block.call(smc)
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
