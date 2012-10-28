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

    class SimpleProcessManager
      def initialize
        @agents = []
        @started_agents = []
        @finish_flag = BlockingFlag.new
        @finish_complete_flag = BlockingFlag.new
      end

      def restart(immediate, agent_group, conf)
        unless @agents.empty?
          raise "SimpleProcessManager does not support restart"
        end

        collect_agents(agent_group, @agents)

        @agents.each {|agent|
          agent.start
          @started_agents << agent
        }
      end

      def run
        until @finish_flag.set?
          @finish_flag.wait(0.5)
        end
      end

      def stop(immediate)
        @finish_flag.set!

        @started_agents.each {|agent|
          agent.stop
        }
        @started_agents.clear

        @agents.each {|agent|
          agent.shutdown
        }

        @finish_complete_flag.set!
      end

      def join
        until @finish_complete_flag.set?
          @finish_complete_flag.wait(0.5)
        end
      end

      def logrotate
        # do nothing
      end

      private

      def collect_agents(agent_group, array=[])
        array.concat agent_group.agents
        agent_group.agent_groups.each {|nested_agent_group|
          collect_agents(nested_agent_group, array)
        }
        return array
      end
    end

  end
end

