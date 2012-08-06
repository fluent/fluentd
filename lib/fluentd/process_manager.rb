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

  # TODO multi-process is not implemented yet

  class ProcessManager
    def initialize
      @finish_flag = BlockingFlag.new
    end

    def reset(agent_group, conf)
      processor = Processor.new(0)

      # assign agents to processors
      agents = agent_group.collect_agents
      agents.each {|agent|
        processor.assign_agent(agent)
      }

      # setup collector proxies
      processor.setup_collector_proxy!

      @processor = processor
    end

    def run
      @processor.start
      until @finish_flag.set?
        sleep 1
      end
    end

    def stop(immediate)
      @processor.stop(immediate)
      @finish_flag.set!
    end

    def join
      @processor.join
    end

    def shutdown
      @processor.shutdown
    end

    private
  end


  class Processor
    def initialize(processor_id)
      @processor_id = processor_id
      @agents = []

      @started_agents = []
    end

    def assign_agent(agent)
      @agents << agent
    end

    def setup_collector_proxy!
      @agents.select {|agent|
        agent.is_a?(Collector)
      }.each {|c|
        c.extend(CollectorProxy)
        c.setup_collector_proxy!(@processor_id)
      }
    end

    def start
      # TODO do this in child process
      Processor.const_set(:PROCESSOR_ID, @processor_id)

      @agents.each {|agent|
        agent.start
        @started_agents << agent
      }
    end

    def stop(immediate)
      @started_agents.each {|agent|
        agent.stop
      }
    end

    def shutdown
      @agents.each {|agent|
        agent.shutdown
      }
    end

    def join
    end

    module CollectorProxy
      def setup_collector_proxy!(processor_id)
        @processor_id = processor_id
      end

      def open
        if Processor::PROCESSOR_ID == @processor_id
          return super
        end

        # TODO proxy
        super
      end
    end

  end

end

