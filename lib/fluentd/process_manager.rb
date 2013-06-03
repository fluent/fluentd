#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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

  #
  # ProcessManager assigns agents to a processor.
  # See agent.rb for details.
  #
  # MultiProcessManager is a subclass of ProcessManager that
  # assigns creates multiple processors.
  #
  # Processor is an UNIX process managed by ServerEngine. See also server.rb.
  #
  class ProcessManager
    def initialize
      # use single-process SocketManager
      @socket_manager = SocketManager::API.new
    end

    attr_accessor :logger

    attr_reader :socket_manager
    attr_reader :processors

    def configure(config)
      @config = config
    end

    attr_reader :config

    def assign_processors(root_agent)
      pc = Processor.new

      assign(pc, root_agent)

      @processors = [pc]
    end

    def assign(pc, agent)
      pc.assign_agent(agent)

      agent.agents.each {|a|
        assign(pc, a)
      }
    end

    def before_run
    end

    def after_run
    end

    def after_start
    end

    class Processor
      def initialize
        @agents = []
        @started_agents = []
        @stop_flag = ServerEngine::BlockingFlag.new
      end

      def assign_agent(agent)
        @agents << agent
        agent.processors << self
      end

      def worker_initialize(pm, worker_id)
        @log = pm.config.log
        @process_manager = pm
        @worker_id = worker_id
      end

      attr_reader :agents, :worker_id

      attr_reader :process_manager

      def before_fork
      end

      def run
        @log.info "Starting worker #{@worker_id}"

        sources = []
        outputs = []

        @agents.each {|a|
          if a.is_a?(MessageSource)
            sources << a
          else
            outputs << a
          end
        }

        # start outputs first
        outputs.each {|a|
          a.start
          @started_agents << a
        }

        # start inputs
        sources.each {|a|
          a.start
          @started_agents << a
        }

        @stop_flag.wait
        nil

      ensure
        @log.info "Shutting down worker #{@worker_id}"

        # stop first in reversed order
        @started_agents.reverse.map {|s|
          ensured_thread { s.stop }
        }.each {|t| t.join }

        # shutdown in reversed order
        @started_agents.reverse.map {|s|
          ensured_thread { s.stop }
        }.each {|t| t.join }
      end

      def stop
        @stop_flag.set!
      end

      def reload
      end

      def after_start
      end

      private

      def ensured_thread(&block)
        Thread.new do
          begin
            block.call
          rescue
            @log.warn "unexpected error while shutting down", :error=>$!.to_s
            @log.warn_backtrace
          end
        end
      end
    end
  end

end
