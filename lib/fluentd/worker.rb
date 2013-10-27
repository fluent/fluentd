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

  require 'sigdump'

  require 'fluentd/engine'
  require 'fluentd/root_agent'
  require 'fluentd/plugin_registry'

  #
  # Worker corresponds to a <worker> element in a configuration file.
  #
  # Worker is resonsible to:
  #
  # 1) configure RootAgent
  # 2) collect all agents from the RootAgent
  # 3) start, stop, and monitor agents
  #
  # NextStep: 'fluentd/root_agent.rb'
  #
  class Worker
    include ServerEngine::ConfigLoader

    def initialize(options)
      @stop_flag = ServerEngine::BlockingFlag.new
      super(options)  # initialize ServerEngine::ConfigLoader
      @log = create_logger  # ServerEngine::ConfigLoader#create_logger

      # initialize Engine
      Engine.plugins = PluginRegistry.new
      Engine.logger = @log
      Engine.sockets = SocketManager::NonManagedAPI.new
      # TODO socket manager is not used yet
    end

    def configure(conf)
      @worker_id = conf['id']

      # Worker has a RootAgent
      root_agent = RootAgent.new
      root_agent.configure(conf)

      # warn unused config parameters
      conf.check_not_used {|key,e|
        @log.warn "parameter '#{key}' is not used in\n#{e.to_s(nil).strip}"
      }

      # collect all agents recursively
      @agents = collect_agents(root_agent)

      Engine.root_agent = root_agent
    end

    def run
      started_agents = []

      @agents.each {|a|
        a.start
        started_agents << a
      }

      @stop_flag.wait
      nil

    ensure
      @log.info "Shutting down worker #{@worker_id}"

      # call Agent#stop in reversed order
      started_agents.reverse.map {|a|
        Thread.new do
          begin
            a.stop
          rescue => e
            @log.warn "unexpected error while stopping down agent #{a}", error: e.to_s
            @log.warn_backtrace
          end
        end
      }.each {|t| t.join }

      # call Agent#shutdown in reversed order
      started_agents.reverse.map {|a|
        Thread.new do
          begin
            a.shutdown
          rescue => e
            @log.warn "unexpected error while shutting down agent #{a}", error: e.to_s
            @log.warn_backtrace
          end
        end
      }.each {|t| t.join }
    end

    def stop
      @stop_flag.set!
    end

    private

    def collect_agents(agent, array=[])
      agent.agents.each {|a|
        collect_agents(a, array)
      }
      array << agent
      return array
    end
  end

end
