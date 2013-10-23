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

  require_relative 'engine'
  require_relative 'root_agent'
  require_relative 'plugin_registry'

  #
  # Worker corresponds to a <worker> section in a configuration file.
  #
  # Worker has a RootAgent, and starts/stops all nested agents owned
  # by the RootAgent.
  #
  class Worker
    include ServerEngine::ConfigLoader

    def initialize(options)
      @stop_flag = ServerEngine::BlockingFlag.new
      super(options)  # initialize ServerEngine::ConfigLoader
      @log = create_logger  # ServerEngine::ConfigLoader#create_logger

      Engine.plugins = PluginRegistry.new
      Engine.logger = @log
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

      # gather all nested agents recursively
      @agents = gather_agents(root_agent)
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
            @log.warn "unexpected error while stopping down agent #{a}", :error=>$!.to_s
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
            @log.warn "unexpected error while shutting down agent #{a}", :error=>$!.to_s
            @log.warn_backtrace
          end
        end
      }.each {|t| t.join }
    end

    def stop
      @stop_flag.set!
    end

    private

    def gather_agents(agent, array=[])
      agent.agents.each {|a|
        gather_agents(a, array)
      }
      array << agent
      return array
    end
  end

end
