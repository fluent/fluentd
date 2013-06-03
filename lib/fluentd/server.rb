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

  module Server
    extend Forwardable

    DEFAULT_PARAMETERS = {
      :daemon_process_name => 'fluentd:master',
      :worker_process_name => 'fluentd:worker %1',
    }

    OVERWRITE_PARAMETERS = {
      :restart_server_process => false,
      :disable_reload => true,  # disables reload and uses restart always
      :logger_class => Logger,  # overwrites logger class
      :supervisor => false,
    }

    def self.run(opts={})
      require 'serverengine'

      #
      # ServerEngine calls:
      #
      #   1. Server#initialize
      #   2. Server#before_run  -> @process_manager.before_run
      #   3. for each workers:
      #      3.1. Server::Worker#before_fork  -> Processor#before_fork
      #      3.2. Server::Worker#run          -> Processor#run
      #      3.3. Server::Worker#after_start  -> Processor#after_start
      #   4. Server#stop        -> @process_manager.stop
      #   5. Server#after_run   -> @process_manager.after_run
      #
      # See ServerEngine for details.
      #
      ServerEngine.create(Server, Server::Worker) do
        DEFAULT_PARAMETERS.merge(opts).merge(OVERWRITE_PARAMETERS)
      end.run
    end

    def_delegators :@process_manager, :before_run, :after_run, :after_start

    def initialize
      if self.is_a?(ServerEngine::MultiProcessServer)
        @process_manager = MultiProcessManager.new
      else
        @process_manager = ProcessManager.new
      end
      @process_manager.logger = logger

      Fluentd.logger = logger
      Fluentd.plugin = PluginRegistry.new
      Fluentd.socket_manager_api = @process_manager.socket_manager

      # MultiProcessManager::MultiProcessProcessor#run overwrites Fluentd.socket_manager_api

      super  # will call reload_config
    end

    # ServerEngine hook point
    def reload_config
      super
      configure!
    end

    def configure!
      path = config[:config_path]
      c = Config.read(path)

      root_agent = RootAgent.new

      # enables c.context.xxx=
      Config::Context.inject!(c)
      c.context.log = logger
      c.context.logger = logger
      c.context.root_agent = root_agent

      # RootAgent#configure recursively calls Agent#configure
      root_agent.configure(c)

      # configure ProcessManager
      @process_manager.configure(c)

      # assign agents to processors
      @process_manager.assign_processors(root_agent)

      @root_agent = root_agent
      @processors = @process_manager.processors

      if self.is_a?(ServerEngine::MultiProcessServer)
        # change number of ServerEngine workers
        scale_workers(@processors.size)
      end

      # warn unused config parameters
      c.check_not_used {|key,e|
        logger.warn "parameter '#{key}' is not used in\n#{e.to_s(nil).strip}"
      }
    end

    attr_reader :process_manager, :processors

    module Worker
      extend Forwardable

      def initialize
        @processor = server.processors[worker_id]
        @processor.worker_initialize(server.process_manager, worker_id)
      end

      def_delegators :@processor, :before_fork, :run, :stop, :reload, :after_start
    end
  end

end
