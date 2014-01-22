#
# Fluentd
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

  require 'drb'
  require 'serverengine'

  require 'fluentd/logger'
  require 'fluentd/engine'
  require 'fluentd/version'
  require 'fluentd/socket_manager'
  require 'fluentd/worker_launcher'
  require 'fluentd/config/parser'
  require 'fluentd/config/dsl'
  require 'fluentd/config/compat_parser'

  #
  # Server is the master process of all worker processes.
  #
  # It creates a thread (WorkerLauncher) for each <worker> elements in config file.
  # Note that Server and WorkerLauncher are managed by a gem called "ServerEngine".
  #
  #         Managed by ServerEngine
  # +----------------------------------------+
  # |                                        |
  # |          +--> <worker> WorkerLauncher ----> spawn Worker
  # |          |                             |
  # | Server --+--> <worker> WorkerLauncher ----> spawn Worker
  # |          |                             |
  # |          ---> <worker> WorkerLauncher ----> spawn Worker
  # |                                        |
  # +----------------------------------------+
  #
  # Next step: 'fluentd/worker_launcher.rb'
  #
  # ServerEngine:
  # https://github.com/frsyuki/serverengine
  #
  module Server
    DEFAULT_PARAMETERS = {
      #daemon_process_name: 'fluentd:master',
      log: '-',
    }

    OVERWRITE_PARAMETERS = {
      restart_server_process: false,
      disable_reload: true,  # disables reload and uses restart always
      logger_class: Fluentd::Logger,  # overwrites logger class
      worker_type: 'thread',  # thread for each worker process
    }

    def self.run(options={})
      #
      # ServerEngine.create calls:
      #
      #   1. Server#initialize
      #   2. Server#reload_config
      #   3. Server#before_run
      #   4. WorkerLauncher#run for each workers
      #
      ServerEngine.create(Server, WorkerLauncher) do
        # This block should return configuration parameters.
        # ServerEngine calls this block when the process received reload signal.
        DEFAULT_PARAMETERS.merge(options).merge(OVERWRITE_PARAMETERS)
      end.run
    end

    def initialize
      # show greeting messages to log file
      greeting_logs

      # initialzie Engine.shared_data, the process-global data
      Engine.shared_data = {}
      Engine.shared_data['fluentd_options'] = config

      super  # ServerEngine calls #reload_config
    end

    attr_reader :worker_elements

    attr_reader :socket_server

    # ServerEngine hook point
    def reload_config
      super

      # load fluentd.conf
      conf = Server.read_config(config[:config_path])

      if config[:suppress_config_dump]
        if conf[:compat]
          logger.warn "Using backward compatible configuration file. Please replace it with configuration with <worker> tag."
        end
      else
        if conf[:compat]
          logger.warn "Using backward compatible configuration file. Please replace it with following content to not show this message again:\n#{conf.elements[0].to_s}"
        else
          logger.info "Configuration:\n#{conf.to_s}"
        end
      end

      # <worker> element creates a worker instances
      worker_elements = conf.elements.select {|e| e.name == 'worker' }

      if worker_elements.empty?
        raise ConfigError, "No <worker> elements in the config file"
      end

      # config must be marshal-able so that WorkerLauncher can receive it through DRb
      begin
        Marshal.dump(conf)
      rescue => e
        raise ConfigError, "Configuration includes objects that Marshal can't serialize: #{e}"
      end
      Engine.shared_data['fluentd_config'] = conf

      # set number of ServerEngine workers using
      # ServerEngine::Server#scale_workers method
      scale_workers(worker_elements.size)
    end

    # ServerEngine callback
    def before_run
      # start DRb primary server
      DRb.start_service(nil, Engine)

      # initializes socket manager server
      @socket_server = SocketManager::Server.new
      @socket_server.start
    end

    # used by WorkerLauncher#initialize
    def drb_uri
      DRb.uri
    end

    # ServerEngine callback
    def after_run
      stop(false)
      join_workers  # ServerEngine::MultiWorkerServer#join_workers

      @socket_server.shutdown
    end

    private

    def greeting_logs
      logger.info "fluentd v#{Fluentd::VERSION}"

      # plugins / configuration dumps
      Gem::Specification.find_all.select{|x| x.name =~ /^fluentd(-(plugin|mixin)-.*)?$/}.each do |spec|
        logger.info "gem '#{spec.name}' version '#{spec.version}'"
      end
    end

    # read configuration file
    def self.read_config(path)
      if path =~ /\.rb$/
        return Config::DSL::Parser.read(path)
      end

      begin
        # try to use v11 mode first
        conf = Config::Parser.read(path, Kernel.binding)

        # use backward compatible mode if <worker> element doesn't exist
        unless conf.elements.find {|e| e.name == 'worker' }
          compat_conf = Config::CompatParser.read(path)
        end

      rescue => e
        # falling back to compatible mode
        begin
          compat_conf = Config::CompatParser.read(path)
          if compat_conf.elements.any? {|e| e.name == 'worker' }
            raise e
          end
        rescue
          raise e
        end
      end

      if compat_conf
        # generates root <worker> element for v10 compat configuration
        compat_conf.name = 'worker'
        # logger.warn "Using backward compatible configuration file. Please replace it with following content to not show this message again:\n#{compat_conf.to_s}"
        conf = Config::Element.new('ROOT', '', {compat: true}, [compat_conf])
      end

      return conf
    end
  end

end
