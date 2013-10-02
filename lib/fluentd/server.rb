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

  require 'serverengine'

  require_relative 'worker_launcher'
  require_relative 'socket_manager'
  require_relative 'version'
  require_relative 'config/parser'
  require_relative 'config/dsl'
  require_relative 'config/compat_parser'
  require_relative 'logger'

  #
  # Server is the parent process of all worker processes.
  #
  module Server
    DEFAULT_PARAMETERS = {
      #:daemon_process_name => 'fluentd:master',
    }

    OVERWRITE_PARAMETERS = {
      :restart_server_process => false,
      :disable_reload => true,  # disables reload and uses restart always
      :logger_class => Fluentd::Logger,  # overwrites logger class
      :worker_type => 'thread',
    }

    def self.run(opts={})
      #
      # ServerEngine.create calls:
      #
      #   1. Server#initialize
      #   2. Server#before_run
      #   3. Server::WorkerLauncher#run for each workers
      #   4. Server#stop
      #   5. Server#after_run
      #
      # See ServerEngine documents for details:
      # https://github.com/frsyuki/serverengine
      #
      # WorkerLauncher creates Worker instance for each <server> section
      # in the configuration file.
      #
      # WorkerLauncher.main is the entry point of worker processes.
      #
      ServerEngine.create(Server, WorkerLauncher) do
        DEFAULT_PARAMETERS.merge(opts).merge(OVERWRITE_PARAMETERS)
      end.run
    end

    def initialize
      super  # calls reload_config
    end

    # ServerEngine hook point
    def reload_config
      super

      # load fluentd.conf
      conf = read_config(config[:config_path])

      # <worker> section creates a worker instances
      @worker_elements = conf.elements.select {|e| e.name == 'worker' }

      if @worker_elements.empty?
        raise ConfigError, "No <worker> elements in the config file"
      end

      # plugins / configuration dumps
      Gem::Specification.find_all.select{|x| x.name =~ /^fluentd(-(plugin|mixin)-.*)?$/}.each do |spec|
        logger.info "gem '#{spec.name}' version '#{spec.version}'"
      end
      unless config[:suppress_config_dump]
        logger.info "starting with configuration:\n#{conf.to_s}"
      end

      # set number of ServerEngine workers using
      # ServerEngine::Server#scale_workers method
      scale_workers(@worker_elements.size)
    end

    attr_reader :worker_elements

    # ServerEngine callback
    def before_run
      logger.info "fluentd v#{Fluentd::VERSION}"

      # initializes socket manager server
      @socket_server = SocketManager::Server.new
      @socket_server.start
    end

    # ServerEngine callback
    def after_run
      stop(false)
      join_workers

      @socket_server.shutdown
    end

    attr_reader :socket_server

    private

    # read configuration file
    def read_config(path)
      if path =~ /\.rb$/
        return Config::DSL::Parser.read(path)
      end

      begin
        # try to use v11 mode first
        conf = Config::Parser.read(path)

        # use backward compatible mode if <worker> section doesn't exist
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
        # generates root <worker> section for v10 compat configuration
        compat_conf.name = 'worker'
        logger.warn "Using backward compatible configuration file. Please replace it with following content to not show this message again:\n#{compat_conf.to_s}"
        conf = Config::Element.new('ROOT', '', {}, [compat_conf])
      end

      return conf
    end
  end

end
