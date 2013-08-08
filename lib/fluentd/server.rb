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
  # Server is the parent process of all worker processes.
  #
  module Server
    DEFAULT_PARAMETERS = {
      #:daemon_process_name => 'fluentd:master',
    }

    OVERWRITE_PARAMETERS = {
      :restart_server_process => false,
      :disable_reload => true,  # disables reload and uses restart always
      :logger_class => Logger,  # overwrites logger class
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

      # <server> section creates a worker instances
      @server_elements = conf.elements.select {|e| e.name == 'server' }

      if @server_elements.empty?
        raise ConfigError, "No <server> elements in the config file"
      end

      # set number of ServerEngine workers
      scale_workers(@server_elements.size)
    end

    attr_reader :server_elements

    # ServerEngine callback
    def before_run
      logger.info "fluentd v#{Fluentd::VERSION}"

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

    def read_config(path)
      begin
        conf = Config.read(path)

        # use backward compatible mode if <server> section doesn't exist
        unless conf.elements.find {|e| e.name == 'server' }
          compat_conf = Config::CompatParser.read(path)
        end
      rescue => e
        begin
          compat_conf = Config::CompatParser.read(path)
          if compat_conf.elements.any? {|e| e.name == 'server' }
            raise e
          end
        rescue
          raise e
        end
      end

      if compat_conf
        compat_conf.name = 'server'
        logger.warn "Using backward compatible configuration file. Please replace it with following content to not show this message again:\n#{compat_conf.to_s}"
        conf = Config::Element.new('ROOT', '', {}, [compat_conf])
      end

      return conf
    end

    module WorkerLauncher
      SOCKET_MANAGER_FILENO = 3

      def initialize
        @opts = server.config
        @server_element = server.server_elements[worker_id]
        @server_element['id'] ||= worker_id
      end

      def before_fork
        # spawn process before run, and monitor the spawned process
        # in the run method
        @monitor = spawn_process
      rescue => e
        logger.error e.to_s
        logger.error_backtrace e.backtrace
        raise
      end

      def run
        # TODO heartbeat monitoring
        @monitor
      rescue => e
        logger.error e.to_s
        logger.error_backtrace e.backtrace
        raise
      ensure
        if pid = @pid
          Process.waitpid2(pid)
        end
      end

      def stop
        if pid = @pid
          begin
            Process.kill('TERM', pid)
          rescue #Errno::ECHILD, Errno::ESRCH, Errno::EPERM
          end
        end
      end

      def self.main
        # receives context data from the parent process
        spawn_data = STDIN.read
        STDIN.reopen(File::NULL)
        opts, server_element = SpawnData.restore(spawn_data)

        # set process name
        $0 = "#{opts[:worker_process_name]} #{server_element['id']}"

        # change user and group
        chuser = server_element['process_user'] || opts[:chuser]
        chgroup = server_element['process_group'] || opts[:chgroup]
        ServerEngine::Daemon.change_privilege(chuser, chgroup)

        # setup SocketManager used by actors/socket_actor.rb
        socket_client = SocketManager::Client.new(SOCKET_MANAGER_FILENO, server_element['id'])

        # initialize worker
        w = Worker.new(opts, socket_client)
        w.install_signal_handlers
        w.configure(server_element)

        socket_client.start_heartbeat

        w.run
      end

      private

      def spawn_process
        # fluentd/command/fluentd-worker.rb calls WorkerLauncher.main
        worker_main = File.expand_path File.join(File.dirname(__FILE__), 'command', 'fluentd-worker.rb')

        # arguments for Process.spawn
        env = {'fluentd_worker_process'=>worker_id.to_s}
        cmdline = [RbConfig.ruby, worker_main]

        options = { }
        setup_resource_options(options)

        # sends context data to the worker process
        spawn_data = SpawnData.dump([@opts, @server_element])

        cpipe, monitor = server.socket_server.new_client_pipe

        rpipe, wpipe = IO.pipe
        begin
          options[:in] = rpipe
          options[SOCKET_MANAGER_FILENO] = cpipe

          # spawn here
          @pid = Process.spawn(env, *cmdline, options)  # calls WorkerLauncher.main

          launch_success = false
          begin
            cpipe.close
            wpipe.write spawn_data

            # TODO wait for completion of Worker#configure

            launch_success = true

          ensure
            unless launch_success
              Process.kill('TERM', @pid)
              Process.waitpid2(@pid)
            end
          end

        ensure
          rpipe.close
          wpipe.close
        end

        return monitor
      end

      def setup_resource_options(options)
        @opts.each_pair {|k,v|
          if k =~ /^rlimit_/
            options[k.to_s] = v.to_s
          end
        }

        if @opts[:chumask]
          options['umask'] = @opts[:chumask].to_s
        end
      end

      class SpawnData
        def initialize(user_data)
          @loaded_features = $LOADED_FEATURES
          @load_path = $LOAD_PATH
          @debug = $DEBUG
          @gc_profiler = GC::Profiler.enabled?
          @user_data = user_data
        end

        def restore!
          GC::Profiler.enable if @gc_profiler

          $DEBUG = @debug

          $LOAD_PATH.clear
          @load_path.each {|path| $LOAD_PATH << path }

          @loaded_features.each {|feature|
            require feature
          }

          return @user_data
        end

        def self.dump(user_data)
          Marshal.dump(new(user_data))
        end

        def self.restore(user_data)
          Marshal.load(user_data).restore!
        end
      end
    end
  end

end
