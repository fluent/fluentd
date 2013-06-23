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
      # ServerEngine calls:
      #
      #   1. Server#initialize
      #   2. Server#before_run
      #   3. Server::WorkerLauncher#run for each workers
      #   4. Server#stop
      #   5. Server#after_run
      #
      # See ServerEngine for details.
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

      # set number of ServerEngine server_elements
      scale_workers(@server_elements.size)
    end

    attr_reader :server_elements

    # ServerEngine callback
    def before_run
    end

    # ServerEngine callback
    def after_run
      @socket_manager.close
    end

    private

    def read_config(path)
      begin
        conf = Config.read(path)

        unless conf.elements.find {|e| e.name == 'server' }
          # TODO backward compatible mode
          compat_conf = Config::CompatParser.read(path)
        end
      rescue => e
        begin
          compat_conf = Config::CompatParser.read(path)
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
      def initialize
        @server_config = server.config
        @server_element = server.server_elements[worker_id]
        @server_element['worker_id'] = worker_id
      end

      def run
        begin
          spawn_process
        ensure
          pid, status = Process.waitpid2(@pid) if @pid
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
        spawn_data = STDIN.read
        STDIN.reopen(File::NULL)

        server_config, server_element = SpawnData.restore(spawn_data)

        name = server_element.arg.empty? ? server_element['worker_id'] : server_element.arg
        $0 = "fluentd:worker #{name}"

        chuser = server_element['process_user'] || server_config[:chuser]
        chgroup = server_element['process_group'] || server_config[:chgroup]
        ServerEngine::Daemon.change_privilege(chuser, chgroup)

        w = Worker.new(server_config, server_element)
        w.install_signal_handlers
        w.run
      end

      private

      def spawn_process
        worker_main = File.expand_path File.join(File.dirname(__FILE__), 'command', 'fluentd-worker.rb')

        env = {'fluentd_worker_process'=>worker_id.to_s}
        cmdline = [RbConfig.ruby, worker_main]

        options = { }
        setup_resource_options(options)

        rpipe, wpipe = IO.pipe
        begin
          options[:in] = rpipe
          @pid = Process.spawn(env, *cmdline, options)

          wpipe.write SpawnData.dump([@server_config, @server_element])

        rescue => e
          logger.error e.to_s
          logger.error_backtrace e.backtrace

        ensure
          rpipe.close
          wpipe.close
        end

        # TODO wait for configure

        return pid
      end

      def setup_resource_options(options)
        @server_config.each_pair {|k,v|
          if k =~ /^rlimit_/
            options[k.to_s] = v.to_s
          end
        }

        if @server_config[:chumask]
          options['umask'] = @server_config[:chumask].to_s
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
