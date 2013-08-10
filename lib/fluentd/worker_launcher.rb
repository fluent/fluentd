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

  require_relative 'worker'
  require_relative 'socket_manager'
  require_relative 'server'  # Marshal.load requires classes loaded

  module WorkerLauncher
    SOCKET_MANAGER_FILENO = 3

    def initialize
      @opts = server.config
      @worker_element = server.worker_elements[worker_id]
      @worker_element['id'] ||= worker_id
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
      # TODO monitor child process using heartbeat.
      # So far here does nothing
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

    # entry point of the worker process
    def self.main
      # receives context data from the parent process
      spawn_data = STDIN.read
      STDIN.reopen(File::NULL)
      opts, worker_element = SpawnData.restore(spawn_data)

      # set process name
      $0 = "#{opts[:worker_process_name]} #{worker_element['id']}"

      # change user and group
      chuser = worker_element['process_user'] || opts[:chuser]
      chgroup = worker_element['process_group'] || opts[:chgroup]
      ServerEngine::Daemon.change_privilege(chuser, chgroup)

      # setup SocketManager used by actors/socket_actor.rb
      socket_client = SocketManager::Client.new(SOCKET_MANAGER_FILENO, worker_element['id'])

      # initialize worker
      w = Worker.new(opts, socket_client)
      w.install_signal_handlers
      w.configure(worker_element)

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

      # sends context data to the worker process through STDIN
      spawn_data = SpawnData.dump([@opts, @worker_element])

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

