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

require 'fileutils'
require 'open3'
require 'pathname'
require 'find'

require 'fluent/config'
require 'fluent/counter'
require 'fluent/env'
require 'fluent/engine'
require 'fluent/error'
require 'fluent/log'
require 'fluent/plugin'
require 'fluent/rpc'
require 'fluent/system_config'
require 'fluent/msgpack_factory'
require 'fluent/variable_store'
require 'serverengine'

if Fluent.windows?
  require 'win32/ipc'
  require 'win32/event'
end

module Fluent
  module ServerModule
    def before_run
      @fluentd_conf = config[:fluentd_conf]
      @rpc_endpoint = nil
      @rpc_server = nil
      @counter = nil
      @socket_manager_server = nil
      @starting_new_supervisor_with_zero_downtime = false
      @new_supervisor_pid = nil
      start_in_parallel = ENV.key?("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD")
      @zero_downtime_restart_mutex = Mutex.new

      @fluentd_lock_dir = Dir.mktmpdir("fluentd-lock-")
      ENV['FLUENTD_LOCK_DIR'] = @fluentd_lock_dir

      if config[:rpc_endpoint] and not start_in_parallel
        @rpc_endpoint = config[:rpc_endpoint]
        @enable_get_dump = config[:enable_get_dump]
        run_rpc_server
      end

      if Fluent.windows?
        install_windows_event_handler
      else
        install_supervisor_signal_handlers
      end

      if counter = config[:counter_server] and not start_in_parallel
        run_counter_server(counter)
      end

      if config[:disable_shared_socket]
        $log.info "shared socket for multiple workers is disabled"
      elsif start_in_parallel
        begin
          raise "[BUG] SERVERENGINE_SOCKETMANAGER_PATH env var must exist when starting in parallel" unless ENV.key?('SERVERENGINE_SOCKETMANAGER_PATH')
          @socket_manager_server = ServerEngine::SocketManager::Server.share_sockets_with_another_server(ENV['SERVERENGINE_SOCKETMANAGER_PATH'])
          $log.info "zero-downtime-restart: took over the shared sockets", path: ENV['SERVERENGINE_SOCKETMANAGER_PATH']
        rescue => e
          $log.error "zero-downtime-restart: cancel sequence because failed to take over the shared sockets", error: e
          raise
        end
      else
        @socket_manager_server = ServerEngine::SocketManager::Server.open
        ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_server.path.to_s
      end

      stop_parallel_old_supervisor_after_delay if start_in_parallel
    end

    def after_run
      stop_windows_event_thread if Fluent.windows?
      stop_rpc_server if @rpc_endpoint
      stop_counter_server if @counter
      cleanup_lock_dir
      Fluent::Supervisor.cleanup_socketmanager_path unless @starting_new_supervisor_with_zero_downtime
    ensure
      notify_new_supervisor_that_old_one_has_stopped if @starting_new_supervisor_with_zero_downtime
    end

    def cleanup_lock_dir
      FileUtils.rm(Dir.glob(File.join(@fluentd_lock_dir, "fluentd-*.lock")))
      FileUtils.rmdir(@fluentd_lock_dir)
    end

    def run_rpc_server
      @rpc_server = RPC::Server.new(@rpc_endpoint, $log)

      # built-in RPC for signals
      @rpc_server.mount_proc('/api/processes.interruptWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.interruptWorkers request"
        Process.kill :INT, Process.pid
        nil
      }
      @rpc_server.mount_proc('/api/processes.killWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.killWorkers request"
        Process.kill :TERM, Process.pid
        nil
      }
      @rpc_server.mount_proc('/api/processes.flushBuffersAndKillWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.flushBuffersAndKillWorkers request"
        if Fluent.windows?
          supervisor_sigusr1_handler
          stop(true)
        else
          Process.kill :USR1, Process.pid
          Process.kill :TERM, Process.pid
        end
        nil
      }
      unless Fluent.windows?
        @rpc_server.mount_proc('/api/processes.zeroDowntimeRestart') { |req, res|
          $log.debug "fluentd RPC got /api/processes.zeroDowntimeRestart request"
          Process.kill :USR2, Process.pid
          nil
        }
      end
      @rpc_server.mount_proc('/api/plugins.flushBuffers') { |req, res|
        $log.debug "fluentd RPC got /api/plugins.flushBuffers request"
        if Fluent.windows?
          supervisor_sigusr1_handler
        else
          Process.kill :USR1, Process.pid
        end
        nil
      }
      @rpc_server.mount_proc('/api/config.reload') { |req, res|
        $log.debug "fluentd RPC got /api/config.reload request"
        if Fluent.windows?
          # restart worker with auto restarting by killing
          kill_worker
        else
          Process.kill :HUP, Process.pid
        end
        nil
      }
      @rpc_server.mount_proc('/api/config.dump') { |req, res|
        $log.debug "fluentd RPC got /api/config.dump request"
        $log.info "dump in-memory config"
        supervisor_dump_config_handler
        nil
      }

      @rpc_server.mount_proc('/api/config.gracefulReload') { |req, res|
        $log.debug "fluentd RPC got /api/config.gracefulReload request"
        graceful_reload
        nil
      }

      if @enable_get_dump
        @rpc_server.mount_proc('/api/config.getDump') { |req, res|
          $log.debug "fluentd RPC got /api/config.getDump request"
          $log.info "get dump in-memory config via HTTP"
          res.body = supervisor_get_dump_config_handler
          [nil, nil, res]
        }
      end

      @rpc_server.start
    end

    def stop_rpc_server
      @rpc_server&.shutdown
    end

    def run_counter_server(counter_conf)
      @counter = Fluent::Counter::Server.new(
        counter_conf.scope,
        {host: counter_conf.bind, port: counter_conf.port, log: $log, path: counter_conf.backup_path}
      )
      @counter.start
    end

    def stop_counter_server
      @counter.stop
    end

    def stop_parallel_old_supervisor_after_delay
      Thread.new do
        # Delay to wait the new workers to start up.
        # Even if it takes a long time to start the new workers and stop the old Fluentd first,
        # it is no problem because the socket buffer works, as long as the capacity is not exceeded.
        sleep 10
        old_pid = ENV["FLUENT_RUNNING_IN_PARALLEL_WITH_OLD"]&.to_i
        if old_pid
          $log.info "zero-downtime-restart: stop the old supervisor"
          Process.kill :TERM, old_pid
        end
      rescue => e
        $log.warn "zero-downtime-restart: failed to stop the old supervisor." +
                  " If the old one does not exist, please send SIGWINCH to this new process to start to work fully." +
                  " If it exists, something went wrong. Please kill the old one manually.",
                  error: e
      end
    end

    def notify_new_supervisor_that_old_one_has_stopped
      if config[:pid_path]
        new_pid = File.read(config[:pid_path]).to_i
      else
        raise "[BUG] new_supervisor_pid is not saved" unless @new_supervisor_pid
        new_pid = @new_supervisor_pid
      end

      $log.info "zero-downtime-restart: notify the new supervisor (pid: #{new_pid}) that old one has stopped"
      Process.kill :WINCH, new_pid
    rescue => e
      $log.error(
        "zero-downtime-restart: failed to notify the new supervisor." +
        " Please send SIGWINCH to the new supervisor process manually" +
        " if it does not start to work fully.",
        error: e
      )
    end

    def install_supervisor_signal_handlers
      return if Fluent.windows?

      trap :HUP do
        $log.debug "fluentd supervisor process get SIGHUP"
        supervisor_sighup_handler
      end

      trap :USR1 do
        $log.debug "fluentd supervisor process get SIGUSR1"
        supervisor_sigusr1_handler
      end

      trap :USR2 do
        $log.debug 'fluentd supervisor process got SIGUSR2'
        if Fluent.windows?
          graceful_reload
        else
          zero_downtime_restart
        end
      end

      trap :WINCH do
        $log.debug 'fluentd supervisor process got SIGWINCH'
        cancel_source_only
      end
    end

    if Fluent.windows?
      # Override some methods of ServerEngine::MultiSpawnWorker
      # Since Fluentd's Supervisor doesn't use ServerEngine's HUP, USR1 and USR2
      # handlers (see install_supervisor_signal_handlers), they should be
      # disabled also on Windows, just send commands to workers instead.
      def restart(graceful)
        @monitors.each do |m|
          m.send_command(graceful ? "GRACEFUL_RESTART\n" : "IMMEDIATE_RESTART\n")
        end
      end

      def reload
        @monitors.each do |m|
          m.send_command("RELOAD\n")
        end
      end
    end

    def install_windows_event_handler
      return unless Fluent.windows?

      @pid_signame = "fluentd_#{Process.pid}"
      @signame = config[:signame]

      Thread.new do
        ipc = Win32::Ipc.new(nil)
        events = [
          {win32_event: Win32::Event.new("#{@pid_signame}_STOP_EVENT_THREAD"), action: :stop_event_thread},
          {win32_event: Win32::Event.new("#{@pid_signame}"), action: :stop},
          {win32_event: Win32::Event.new("#{@pid_signame}_HUP"), action: :hup},
          {win32_event: Win32::Event.new("#{@pid_signame}_USR1"), action: :usr1},
          {win32_event: Win32::Event.new("#{@pid_signame}_USR2"), action: :usr2},
          {win32_event: Win32::Event.new("#{@pid_signame}_CONT"), action: :cont},
        ]
        if @signame
          signame_events = [
            {win32_event: Win32::Event.new("#{@signame}"), action: :stop},
            {win32_event: Win32::Event.new("#{@signame}_HUP"), action: :hup},
            {win32_event: Win32::Event.new("#{@signame}_USR1"), action: :usr1},
            {win32_event: Win32::Event.new("#{@signame}_USR2"), action: :usr2},
            {win32_event: Win32::Event.new("#{@signame}_CONT"), action: :cont},
          ]
          events.concat(signame_events)
        end
        begin
          loop do
            infinite = 0xFFFFFFFF
            ipc_idx = ipc.wait_any(events.map {|e| e[:win32_event]}, infinite)
            event_idx = ipc_idx - 1

            if event_idx >= 0 && event_idx < events.length
              $log.debug("Got Win32 event \"#{events[event_idx][:win32_event].name}\"")
            else
              $log.warn("Unexpected return value of Win32::Ipc#wait_any: #{ipc_idx}")
            end
            case events[event_idx][:action]
            when :stop
              stop(true)
            when :hup
              supervisor_sighup_handler
            when :usr1
              supervisor_sigusr1_handler
            when :usr2
              graceful_reload
            when :cont
              supervisor_dump_handler_for_windows
            when :stop_event_thread
              break
            end
          end
        ensure
          events.each { |event| event[:win32_event].close }
        end
      end
    end

    def stop_windows_event_thread
      if Fluent.windows?
        ev = Win32::Event.open("#{@pid_signame}_STOP_EVENT_THREAD")
        ev.set
        ev.close
      end
    end

    def supervisor_sighup_handler
      kill_worker
    end

    def supervisor_sigusr1_handler
      reopen_log
      send_signal_to_workers(:USR1)
    end

    def graceful_reload
      conf = nil
      t = Thread.new do
        $log.info 'Reloading new config'

        # Validate that loading config is valid at first
        conf = Fluent::Config.build(
          config_path: config[:config_path],
          encoding: config[:conf_encoding],
          additional_config: config[:inline_config],
          use_v1_config: config[:use_v1_config],
        )

        Fluent::VariableStore.try_to_reset do
          Fluent::Engine.reload_config(conf, supervisor: true)
        end
      end

      t.report_on_exception = false # Error is handled by myself
      t.join

      reopen_log
      send_signal_to_workers(:USR2)
      @fluentd_conf = conf.to_s
    rescue => e
      $log.error "Failed to reload config file: #{e}"
    end

    def zero_downtime_restart
      Thread.new do
        @zero_downtime_restart_mutex.synchronize do
          $log.info "start zero-downtime-restart sequence"

          if @starting_new_supervisor_with_zero_downtime
            $log.warn "zero-downtime-restart: canceled because it is already starting"
            Thread.exit
          end
          if ENV.key?("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD")
            $log.warn "zero-downtime-restart: canceled because the previous sequence is still running"
            Thread.exit
          end

          @starting_new_supervisor_with_zero_downtime = true
          commands = [ServerEngine.ruby_bin_path, $0] + ARGV
          env_to_add = {
            "SERVERENGINE_SOCKETMANAGER_INTERNAL_TOKEN" => ServerEngine::SocketManager::INTERNAL_TOKEN,
            "FLUENT_RUNNING_IN_PARALLEL_WITH_OLD" => "#{Process.pid}",
          }
          pid = Process.spawn(env_to_add, commands.join(" "))
          @new_supervisor_pid = pid unless config[:daemonize]

          if config[:daemonize]
            Thread.new(pid) do |pid|
              _, status = Process.wait2(pid)
              # check if `ServerEngine::Daemon#daemonize_with_double_fork` succeeded or not
              unless status.success?
                @starting_new_supervisor_with_zero_downtime = false
                $log.error "zero-downtime-restart: failed because new supervisor exits unexpectedly"
              end
            end
          else
            Thread.new(pid) do |pid|
              _, status = Process.wait2(pid)
              @starting_new_supervisor_with_zero_downtime = false
              $log.error "zero-downtime-restart: failed because new supervisor exits unexpectedly", status: status
            end
          end
        end
      rescue => e
        $log.error "zero-downtime-restart: failed", error: e
        @starting_new_supervisor_with_zero_downtime = false
      end
    end

    def cancel_source_only
      if ENV.key?("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD")
        if config[:rpc_endpoint]
          begin
            @rpc_endpoint = config[:rpc_endpoint]
            @enable_get_dump = config[:enable_get_dump]
            run_rpc_server
          rescue => e
            $log.error "failed to start RPC server", error: e
          end
        end

        if counter = config[:counter_server]
          begin
            run_counter_server(counter)
          rescue => e
            $log.error "failed to start counter server", error: e
          end
        end

        $log.info "zero-downtime-restart: done all sequences, now new processes start to work fully"
        ENV.delete("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD")
      end

      send_signal_to_workers(:WINCH)
    end

    def supervisor_dump_handler_for_windows
      # As for UNIX-like, SIGCONT signal to each process makes the process output its dump-file,
      # and it is implemented before the implementation of the function for Windows.
      # It is possible to trap SIGCONT and handle it here also on UNIX-like,
      # but for backward compatibility, this handler is currently for a Windows-only.
      raise "[BUG] This function is for Windows ONLY." unless Fluent.windows?

      Thread.new do
        begin
          FluentSigdump.dump_windows
        rescue => e
          $log.error "failed to dump: #{e}"
        end
      end

      send_signal_to_workers(:CONT)
    rescue => e
      $log.error "failed to dump: #{e}"
    end

    def kill_worker
      if config[:worker_pid]
        pids = config[:worker_pid].clone
        config[:worker_pid].clear
        pids.each_value do |pid|
          if Fluent.windows?
            Process.kill :KILL, pid
          else
            Process.kill :TERM, pid
          end
        end
      end
    end

    def supervisor_dump_config_handler
      $log.info @fluentd_conf
    end

    def supervisor_get_dump_config_handler
      { conf: @fluentd_conf }
    end

    def dump
      super unless @stop
    end

    private

    def reopen_log
      if $log
        # Creating new thread due to mutex can't lock
        # in main thread during trap context
        Thread.new do
          $log.reopen!
        end
      end
    end

    def send_signal_to_workers(signal)
      return unless config[:worker_pid]

      if Fluent.windows?
        send_command_to_workers(signal)
      else
        config[:worker_pid].each_value do |pid|
          # don't rescue Errno::ESRCH here (invalid status)
          Process.kill(signal, pid)
        end
      end
    end

    def send_command_to_workers(signal)
      # Use SeverEngine's CommandSender on Windows
      case signal
      when :HUP
        restart(false)
      when :USR1
        restart(true)
      when :USR2
        reload
      when :CONT
        dump_all_windows_workers
      end
    end

    def dump_all_windows_workers
      @monitors.each do |m|
        m.send_command("DUMP\n")
      end
    end
  end

  module WorkerModule
    def spawn(process_manager)
      main_cmd = config[:main_cmd]
      env = {
        'SERVERENGINE_WORKER_ID' => @worker_id.to_i.to_s,
        'FLUENT_INSTANCE_ID' => Fluent::INSTANCE_ID,
      }
      @pm = process_manager.spawn(env, *main_cmd)
    end

    def after_start
      (config[:worker_pid] ||= {})[@worker_id] = @pm.pid
    end

    def dump
      super unless @stop
    end
  end

  class Supervisor
    def self.serverengine_config(params = {})
      # ServerEngine's "daemonize" option is boolean, and path of pid file is brought by "pid_path"
      pid_path = params['daemonize']
      daemonize = !!params['daemonize']

      se_config = {
        worker_type: 'spawn',
        workers: params['workers'],
        log_stdin: false,
        log_stdout: false,
        log_stderr: false,
        enable_heartbeat: true,
        auto_heartbeat: false,
        unrecoverable_exit_codes: [2],
        stop_immediately_at_unrecoverable_exit: true,
        root_dir: params['root_dir'],
        logger: $log,
        log: $log&.out,
        log_level: params['log_level'],
        chuser: params['chuser'],
        chgroup: params['chgroup'],
        chumask: params['chumask'].is_a?(Integer) ? params['chumask'] : params['chumask']&.to_i(8),
        daemonize: daemonize,
        rpc_endpoint: params['rpc_endpoint'],
        counter_server: params['counter_server'],
        enable_get_dump: params['enable_get_dump'],
        windows_daemon_cmdline: [ServerEngine.ruby_bin_path,
                                 File.join(File.dirname(__FILE__), 'daemon.rb'),
                                 ServerModule.name,
                                 WorkerModule.name,
                                 JSON.dump(params)],
        command_sender: Fluent.windows? ? "pipe" : "signal",
        config_path: params['fluentd_conf_path'],
        fluentd_conf: params['fluentd_conf'],
        conf_encoding: params['conf_encoding'],
        inline_config: params['inline_config'],
        main_cmd: params['main_cmd'],
        signame: params['signame'],
        disable_shared_socket: params['disable_shared_socket'],
        restart_worker_interval: params['restart_worker_interval'],
      }
      se_config[:pid_path] = pid_path if daemonize

      se_config
    end

    def self.default_options
      {
        config_path: Fluent::DEFAULT_CONFIG_PATH,
        plugin_dirs: [Fluent::DEFAULT_PLUGIN_DIR],
        log_level: Fluent::Log::LEVEL_INFO,
        log_path: nil,
        daemonize: nil,
        libs: [],
        setup_path: nil,
        chuser: nil,
        chgroup: nil,
        chumask: "0",
        root_dir: nil,
        suppress_interval: 0,
        suppress_repeated_stacktrace: true,
        ignore_repeated_log_interval: nil,
        without_source: nil,
        with_source_only: nil,
        enable_input_metrics: nil,
        enable_size_metrics: nil,
        use_v1_config: true,
        strict_config_value: nil,
        supervise: true,
        standalone_worker: false,
        signame: nil,
        conf_encoding: 'utf-8',
        disable_shared_socket: nil,
        config_file_type: :guess,
      }
    end

    def self.cleanup_socketmanager_path
      return if Fluent.windows?
      return unless ENV.key?('SERVERENGINE_SOCKETMANAGER_PATH')

      FileUtils.rm_f(ENV['SERVERENGINE_SOCKETMANAGER_PATH'])
    end

    def initialize(cl_opt)
      @cl_opt = cl_opt
      opt = self.class.default_options.merge(cl_opt)

      @config_file_type = opt[:config_file_type]
      @daemonize = opt[:daemonize]
      @standalone_worker= opt[:standalone_worker]
      @config_path = opt[:config_path]
      @inline_config = opt[:inline_config]
      @use_v1_config = opt[:use_v1_config]
      @conf_encoding = opt[:conf_encoding]
      @show_plugin_config = opt[:show_plugin_config]
      @libs = opt[:libs]
      @plugin_dirs = opt[:plugin_dirs]
      @chgroup = opt[:chgroup]
      @chuser = opt[:chuser]
      @chumask = opt[:chumask]
      @signame = opt[:signame]

      # TODO: `@log_path`, `@log_rotate_age` and `@log_rotate_size` should be removed
      # since it should be merged with SystemConfig in `build_system_config()`.
      # We should always use `system_config.log.path`, `system_config.log.rotate_age`
      # and `system_config.log.rotate_size`.
      # However, currently, there is a bug that `system_config.log` parameters
      # are not in `Fluent::SystemConfig::SYSTEM_CONFIG_PARAMETERS`, and these
      # parameters are not merged in `build_system_config()`.
      # Until we fix the bug of `Fluent::SystemConfig`, we need to use these instance variables.
      @log_path = opt[:log_path]
      @log_rotate_age = opt[:log_rotate_age]
      @log_rotate_size = opt[:log_rotate_size]

      @finished = false
    end

    def run_supervisor(dry_run: false)
      if dry_run
        $log.info "starting fluentd-#{Fluent::VERSION} as dry run mode", ruby: RUBY_VERSION
      end

      if @system_config.workers < 1
        raise Fluent::ConfigError, "invalid number of workers (must be > 0):#{@system_config.workers}"
      end

      if Fluent.windows? && @system_config.with_source_only
        raise Fluent::ConfigError, "with-source-only is not supported on Windows"
      end

      root_dir = @system_config.root_dir
      if root_dir
        if File.exist?(root_dir)
          unless Dir.exist?(root_dir)
            raise Fluent::InvalidRootDirectory, "non directory entry exists:#{root_dir}"
          end
        else
          begin
            FileUtils.mkdir_p(root_dir, mode: @system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION)
          rescue => e
            raise Fluent::InvalidRootDirectory, "failed to create root directory:#{root_dir}, #{e.inspect}"
          end
        end
      end

      begin
        ServerEngine::Privilege.change(@chuser, @chgroup)
        MessagePackFactory.init(enable_time_support: @system_config.enable_msgpack_time_support)
        Fluent::Engine.init(@system_config, supervisor_mode: true, start_in_parallel: ENV.key?("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD"))
        Fluent::Engine.run_configure(@conf, dry_run: dry_run)
      rescue Fluent::ConfigError => e
        $log.error 'config error', file: @config_path, error: e
        $log.debug_backtrace
        exit!(1)
      rescue ScriptError => e # LoadError, NotImplementedError, SyntaxError
        if e.respond_to?(:path)
          $log.error e.message, path: e.path, error: e
        else
          $log.error e.message, error: e
        end
        $log.debug_backtrace
        exit!(1)
      rescue => e
        $log.error "unexpected error", error: e
        $log.debug_backtrace
        exit!(1)
      end

      if dry_run
        $log.info 'finished dry run mode'
        exit 0
      else
        supervise
      end
    end

    def options
      {
        'config_path' => @config_path,
        'pid_file' => @daemonize,
        'plugin_dirs' => @plugin_dirs,
        'log_path' => @log_path,
        'root_dir' => @system_config.root_dir,
      }
    end

    def run_worker
      Process.setproctitle("worker:#{@system_config.process_name}") if @process_name

      if @standalone_worker && @system_config.workers != 1
        raise Fluent::ConfigError, "invalid number of workers (must be 1 or unspecified) with --no-supervisor: #{@system_config.workers}"
      end

      if Fluent.windows? && @system_config.with_source_only
        raise Fluent::ConfigError, "with-source-only is not supported on Windows"
      end

      install_main_process_signal_handlers

      # This is the only log messsage for @standalone_worker
      $log.info "starting fluentd-#{Fluent::VERSION} without supervision", pid: Process.pid, ruby: RUBY_VERSION if @standalone_worker

      main_process do
        create_socket_manager if @standalone_worker
        if @standalone_worker
          ServerEngine::Privilege.change(@chuser, @chgroup)
          File.umask(@chumask.to_i(8))
        end
        MessagePackFactory.init(enable_time_support: @system_config.enable_msgpack_time_support)
        Fluent::Engine.init(@system_config, start_in_parallel: ENV.key?("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD"))
        Fluent::Engine.run_configure(@conf)
        Fluent::Engine.run
        self.class.cleanup_socketmanager_path if @standalone_worker
        exit 0
      end
    end

    def configure(supervisor: false)
      setup_global_logger(supervisor: supervisor)

      if @show_plugin_config
        show_plugin_config
      end

      if @inline_config == '-'
        $log.warn('the value "-" for `inline_config` is deprecated. See https://github.com/fluent/fluentd/issues/2711')
        @inline_config = STDIN.read
      end
      @conf = Fluent::Config.build(
        config_path: @config_path,
        encoding: @conf_encoding,
        additional_config: @inline_config,
        use_v1_config: @use_v1_config,
        type: @config_file_type,
      )
      @system_config = build_system_config(@conf)

      $log.info :supervisor, 'parsing config file is succeeded', path: @config_path

      build_additional_configurations do |additional_conf|
        @conf += additional_conf
      end

      @libs.each do |lib|
        require lib
      end

      @plugin_dirs.each do |dir|
        if Dir.exist?(dir)
          dir = File.expand_path(dir)
          Fluent::Plugin.add_plugin_dir(dir)
        end
      end

      if supervisor
        # plugins / configuration dumps
        Gem::Specification.find_all.select { |x| x.name =~ /^fluent(d|-(plugin|mixin)-.*)$/ }.each do |spec|
          $log.info("gem '#{spec.name}' version '#{spec.version}'")
        end
      end
    end

    private

    def setup_global_logger(supervisor: false)
      if supervisor
        worker_id = 0
        process_type = :supervisor
      else
        worker_id = ENV['SERVERENGINE_WORKER_ID'].to_i
        process_type = case
                       when @standalone_worker then :standalone
                       when worker_id == 0 then :worker0
                       else :workers
                       end
      end

      # Parse configuration immediately to initialize logger in early stage.
      # Since we can't confirm the log messages in this parsing process,
      # we must parse the config again after initializing logger.
      conf = Fluent::Config.build(
        config_path: @config_path,
        encoding: @conf_encoding,
        additional_config: @inline_config,
        use_v1_config: @use_v1_config,
        type: @config_file_type,
      )
      system_config = build_system_config(conf)

      # TODO: we should remove this logic. This merging process should be done
      # in `build_system_config()`.
      @log_path ||= system_config.log.path
      @log_rotate_age ||= system_config.log.rotate_age
      @log_rotate_size ||= system_config.log.rotate_size

      rotate = @log_rotate_age || @log_rotate_size
      actual_log_path = @log_path

      # We need to prepare a unique path for each worker since Windows locks files.
      if Fluent.windows? && rotate && @log_path && @log_path != "-"
        actual_log_path = Fluent::Log.per_process_path(@log_path, process_type, worker_id)
      end

      if actual_log_path && actual_log_path != "-"
        FileUtils.mkdir_p(File.dirname(actual_log_path)) unless File.exist?(actual_log_path)
        if rotate
          logdev = Fluent::LogDeviceIO.new(
            actual_log_path,
            shift_age: @log_rotate_age,
            shift_size: @log_rotate_size,
          )
        else
          logdev = File.open(actual_log_path, "a")
        end

        if @chuser || @chgroup
          chuid = @chuser ? ServerEngine::Privilege.get_etc_passwd(@chuser).uid : nil
          chgid = @chgroup ? ServerEngine::Privilege.get_etc_group(@chgroup).gid : nil
          File.chown(chuid, chgid, actual_log_path)
        end

        if system_config.dir_permission
          File.chmod(system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION, File.dirname(actual_log_path))
        end
      else
        logdev = STDOUT
      end

      $log = Fluent::Log.new(
        # log_level: subtract 1 to match serverengine daemon logger side logging severity.
        ServerEngine::DaemonLogger.new(logdev, log_level: system_config.log_level - 1),
        path: actual_log_path,
        process_type: process_type,
        worker_id: worker_id,
        format: system_config.log.format,
        time_format: system_config.log.time_format,
        suppress_repeated_stacktrace: system_config.suppress_repeated_stacktrace,
        ignore_repeated_log_interval: system_config.ignore_repeated_log_interval,
        ignore_same_log_interval: system_config.ignore_same_log_interval,
      )
      $log.enable_color(false) if actual_log_path
      $log.enable_debug if system_config.log_level <= Fluent::Log::LEVEL_DEBUG

      $log.info "init #{process_type} logger",
                path: actual_log_path,
                rotate_age: @log_rotate_age,
                rotate_size: @log_rotate_size
    end

    def create_socket_manager
      server = ServerEngine::SocketManager::Server.open
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = server.path.to_s
    end

    def show_plugin_config
      name, type = @show_plugin_config.split(":") # input:tail
      $log.info "show_plugin_config option is deprecated. Use fluent-plugin-config-format --format=txt #{name} #{type}"
      exit 0
    end

    def supervise
      Process.setproctitle("supervisor:#{@system_config.process_name}") if @system_config.process_name
      $log.info "starting fluentd-#{Fluent::VERSION}", pid: Process.pid, ruby: RUBY_VERSION

      fluentd_spawn_cmd = build_spawn_command
      $log.info "spawn command to main: ", cmdline: fluentd_spawn_cmd

      params = {
        'main_cmd' => fluentd_spawn_cmd,
        'daemonize' => @daemonize,
        'inline_config' => @inline_config,
        'chuser' => @chuser,
        'chgroup' => @chgroup,
        'chumask' => @chumask,
        'fluentd_conf_path' => @config_path,
        'fluentd_conf' => @conf.to_s,
        'use_v1_config' => @use_v1_config,
        'conf_encoding' => @conf_encoding,
        'signame' => @signame,

        'workers' => @system_config.workers,
        'root_dir' => @system_config.root_dir,
        'log_level' => @system_config.log_level,
        'rpc_endpoint' => @system_config.rpc_endpoint,
        'enable_get_dump' => @system_config.enable_get_dump,
        'counter_server' => @system_config.counter_server,
        'disable_shared_socket' => @system_config.disable_shared_socket,
        'restart_worker_interval' => @system_config.restart_worker_interval,
      }

      se = ServerEngine.create(ServerModule, WorkerModule) {
        # Note: This is called only at the initialization of ServerEngine, since
        # Fluentd overwrites all related SIGNAL(HUP,USR1,USR2) and have own reloading feature.
        Fluent::Supervisor.serverengine_config(params)
      }

      se.run
    end

    def install_main_process_signal_handlers
      # Fluentd worker process (worker of ServerEngine) don't use code in serverengine to set signal handlers,
      # because it does almost nothing.
      # This method is the only method to set signal handlers in Fluentd worker process.

      # When user use Ctrl + C not SIGINT, SIGINT is sent to all process in same process group.
      # ServerEngine server process will send SIGTERM to child(spawned) processes by that SIGINT, so
      # worker process SHOULD NOT do anything with SIGINT, SHOULD just ignore.
      trap :INT do
        $log.debug "fluentd main process get SIGINT"

        # When Fluentd is launched without supervisor, worker should handle ctrl-c by itself
        if @standalone_worker
          @finished = true
          $log.debug "getting start to shutdown main process"
          Fluent::Engine.stop
        end
      end

      trap :TERM do
        $log.debug "fluentd main process get SIGTERM"
        unless @finished
          @finished = true
          $log.debug "getting start to shutdown main process"
          Fluent::Engine.stop
        end
      end

      if Fluent.windows?
        install_main_process_command_handlers
      else
        trap :USR1 do
          flush_buffer
        end

        trap :USR2 do
          # Leave the old GracefulReload feature, just in case.
          # We can send SIGUSR2 to the worker process to use this old GracefulReload feature.
          # (Note: Normally, we can send SIGUSR2 to the supervisor process to use
          #  zero-downtime-restart feature as GracefulReload on non-Windows.)
          reload_config
        end

        trap :CONT do
          dump_non_windows
        end

        trap :WINCH do
          cancel_source_only
        end
      end
    end

    def install_main_process_command_handlers
      command_pipe = $stdin.dup
      $stdin.reopen(File::NULL, "rb")
      command_pipe.binmode
      command_pipe.sync = true

      Thread.new do
        loop do
          cmd = command_pipe.gets
          break unless cmd

          case cmd.chomp!
          when "GRACEFUL_STOP", "IMMEDIATE_STOP"
            $log.debug "fluentd main process get #{cmd} command"
            @finished = true
            $log.debug "getting start to shutdown main process"
            Fluent::Engine.stop
            break
          when "GRACEFUL_RESTART"
            $log.debug "fluentd main process get #{cmd} command"
            flush_buffer
          when "RELOAD"
            $log.debug "fluentd main process get #{cmd} command"
            reload_config
          when "DUMP"
            $log.debug "fluentd main process get #{cmd} command"
            dump_windows
          else
            $log.warn "fluentd main process get unknown command [#{cmd}]"
          end
        end
      end
    end

    def flush_buffer
      # Creating new thread due to mutex can't lock
      # in main thread during trap context
      Thread.new do
        begin
          $log.debug "fluentd main process get SIGUSR1"
          $log.info "force flushing buffered events"
          $log.reopen!
          Fluent::Engine.flush!
          $log.debug "flushing thread: flushed"
        rescue Exception => e
          $log.warn "flushing thread error: #{e}"
        end
      end
    end

    def cancel_source_only
      Thread.new do
        begin
          $log.debug "fluentd main process get SIGWINCH"
          $log.info "try to cancel with-source-only mode"
          Fluent::Engine.cancel_source_only!
        rescue Exception => e
          $log.warn "failed to cancel source only", error: e
        end
      end
    end

    def reload_config
      Thread.new do
        $log.debug('worker got SIGUSR2')

        begin
          conf = Fluent::Config.build(
            config_path: @config_path,
            encoding: @conf_encoding,
            additional_config: @inline_config,
            use_v1_config: @use_v1_config,
            type: @config_file_type,
          )

          build_additional_configurations do |additional_conf|
            conf += additional_conf
          end

          Fluent::VariableStore.try_to_reset do
            Fluent::Engine.reload_config(conf)
          end
        rescue => e
          # it is guaranteed that config file is valid by supervisor side. but it's not atomic because of using signals to commnicate between worker and super
          # So need this rescue code
          $log.error("failed to reload config: #{e}")
          next
        end

        @conf = conf
      end
    end

    def dump_non_windows
      begin
        Sigdump.dump unless @finished
      rescue => e
        $log.error("failed to dump: #{e}")
      end
    end

    def dump_windows
      Thread.new do
        begin
          FluentSigdump.dump_windows
        rescue => e
          $log.error("failed to dump: #{e}")
        end
      end
    end

    def logging_with_console_output
      yield $log
      unless $log.stdout?
        logger = ServerEngine::DaemonLogger.new(STDOUT)
        log = Fluent::Log.new(logger)
        log.level = @system_config.log_level
        console = log.enable_debug
        yield console
      end
    end

    def main_process
      if @system_config.process_name
        if @system_config.workers > 1
          Process.setproctitle("worker:#{@system_config.process_name}#{ENV['SERVERENGINE_WORKER_ID']}")
        else
          Process.setproctitle("worker:#{@system_config.process_name}")
        end
      end

      unrecoverable_error = false

      begin
        yield
      rescue Fluent::ConfigError => e
        logging_with_console_output do |log|
          log.error "config error", file: @config_path, error: e
          log.debug_backtrace
        end
        unrecoverable_error = true
      rescue Fluent::UnrecoverableError => e
        logging_with_console_output do |log|
          log.error e.message, error: e
          log.error_backtrace
        end
        unrecoverable_error = true
      rescue ScriptError => e # LoadError, NotImplementedError, SyntaxError
        logging_with_console_output do |log|
          if e.respond_to?(:path)
            log.error e.message, path: e.path, error: e
          else
            log.error e.message, error: e
          end
          log.error_backtrace
        end
        unrecoverable_error = true
      rescue => e
        logging_with_console_output do |log|
          log.error "unexpected error", error: e
          log.error_backtrace
        end
      end

      exit!(unrecoverable_error ? 2 : 1)
    end

    def build_system_config(conf)
      system_config = SystemConfig.create(conf, @cl_opt[:strict_config_value])
      # Prefer the options explicitly specified in the command line
      #
      # TODO: There is a bug that `system_config.log.rotate_age/rotate_size` are
      # not merged with the command line options since they are not in
      # `SYSTEM_CONFIG_PARAMETERS`.
      # We have to fix this bug.
      opt = {}
      Fluent::SystemConfig::SYSTEM_CONFIG_PARAMETERS.each do |param|
        if @cl_opt.key?(param) && !@cl_opt[param].nil?
          opt[param] = @cl_opt[param]
        end
      end
      system_config.overwrite_variables(**opt)
      system_config
    end

    def build_additional_configurations
      if @system_config.config_include_dir&.empty?
        $log.info :supervisor, 'configuration include directory is disabled'
        return
      end
      begin
        supported_suffixes = [".conf", ".yaml", ".yml"]
        Find.find(@system_config.config_include_dir) do |path|
          next if File.directory?(path)
          next unless supported_suffixes.include?(File.extname(path))
          # NOTE: both types of normal config (.conf) and YAML will be loaded.
          # Thus, it does not care whether @config_path is .conf or .yml.
          $log.info :supervisor, 'loading additional configuration file', path: path
          yield Fluent::Config.build(config_path: path,
                                     encoding: @conf_encoding,
                                     use_v1_config: @use_v1_config,
                                     type: :guess)
        end
      rescue Errno::ENOENT
        $log.info :supervisor, 'inaccessible include directory was specified', path: @system_config.config_include_dir
      end
    end

    RUBY_ENCODING_OPTIONS_REGEX = %r{\A(-E|--encoding=|--internal-encoding=|--external-encoding=)}.freeze

    def build_spawn_command
      if ENV['TEST_RUBY_PATH']
        fluentd_spawn_cmd = [ENV['TEST_RUBY_PATH']]
      else
        fluentd_spawn_cmd = [ServerEngine.ruby_bin_path]
      end

      rubyopt = ENV['RUBYOPT']
      if rubyopt
        encodes, others = rubyopt.split(' ').partition { |e| e.match?(RUBY_ENCODING_OPTIONS_REGEX) }
        fluentd_spawn_cmd.concat(others)

        adopted_encodes = encodes.empty? ? ['-Eascii-8bit:ascii-8bit'] : encodes
        fluentd_spawn_cmd.concat(adopted_encodes)
      else
        fluentd_spawn_cmd << '-Eascii-8bit:ascii-8bit'
      end

      if @system_config.enable_jit
        $log.info "enable Ruby JIT for workers (--jit)"
        fluentd_spawn_cmd << '--jit'
      end

      # Adding `-h` so that it can avoid ruby's command blocking
      # e.g. `ruby -Eascii-8bit:ascii-8bit` will block. but `ruby -Eascii-8bit:ascii-8bit -h` won't.
      _, e, s = Open3.capture3(*fluentd_spawn_cmd, "-h")
      if s.exitstatus != 0
        $log.error('Invalid option is passed to RUBYOPT', command: fluentd_spawn_cmd, error: e)
        exit s.exitstatus
      end

      fluentd_spawn_cmd << $0
      fluentd_spawn_cmd += $fluentdargv
      fluentd_spawn_cmd << '--under-supervisor'

      fluentd_spawn_cmd
    end
  end

  module FluentSigdump
    def self.dump_windows
      raise "[BUG] WindowsSigdump::dump is for Windows ONLY." unless Fluent.windows?

      # Sigdump outputs under `/tmp` dir without `SIGDUMP_PATH` specified,
      # but `/tmp` dir may not exist on Windows by default.
      # So use the systemroot-temp-dir instead.
      dump_filepath = ENV['SIGDUMP_PATH'].nil? || ENV['SIGDUMP_PATH'].empty? \
        ? "#{ENV['windir']}/Temp/fluentd-sigdump-#{Process.pid}.log"
        : get_path_with_pid(ENV['SIGDUMP_PATH'])

      require 'sigdump'
      Sigdump.dump(dump_filepath)

      $log.info "dump to #{dump_filepath}."
    end

    def self.get_path_with_pid(raw_path)
      path = Pathname.new(raw_path)
      path.sub_ext("-#{Process.pid}#{path.extname}").to_s
    end
  end
end
