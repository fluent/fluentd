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
  require 'windows/library'
  require 'windows/synchronize'
  require 'windows/system_info'
  include Windows::Library
  include Windows::Synchronize
  include Windows::SystemInfo
  require 'win32/ipc'
  require 'win32/event'
end

module Fluent
  module ServerModule
    def before_run
      @fluentd_conf = config[:fluentd_conf]
      @rpc_server = nil
      @counter = nil

      if config[:rpc_endpoint]
        @rpc_endpoint = config[:rpc_endpoint]
        @enable_get_dump = config[:enable_get_dump]
        run_rpc_server
      end

      if Fluent.windows?
        install_windows_event_handler
      else
        install_supervisor_signal_handlers
      end

      if counter = config[:counter_server]
        run_counter_server(counter)
      end

      socket_manager_path = ServerEngine::SocketManager::Server.generate_path
      ServerEngine::SocketManager::Server.open(socket_manager_path)
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
    end

    def after_run
      stop_windows_event_thread if Fluent.windows?
      stop_rpc_server if @rpc_endpoint
      stop_counter_server if @counter
      Fluent::Supervisor.cleanup_resources
    end

    def run_rpc_server
      @rpc_server = RPC::Server.new(@rpc_endpoint, $log)

      # built-in RPC for signals
      @rpc_server.mount_proc('/api/processes.interruptWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.interruptWorkers request"
        Process.kill :INT, $$
        nil
      }
      @rpc_server.mount_proc('/api/processes.killWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.killWorkers request"
        Process.kill :TERM, $$
        nil
      }
      @rpc_server.mount_proc('/api/processes.flushBuffersAndKillWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.flushBuffersAndKillWorkers request"
        if Fluent.windows?
          supervisor_sigusr1_handler
          stop(true)
        else
          Process.kill :USR1, $$
          Process.kill :TERM, $$
        end
        nil
      }
      @rpc_server.mount_proc('/api/plugins.flushBuffers') { |req, res|
        $log.debug "fluentd RPC got /api/plugins.flushBuffers request"
        if Fluent.windows?
          supervisor_sigusr1_handler
        else
          Process.kill :USR1, $$
        end
        nil
      }
      @rpc_server.mount_proc('/api/config.reload') { |req, res|
        $log.debug "fluentd RPC got /api/config.reload request"
        if Fluent.windows?
          # restart worker with auto restarting by killing
          kill_worker
        else
          Process.kill :HUP, $$
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
        if Fluent.windows?
          supervisor_sigusr2_handler
        else
          Process.kill :USR2, $$
        end

        nil
      }

      @rpc_server.mount_proc('/api/config.getDump') { |req, res|
        $log.debug "fluentd RPC got /api/config.getDump request"
        $log.info "get dump in-memory config via HTTP"
        res.body = supervisor_get_dump_config_handler
        [nil, nil, res]
      } if @enable_get_dump

      @rpc_server.start
    end

    def stop_rpc_server
      @rpc_server.shutdown
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
        supervisor_sigusr2_handler
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

      @pid_signame = "fluentd_#{$$}"
      @signame = config[:signame]

      Thread.new do
        ipc = Win32::Ipc.new(nil)
        events = [
          Win32::Event.new("#{@pid_signame}_STOP_EVENT_THREAD"),
          Win32::Event.new("#{@pid_signame}"),
          Win32::Event.new("#{@pid_signame}_HUP"),
          Win32::Event.new("#{@pid_signame}_USR1"),
          Win32::Event.new("#{@pid_signame}_USR2"),
        ]
        if @signame
          signame_events = [
            Win32::Event.new("#{@signame}"),
            Win32::Event.new("#{@signame}_HUP"),
            Win32::Event.new("#{@signame}_USR1"),
            Win32::Event.new("#{@signame}_USR2"),
          ]
          events.concat(signame_events)
        end
        begin
          loop do
            idx = ipc.wait_any(events, Windows::Synchronize::INFINITE)
            if idx > 0 && idx <= events.length
              $log.debug("Got Win32 event \"#{events[idx - 1].name}\"")
            else
              $log.warn("Unexpected reutrn value of Win32::Ipc#wait_any: #{idx}")
            end
            case idx
            when 2, 6
              stop(true)
            when 3, 7
              supervisor_sighup_handler
            when 4, 8
              supervisor_sigusr1_handler
            when 5, 9
              supervisor_sigusr2_handler
            when 1
              break
            end
          end
        ensure
          events.each { |event| event.close }
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

    def supervisor_sigusr2_handler
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

    private

    def reopen_log
      if (log = config[:logger_initializer])
        # Creating new thread due to mutex can't lock
        # in main thread during trap context
        Thread.new do
          log.reopen!
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
      end
    end
  end

  module WorkerModule
    def spawn(process_manager)
      main_cmd = config[:main_cmd]
      env = {
        'SERVERENGINE_WORKER_ID' => @worker_id.to_i.to_s,
      }
      @pm = process_manager.spawn(env, *main_cmd)
    end

    def after_start
      (config[:worker_pid] ||= {})[@worker_id] = @pm.pid
    end
  end

  class Supervisor
    def self.load_config(path, params = {})
      pre_loadtime = 0
      pre_loadtime = params['pre_loadtime'].to_i if params['pre_loadtime']
      pre_config_mtime = nil
      pre_config_mtime = params['pre_config_mtime'] if params['pre_config_mtime']
      config_mtime = File.mtime(path)

      # reuse previous config if last load time is within 5 seconds and mtime of the config file is not changed
      if Time.now - Time.at(pre_loadtime) < 5 and config_mtime == pre_config_mtime
        return params['pre_conf']
      end

      log_level = params['log_level']
      suppress_repeated_stacktrace = params['suppress_repeated_stacktrace']
      ignore_repeated_log_interval = params['ignore_repeated_log_interval']
      ignore_same_log_interval = params['ignore_same_log_interval']

      log_path = params['log_path']
      chuser = params['chuser']
      chgroup = params['chgroup']
      log_rotate_age = params['log_rotate_age']
      log_rotate_size = params['log_rotate_size']

      log_opts = {suppress_repeated_stacktrace: suppress_repeated_stacktrace, ignore_repeated_log_interval: ignore_repeated_log_interval,
                  ignore_same_log_interval: ignore_same_log_interval}
      logger_initializer = Supervisor::LoggerInitializer.new(
        log_path, log_level, chuser, chgroup, log_opts,
        log_rotate_age: log_rotate_age,
        log_rotate_size: log_rotate_size
      )
      # this #init sets initialized logger to $log
      logger_initializer.init(:supervisor, 0)
      logger_initializer.apply_options(format: params['log_format'], time_format: params['log_time_format'])
      logger = $log

      command_sender = Fluent.windows? ? "pipe" : "signal"

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
        logger: logger,
        log: logger.out,
        log_path: log_path,
        log_level: log_level,
        logger_initializer: logger_initializer,
        chuser: chuser,
        chgroup: chgroup,
        chumask: 0,
        suppress_repeated_stacktrace: suppress_repeated_stacktrace,
        ignore_repeated_log_interval: ignore_repeated_log_interval,
        ignore_same_log_interval: ignore_same_log_interval,
        daemonize: daemonize,
        rpc_endpoint: params['rpc_endpoint'],
        counter_server: params['counter_server'],
        enable_get_dump: params['enable_get_dump'],
        windows_daemon_cmdline: [ServerEngine.ruby_bin_path,
                                 File.join(File.dirname(__FILE__), 'daemon.rb'),
                                 ServerModule.name,
                                 WorkerModule.name,
                                 path,
                                 JSON.dump(params)],
        command_sender: command_sender,
        fluentd_conf: params['fluentd_conf'],
        conf_encoding: params['conf_encoding'],
        inline_config: params['inline_config'],
        config_path: path,
        main_cmd: params['main_cmd'],
        signame: params['signame'],
      }
      if daemonize
        se_config[:pid_path] = pid_path
      end
      pre_params = params.dup
      params['pre_loadtime'] = Time.now.to_i
      params['pre_config_mtime'] = config_mtime
      params['pre_conf'] = se_config
      # prevent pre_conf from being too big by reloading many times.
      pre_params['pre_conf'] = nil
      params['pre_conf'][:windows_daemon_cmdline][5] = JSON.dump(pre_params)

      se_config
    end

    class LoggerInitializer
      def initialize(path, level, chuser, chgroup, opts, log_rotate_age: nil, log_rotate_size: nil)
        @path = path
        @level = level
        @chuser = chuser
        @chgroup = chgroup
        @opts = opts
        @log_rotate_age = log_rotate_age
        @log_rotate_size = log_rotate_size
      end

      def worker_id_suffixed_path(worker_id, path)
        require 'pathname'

        Pathname(path).sub_ext("-#{worker_id}#{Pathname(path).extname}").to_s
      end

      def init(process_type, worker_id)
        @opts[:process_type] = process_type
        @opts[:worker_id] = worker_id

        if @path && @path != "-"
          unless File.exist?(@path)
            FileUtils.mkdir_p(File.dirname(@path))
          end

          @logdev = if @log_rotate_age || @log_rotate_size
                     Fluent::LogDeviceIO.new(Fluent.windows? ?
                                               worker_id_suffixed_path(worker_id, @path) : @path,
                                             shift_age: @log_rotate_age, shift_size: @log_rotate_size)
                   else
                     File.open(@path, "a")
                   end
          if @chuser || @chgroup
            chuid = @chuser ? ServerEngine::Privilege.get_etc_passwd(@chuser).uid : nil
            chgid = @chgroup ? ServerEngine::Privilege.get_etc_group(@chgroup).gid : nil
            File.chown(chuid, chgid, @path)
          end
        else
          @logdev = STDOUT
        end

        dl_opts = {}
        # subtract 1 to match serverengine daemon logger side logging severity.
        dl_opts[:log_level] = @level - 1
        logger = ServerEngine::DaemonLogger.new(@logdev, dl_opts)
        $log = Fluent::Log.new(logger, @opts)
        $log.enable_color(false) if @path
        $log.enable_debug if @level <= Fluent::Log::LEVEL_DEBUG
      end

      def stdout?
        @logdev == STDOUT
      end

      def reopen!
        if @path && @path != "-"
          @logdev.reopen(@path, "a")
        end
        self
      end

      def apply_options(format: nil, time_format: nil, log_dir_perm: nil, ignore_repeated_log_interval: nil, ignore_same_log_interval: nil)
        $log.format = format if format
        $log.time_format = time_format if time_format
        $log.ignore_repeated_log_interval = ignore_repeated_log_interval if ignore_repeated_log_interval
        $log.ignore_same_log_interval = ignore_same_log_interval if ignore_same_log_interval

        if @path && log_dir_perm
          File.chmod(log_dir_perm || 0755, File.dirname(@path))
        end
      end

      def level=(level)
        @level = level
        $log.level = level
      end
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
        root_dir: nil,
        suppress_interval: 0,
        suppress_repeated_stacktrace: true,
        ignore_repeated_log_interval: nil,
        without_source: nil,
        use_v1_config: true,
        strict_config_value: nil,
        supervise: true,
        standalone_worker: false,
        signame: nil,
        conf_encoding: 'utf-8'
      }
    end

    def self.cleanup_resources
      unless Fluent.windows?
        if ENV.has_key?('SERVERENGINE_SOCKETMANAGER_PATH')
          FileUtils.rm_f(ENV['SERVERENGINE_SOCKETMANAGER_PATH'])
        end
      end
    end

    def initialize(opt)
      @daemonize = opt[:daemonize]
      @standalone_worker= opt[:standalone_worker]
      @config_path = opt[:config_path]
      @inline_config = opt[:inline_config]
      @use_v1_config = opt[:use_v1_config]
      @conf_encoding = opt[:conf_encoding]
      @log_path = opt[:log_path]
      @show_plugin_config = opt[:show_plugin_config]
      @libs = opt[:libs]
      @plugin_dirs = opt[:plugin_dirs]
      @chgroup = opt[:chgroup]
      @chuser = opt[:chuser]

      @log_rotate_age = opt[:log_rotate_age]
      @log_rotate_size = opt[:log_rotate_size]
      @signame = opt[:signame]

      @cl_opt = opt
      @conf = nil

      log_opts = {suppress_repeated_stacktrace: opt[:suppress_repeated_stacktrace], ignore_repeated_log_interval: opt[:ignore_repeated_log_interval],
                  ignore_same_log_interval: opt[:ignore_same_log_interval]}
      @log = LoggerInitializer.new(
        @log_path, opt[:log_level], @chuser, @chgroup, log_opts,
        log_rotate_age: @log_rotate_age,
        log_rotate_size: @log_rotate_size
      )
      @finished = false
    end

    def run_supervisor(dry_run: false)
      if dry_run
        $log.info "starting fluentd-#{Fluent::VERSION} as dry run mode", ruby: RUBY_VERSION
      end

      if @system_config.workers < 1
        raise Fluent::ConfigError, "invalid number of workers (must be > 0):#{@system_config.workers}"
      end

      root_dir = @system_config.root_dir
      if root_dir
        if File.exist?(root_dir)
          unless Dir.exist?(root_dir)
            raise Fluent::InvalidRootDirectory, "non directory entry exists:#{root_dir}"
          end
        else
          begin
            FileUtils.mkdir_p(root_dir, mode: @system_config.dir_permission || 0755)
          rescue => e
            raise Fluent::InvalidRootDirectory, "failed to create root directory:#{root_dir}, #{e.inspect}"
          end
        end
      end

      begin
        ServerEngine::Privilege.change(@chuser, @chgroup)
        MessagePackFactory.init(enable_time_support: @system_config.enable_msgpack_time_support)
        Fluent::Engine.init(@system_config, supervisor_mode: true)
        Fluent::Engine.run_configure(@conf, dry_run: dry_run)
      rescue Fluent::ConfigError => e
        $log.error 'config error', file: @config_path, error: e
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
      begin
        require 'sigdump/setup'
      rescue Exception
        # ignore LoadError and others (related with signals): it may raise these errors in Windows
      end

      Process.setproctitle("worker:#{@system_config.process_name}") if @process_name

      if @standalone_worker && @system_config.workers != 1
        raise Fluent::ConfigError, "invalid number of workers (must be 1 or unspecified) with --no-supervisor: #{@system_config.workers}"
      end

      install_main_process_signal_handlers

      # This is the only log messsage for @standalone_worker
      $log.info "starting fluentd-#{Fluent::VERSION} without supervision", pid: Process.pid, ruby: RUBY_VERSION if @standalone_worker

      main_process do
        create_socket_manager if @standalone_worker
        if @standalone_worker
          ServerEngine::Privilege.change(@chuser, @chgroup)
          File.umask(0)
        end
        MessagePackFactory.init(enable_time_support: @system_config.enable_msgpack_time_support)
        Fluent::Engine.init(@system_config)
        Fluent::Engine.run_configure(@conf)
        Fluent::Engine.run
        self.class.cleanup_resources if @standalone_worker
        exit 0
      end
    end

    def configure(supervisor: false)
      if supervisor
        @log.init(:supervisor, 0)
      else
        worker_id = ENV['SERVERENGINE_WORKER_ID'].to_i
        process_type = case
                       when @standalone_worker then :standalone
                       when worker_id == 0 then :worker0
                       else :workers
                       end
        @log.init(process_type, worker_id)
      end

      if @show_plugin_config
        show_plugin_config
      end

      if @inline_config == '-'
        $log.warn('the value "-" for `inline_config` is deprecated. See https://github.com/fluent/fluentd/issues/2711')
        @inline_config = STDIN.read
      end
      @conf = Fluent::Config.build(config_path: @config_path, encoding: @conf_encoding, additional_config: @inline_config, use_v1_config: @use_v1_config)
      @system_config = build_system_config(@conf)

      @log.level = @system_config.log_level
      @log.apply_options(
        format: @system_config.log.format,
        time_format: @system_config.log.time_format,
        log_dir_perm: @system_config.dir_permission,
        ignore_repeated_log_interval: @system_config.ignore_repeated_log_interval,
        ignore_same_log_interval: @system_config.ignore_same_log_interval
      )

      $log.info :supervisor, 'parsing config file is succeeded', path: @config_path

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

    def create_socket_manager
      socket_manager_path = ServerEngine::SocketManager::Server.generate_path
      ServerEngine::SocketManager::Server.open(socket_manager_path)
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
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
        'log_path' => @log_path,
        'log_rotate_age' => @log_rotate_age,
        'log_rotate_size' => @log_rotate_size,
        'chuser' => @chuser,
        'chgroup' => @chgroup,
        'use_v1_config' => @use_v1_config,
        'conf_encoding' => @conf_encoding,
        'signame' => @signame,
        'fluentd_conf' => @conf.to_s,

        'workers' => @system_config.workers,
        'root_dir' => @system_config.root_dir,
        'log_level' => @system_config.log_level,
        'suppress_repeated_stacktrace' => @system_config.suppress_repeated_stacktrace,
        'ignore_repeated_log_interval' => @system_config.ignore_repeated_log_interval,
        'rpc_endpoint' => @system_config.rpc_endpoint,
        'enable_get_dump' => @system_config.enable_get_dump,
        'counter_server' => @system_config.counter_server,
        'log_format' => @system_config.log.format,
        'log_time_format' => @system_config.log.time_format,
      }

      se = ServerEngine.create(ServerModule, WorkerModule){
        Fluent::Supervisor.load_config(@config_path, params)
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
          reload_config
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
          @log.reopen!
          Fluent::Engine.flush!
          $log.debug "flushing thread: flushed"
        rescue Exception => e
          $log.warn "flushing thread error: #{e}"
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
          )

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

    def logging_with_console_output
      yield $log
      unless @log.stdout?
        logger = ServerEngine::DaemonLogger.new(STDOUT)
        log = Fluent::Log.new(logger)
        log.level = @system_config.log_level
        console = log.enable_debug
        yield console
      end
    end

    def main_process(&block)
      if @system_config.process_name
        if @system_config.workers > 1
          Process.setproctitle("worker:#{@system_config.process_name}#{ENV['SERVERENGINE_WORKER_ID']}")
        else
          Process.setproctitle("worker:#{@system_config.process_name}")
        end
      end

      unrecoverable_error = false

      begin
        block.call
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
      opt = {}
      Fluent::SystemConfig::SYSTEM_CONFIG_PARAMETERS.each do |param|
        if @cl_opt.key?(param) && !@cl_opt[param].nil?
          if param == :log_level && @cl_opt[:log_level] == Fluent::Log::LEVEL_INFO
            # info level can't be specified via command line option.
            # log_level is info here, it is default value and <system>'s log_level should be applied if exists.
            next
          end

          opt[param] = @cl_opt[param]
        end
      end
      system_config.overwrite_variables(**opt)
      system_config
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
end
