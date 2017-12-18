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

require 'fluent/config'
require 'fluent/env'
require 'fluent/engine'
require 'fluent/error'
require 'fluent/log'
require 'fluent/plugin'
require 'fluent/rpc'
require 'fluent/system_config'
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
      @start_time = Time.now

      if config[:rpc_endpoint]
        @rpc_endpoint = config[:rpc_endpoint]
        @enable_get_dump = config[:enable_get_dump]
        run_rpc_server
      end
      install_supervisor_signal_handlers

      if config[:signame]
        @signame = config[:signame]
        install_windows_event_handler
      end

      socket_manager_path = ServerEngine::SocketManager::Server.generate_path
      ServerEngine::SocketManager::Server.open(socket_manager_path)
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
    end

    def after_run
      stop_rpc_server if @rpc_endpoint
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
          $log.warn "operation 'flushBuffersAndKillWorkers' is not supported on Windows now."
        else
          Process.kill :USR1, $$
          Process.kill :TERM, $$
        end
        nil
      }
      @rpc_server.mount_proc('/api/plugins.flushBuffers') { |req, res|
        $log.debug "fluentd RPC got /api/plugins.flushBuffers request"
        unless Fluent.windows?
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

      @rpc_server.mount_proc('/api/config.getDump') { |req, res|
        $log.debug "fluentd RPC got /api/config.dump request"
        $log.info "get dump in-memory config via HTTP"
        res.body = supervisor_get_dump_config_handler
        [nil, nil, res]
      } if @enable_get_dump

      @rpc_server.start
    end

    def stop_rpc_server
      @rpc_server.shutdown
    end

    def install_supervisor_signal_handlers
      trap :HUP do
        $log.debug "fluentd supervisor process get SIGHUP"
        supervisor_sighup_handler
      end unless Fluent.windows?

      trap :USR1 do
        $log.debug "fluentd supervisor process get SIGUSR1"
        supervisor_sigusr1_handler
      end unless Fluent.windows?
    end

    def install_windows_event_handler
      Thread.new do
        ev = Win32::Event.new(@signame)
        begin
          ev.reset
          until WaitForSingleObject(ev.handle, 0) == WAIT_OBJECT_0
            sleep 1
          end
          kill_worker
          stop(true)
        ensure
          ev.close
        end
      end
    end

    def supervisor_sighup_handler
      kill_worker
    end

    def supervisor_sigusr1_handler
      if log = config[:logger_initializer]
        log.reopen!
      end

      if config[:worker_pid]
        config[:worker_pid].each_value do |pid|
          Process.kill(:USR1, pid)
          # don't rescue Erro::ESRSH here (invalid status)
        end
      end
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
      $log.info config[:fluentd_conf].to_s
    end

    def supervisor_get_dump_config_handler
      {conf: config[:fluentd_conf].to_s}
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

      config_fname = File.basename(path)
      config_basedir = File.dirname(path)
      # Assume fluent.conf encoding is UTF-8
      config_data = File.open(path, "r:utf-8:utf-8") {|f| f.read }
      inline_config = params['inline_config']
      if inline_config == '-'
        config_data << "\n" << STDIN.read
      elsif inline_config
        config_data << "\n" << inline_config.gsub("\\n","\n")
      end
      fluentd_conf = Fluent::Config.parse(config_data, config_fname, config_basedir, params['use_v1_config'])
      system_config = SystemConfig.create(fluentd_conf)

      # these params must NOT be configured via system config here.
      # these may be overridden by command line params.
      workers = params['workers']
      root_dir = params['root_dir']
      log_level = params['log_level']
      suppress_repeated_stacktrace = params['suppress_repeated_stacktrace']

      log_path = params['log_path']
      chuser = params['chuser']
      chgroup = params['chgroup']
      log_rotate_age = params['log_rotate_age']
      log_rotate_size = params['log_rotate_size']
      rpc_endpoint = system_config.rpc_endpoint
      enable_get_dump = system_config.enable_get_dump

      log_opts = {suppress_repeated_stacktrace: suppress_repeated_stacktrace}
      logger_initializer = Supervisor::LoggerInitializer.new(
        log_path, log_level, chuser, chgroup, log_opts,
        log_rotate_age: log_rotate_age,
        log_rotate_size: log_rotate_size
      )
      # this #init sets initialized logger to $log
      logger_initializer.init(:supervisor, 0)
      logger = $log

      command_sender = Fluent.windows? ? "pipe" : "signal"

      # ServerEngine's "daemonize" option is boolean, and path of pid file is brought by "pid_path"
      pid_path = params['daemonize']
      daemonize = !!params['daemonize']
      main_cmd = params['main_cmd']
      signame = params['signame']

      se_config = {
          worker_type: 'spawn',
          workers: workers,
          log_stdin: false,
          log_stdout: false,
          log_stderr: false,
          enable_heartbeat: true,
          auto_heartbeat: false,
          unrecoverable_exit_codes: [2],
          stop_immediately_at_unrecoverable_exit: true,
          root_dir: root_dir,
          logger: logger,
          log: logger.out,
          log_path: log_path,
          log_level: log_level,
          logger_initializer: logger_initializer,
          chuser: chuser,
          chgroup: chgroup,
          chumask: 0,
          suppress_repeated_stacktrace: suppress_repeated_stacktrace,
          daemonize: daemonize,
          rpc_endpoint: rpc_endpoint,
          enable_get_dump: enable_get_dump,
          windows_daemon_cmdline: [ServerEngine.ruby_bin_path,
                                   File.join(File.dirname(__FILE__), 'daemon.rb'),
                                   ServerModule.name,
                                   WorkerModule.name,
                                   path,
                                   JSON.dump(params)],
          command_sender: command_sender,
          fluentd_conf: fluentd_conf,
          main_cmd: main_cmd,
          signame: signame,
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

      return se_config
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

      def init(process_type, worker_id)
        @opts[:process_type] = process_type
        @opts[:worker_id] = worker_id

        if @path && @path != "-"
          @logdev = if @log_rotate_age || @log_rotate_size
                     Fluent::LogDeviceIO.new(@path, shift_age: @log_rotate_age, shift_size: @log_rotate_size)
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

      def apply_options(opts)
        $log.format = opts[:format] if opts[:format]
        $log.time_format = opts[:time_format] if opts[:time_format]
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
        without_source: nil,
        use_v1_config: true,
        supervise: true,
        standalone_worker: false,
        signame: nil,
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
      @supervise = opt[:supervise]
      @standalone_worker= opt[:standalone_worker]
      @config_path = opt[:config_path]
      @inline_config = opt[:inline_config]
      @use_v1_config = opt[:use_v1_config]
      @log_path = opt[:log_path]
      @dry_run = opt[:dry_run]
      @show_plugin_config = opt[:show_plugin_config]
      @libs = opt[:libs]
      @plugin_dirs = opt[:plugin_dirs]
      @chgroup = opt[:chgroup]
      @chuser = opt[:chuser]
      @rpc_server = nil
      @process_name = nil

      @workers = opt[:workers]
      @root_dir = opt[:root_dir]
      @log_level = opt[:log_level]
      @log_rotate_age = opt[:log_rotate_age]
      @log_rotate_size = opt[:log_rotate_size]
      @suppress_interval = opt[:suppress_interval]
      @suppress_config_dump = opt[:suppress_config_dump]
      @log_event_verbose = opt[:log_event_verbose]
      @without_source = opt[:without_source]
      @signame = opt[:signame]

      @suppress_repeated_stacktrace = opt[:suppress_repeated_stacktrace]
      log_opts = {suppress_repeated_stacktrace: @suppress_repeated_stacktrace}
      @log = LoggerInitializer.new(
        @log_path, @log_level, @chuser, @chgroup, log_opts,
        log_rotate_age: @log_rotate_age,
        log_rotate_size: @log_rotate_size
      )
      @finished = false
    end

    def run_supervisor
      @log.init(:supervisor, 0)
      show_plugin_config if @show_plugin_config
      read_config
      set_system_config
      @log.apply_options(format: @system_config.log.format, time_format: @system_config.log.time_format)

      $log.info :supervisor, "parsing config file is succeeded", path: @config_path

      if @workers < 1
        raise Fluent::ConfigError, "invalid number of workers (must be > 0):#{@workers}"
      end

      if @root_dir
        if File.exist?(@root_dir)
          unless Dir.exist?(@root_dir)
            raise Fluent::InvalidRootDirectory, "non directory entry exists:#{@root_dir}"
          end
        else
          begin
            FileUtils.mkdir_p(@root_dir)
          rescue => e
            raise Fluent::InvalidRootDirectory, "failed to create root directory:#{@root_dir}, #{e.inspect}"
          end
        end
      end

      dry_run_cmd if @dry_run
      supervise
    end

    def options
      {
        'config_path' => @config_path,
        'pid_file' => @daemonize,
        'plugin_dirs' => @plugin_dirs,
        'log_path' => @log_path,
        'root_dir' => @root_dir,
      }
    end

    def run_worker
      begin
        require 'sigdump/setup'
      rescue Exception
        # ignore LoadError and others (related with signals): it may raise these errors in Windows
      end
      worker_id = ENV['SERVERENGINE_WORKER_ID'].to_i
      process_type = case
                     when @standalone_worker then :standalone
                     when worker_id == 0 then :worker0
                     else :workers
                     end
      @log.init(process_type, worker_id)
      show_plugin_config if @show_plugin_config
      read_config
      set_system_config
      @log.apply_options(format: @system_config.log.format, time_format: @system_config.log.time_format)

      Process.setproctitle("worker:#{@process_name}") if @process_name

      if @standalone_worker && @workers != 1
        raise Fluent::ConfigError, "invalid number of workers (must be 1 or unspecified) with --no-supervisor: #{@workers}"
      end

      install_main_process_signal_handlers

      # This is the only log messsage for @standalone_worker
      $log.info "starting fluentd-#{Fluent::VERSION} without supervision", pid: Process.pid, ruby: RUBY_VERSION if @standalone_worker

      main_process do
        create_socket_manager if @standalone_worker
        change_privilege if @standalone_worker
        init_engine
        run_configure
        run_engine
        self.class.cleanup_resources if @standalone_worker
        exit 0
      end
    end

    private

    def create_socket_manager
      socket_manager_path = ServerEngine::SocketManager::Server.generate_path
      ServerEngine::SocketManager::Server.open(socket_manager_path)
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
    end

    def dry_run_cmd
      $log.info "starting fluentd-#{Fluent::VERSION} as dry run mode", ruby: RUBY_VERSION
      @system_config.suppress_config_dump = true
      dry_run
      exit 0
    rescue => e
      $log.error "dry run failed: #{e}"
      exit 1
    end

    ## Set Engine's dry_run_mode true to override all target_id of worker sections
    def dry_run
      begin
        Fluent::Engine.dry_run_mode = true
        change_privilege
        init_engine
        run_configure
      rescue Fluent::ConfigError => e
        $log.error "config error", file: @config_path, error: e
        $log.debug_backtrace
        exit!(1)
      ensure
        Fluent::Engine.dry_run_mode = false
      end
    end

    def show_plugin_config
      name, type = @show_plugin_config.split(":") # input:tail
      $log.info "Use fluent-plugin-config-format --format=txt #{name} #{type}"
      exit 0
    end

    def supervise
      # Make dumpable conf, which is set corresponding_proxies for all elements in all worker sections
      dry_run

      Process.setproctitle("supervisor:#{@process_name}") if @process_name
      $log.info "starting fluentd-#{Fluent::VERSION}", pid: Process.pid, ruby: RUBY_VERSION

      rubyopt = ENV["RUBYOPT"]
      fluentd_spawn_cmd = [ServerEngine.ruby_bin_path, "-Eascii-8bit:ascii-8bit"]
      fluentd_spawn_cmd << rubyopt if rubyopt
      fluentd_spawn_cmd << $0
      fluentd_spawn_cmd += $fluentdargv
      fluentd_spawn_cmd << "--under-supervisor"

      $log.info "spawn command to main: ", cmdline: fluentd_spawn_cmd

      params = {}
      params['main_cmd'] = fluentd_spawn_cmd
      params['daemonize'] = @daemonize
      params['inline_config'] = @inline_config
      params['log_path'] = @log_path
      params['log_rotate_age'] = @log_rotate_age
      params['log_rotate_size'] = @log_rotate_size
      params['chuser'] = @chuser
      params['chgroup'] = @chgroup
      params['use_v1_config'] = @use_v1_config

      # system config parameters
      params['workers'] = @workers
      params['root_dir'] = @root_dir
      params['log_level'] = @log_level
      params['suppress_repeated_stacktrace'] = @suppress_repeated_stacktrace
      params['signame'] = @signame

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

      trap :USR1 do
        flush_buffer
      end unless Fluent.windows?

      if Fluent.windows?
        command_pipe = STDIN.dup
        STDIN.reopen(File::NULL, "rb")
        command_pipe.binmode
        command_pipe.sync = true

        Thread.new do
          loop do
            cmd = command_pipe.gets.chomp
            case cmd
            when "GRACEFUL_STOP", "IMMEDIATE_STOP"
              $log.debug "fluentd main process get #{cmd} command"
              @finished = true
              $log.debug "getting start to shutdown main process"
              Fluent::Engine.stop
              break
            else
              $log.warn "fluentd main process get unknown command [#{cmd}]"
            end
          end
        end
      end
    end

    def flush_buffer
      $log.debug "fluentd main process get SIGUSR1"
      $log.info "force flushing buffered events"
      @log.reopen!

      # Creating new thread due to mutex can't lock
      # in main thread during trap context
      Thread.new {
        begin
          Fluent::Engine.flush!
          $log.debug "flushing thread: flushed"
        rescue Exception => e
          $log.warn "flushing thread error: #{e}"
        end
      }.run
    end

    def logging_with_console_output
      yield $log
      unless @log.stdout?
        logger = ServerEngine::DaemonLogger.new(STDOUT)
        log = Fluent::Log.new(logger)
        log.level = @log_level
        console = log.enable_debug
        yield console
      end
    end

    def main_process(&block)
      Process.setproctitle("worker:#{@process_name}") if @process_name

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

    def read_config
      @config_fname = File.basename(@config_path)
      @config_basedir = File.dirname(@config_path)
      @config_data = File.open(@config_path, "r:utf-8:utf-8") {|f| f.read }
      if @inline_config == '-'
        @config_data << "\n" << STDIN.read
      elsif @inline_config
        @config_data << "\n" << @inline_config.gsub("\\n","\n")
      end
      @conf = Fluent::Config.parse(@config_data, @config_fname, @config_basedir, @use_v1_config)
    end

    def set_system_config
      @system_config = SystemConfig.create(@conf) # @conf is set in read_config
      @system_config.attach(self)
      @system_config.apply(self)
    end

    def change_privilege
      ServerEngine::Privilege.change(@chuser, @chgroup)
    end

    def init_engine
      Fluent::Engine.init(@system_config)

      @libs.each {|lib|
        require lib
      }

      @plugin_dirs.each {|dir|
        if Dir.exist?(dir)
          dir = File.expand_path(dir)
          Fluent::Engine.add_plugin_dir(dir)
        end
      }
    end

    def run_configure
      Fluent::Engine.run_configure(@conf)
    end

    def run_engine
      Fluent::Engine.run
    end
  end
end
