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

require 'etc'
require 'fcntl'

begin
  require 'sigdump/setup'
rescue Exception
  # ignore LoadError and others (related with signals): it may raise these errors in Windows
end

require 'fluent/config'
require 'fluent/env'
require 'fluent/engine'
require 'fluent/log'
require 'fluent/plugin'
require 'fluent/rpc'
require 'fluent/system_config'

module Fluent
  class Supervisor
    def self.get_etc_passwd(user)
      if user.to_i.to_s == user
        Etc.getpwuid(user.to_i)
      else
        Etc.getpwnam(user)
      end
    end

    def self.get_etc_group(group)
      if group.to_i.to_s == group
        Etc.getgrgid(group.to_i)
      else
        Etc.getgrnam(group)
      end
    end

    class LoggerInitializer
      def initialize(path, level, chuser, chgroup, opts)
        @path = path
        @level = level
        @chuser = chuser
        @chgroup = chgroup
        @opts = opts
      end

      def init
        if @path && @path != "-"
          @io = File.open(@path, "a")
          if @chuser || @chgroup
            chuid = @chuser ? Supervisor.get_etc_passwd(@chuser).uid : nil
            chgid = @chgroup ? Supervisor.get_etc_group(@chgroup).gid : nil
            File.chown(chuid, chgid, @path)
          end
        else
          @io = STDOUT
        end

        $log = Fluent::Log.new(@io, @level, @opts)
        $log.enable_color(false) if @path
        $log.enable_debug if @level <= Fluent::Log::LEVEL_DEBUG
      end

      def stdout?
        @io == STDOUT
      end

      def reopen!
        if @path && @path != "-"
          @io.reopen(@path, "a")
        end
        self
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
        suppress_interval: 0,
        suppress_repeated_stacktrace: true,
        without_source: false,
        use_v1_config: true,
        supervise: true,
      }
    end

    def initialize(opt)
      @daemonize = opt[:daemonize]
      @supervise = opt[:supervise]
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

      @log_level = opt[:log_level]
      @suppress_interval = opt[:suppress_interval]
      @suppress_config_dump = opt[:suppress_config_dump]
      @without_source = opt[:without_source]

      log_opts = {suppress_repeated_stacktrace: opt[:suppress_repeated_stacktrace]}
      @log = LoggerInitializer.new(@log_path, @log_level, @chuser, @chgroup, log_opts)
      @finished = false
      @main_pid = nil
    end

    def start
      @log.init
      show_plugin_config if @show_plugin_config
      read_config
      set_system_config

      dry_run if @dry_run
      start_daemonize if @daemonize
      setup_rpc_server if @rpc_endpoint
      setup_rpc_get_dump if @enable_get_dump

      if @supervise
        install_supervisor_signal_handlers
        run_rpc_server if @rpc_endpoint
        until @finished
          supervise do
            change_privilege
            init_engine
            install_main_process_signal_handlers
            run_configure
            finish_daemonize if @daemonize
            run_engine
            exit 0
          end
          $log.error "fluentd main process died unexpectedly. restarting." unless @finished
        end
      else
        $log.info "starting fluentd-#{Fluent::VERSION} without supervision"
        run_rpc_server if @rpc_endpoint
        main_process do
          change_privilege
          init_engine
          install_main_process_signal_handlers
          run_configure
          finish_daemonize if @daemonize
          run_engine
          exit 0
        end
      end
      stop_rpc_server if @rpc_endpoint
    end

    def options
      {
        'config_path' => @config_path,
        'pid_file' => @daemonize,
        'plugin_dirs' => @plugin_dirs,
        'log_path' => @log_path
      }
    end

    private

    def dry_run
      $log.info "starting fluentd-#{Fluent::VERSION} as dry run mode"

      change_privilege
      init_engine
      install_main_process_signal_handlers
      run_configure
      exit 0
    rescue => e
      $log.error "dry run failed: #{e}"
      exit 1
    end

    def show_plugin_config
      $log.info "Show config for #{@show_plugin_config}"
      name, type = @show_plugin_config.split(":")
      plugin = Plugin.__send__("new_#{name}", type)
      dumped_config = "\n"
      level = 0
      plugin.class.ancestors.reverse_each do |plugin_class|
        if plugin_class.respond_to?(:dump)
          $log.on_debug do
            dumped_config << plugin_class.name
            dumped_config << "\n"
            level = 1
          end
          dumped_config << plugin_class.dump(level)
        end
      end
      $log.info dumped_config
      exit 0
    rescue => e
      $log.error "show config failed: #{e}"
      exit 1
    end

    def start_daemonize
      @wait_daemonize_pipe_r, @wait_daemonize_pipe_w = IO.pipe

      if fork
        # console process
        @wait_daemonize_pipe_w.close
        @wait_daemonize_pipe_w = nil
        wait_daemonize
        exit 0
      end

      # daemonize intermediate process
      @wait_daemonize_pipe_r.close
      @wait_daemonize_pipe_r = nil

      # in case the child process forked during run_configure
      @wait_daemonize_pipe_w.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)

      Process.setsid
      exit!(0) if fork
      File.umask(0)

      # supervisor process
      @supervisor_pid = Process.pid
    end

    def wait_daemonize
      supervisor_pid = @wait_daemonize_pipe_r.read
      if supervisor_pid.empty?
        # initialization failed
        exit! 1
      end

      @wait_daemonize_pipe_r.close
      @wait_daemonize_pipe_r = nil

      # write pid file
      File.open(@daemonize, "w") {|f|
        f.write supervisor_pid
      }
    end

    def finish_daemonize
      if @wait_daemonize_pipe_w
        STDIN.reopen("/dev/null")
        STDOUT.reopen("/dev/null", "w")
        STDERR.reopen("/dev/null", "w")
        @wait_daemonize_pipe_w.write @supervisor_pid.to_s
        @wait_daemonize_pipe_w.close
        @wait_daemonize_pipe_w = nil
      end
    end

    def setup_rpc_server
      @rpc_server = RPC::Server.new(@rpc_endpoint, $log)

      # built-in RPC for signals
      @rpc_server.mount_proc('/api/processes.interruptWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.interruptWorkers request"
        supervisor_sigint_handler
        nil
      }
      @rpc_server.mount_proc('/api/processes.killWorkers') { |req, res|
        $log.debug "fluentd RPC got /api/processes.killWorkers request"
        supervisor_sigterm_handler
        nil
      }
      @rpc_server.mount_proc('/api/plugins.flushBuffers') { |req, res|
        $log.debug "fluentd RPC got /api/plugins.flushBuffers request"
        supervisor_sigusr1_handler
        nil
      }
      @rpc_server.mount_proc('/api/config.reload') { |req, res|
        $log.debug "fluentd RPC got /api/config.reload request"
        $log.info "restarting"
        supervisor_sighup_handler
        nil
      }
      @rpc_server.mount_proc('/api/config.dump') { |req, res|
        $log.debug "fluentd RPC got /api/config.dump request"
        $log.info "dump in-memory config"
        supervisor_dump_config_handler
        nil
      }
    end

    def setup_rpc_get_dump
      @rpc_server.mount_proc('/api/config.getDump') { |req, res|
        $log.debug "fluentd RPC got /api/config.dump request"
        $log.info "get dump in-memory config via HTTP"
        res.body = supervisor_get_dump_config_handler
        [nil, nil, res]
      }
    end

    def run_rpc_server
      @rpc_server.start
    end

    def stop_rpc_server
      @rpc_server.shutdown
    end

    def supervise(&block)
      start_time = Time.now

      Process.setproctitle("supervisor:#{@process_name}") if @process_name
      $log.info "starting fluentd-#{Fluent::VERSION}"
      @main_pid = fork do
        main_process(&block)
      end

      if @daemonize && @wait_daemonize_pipe_w
        STDIN.reopen("/dev/null")
        STDOUT.reopen("/dev/null", "w")
        STDERR.reopen("/dev/null", "w")
        @wait_daemonize_pipe_w.close
        @wait_daemonize_pipe_w = nil
      end

      Process.waitpid(@main_pid)
      @main_pid = nil
      exitstatus = $?.exitstatus
      signal = $?.termsig

      $log.info "process finished", status: exitstatus, signal: signal

      if !@finished && Time.now - start_time < 1
        $log.warn "process died within 1 second. exit."
        exit exitstatus || 1
      end
    end

    def main_process(&block)
      Process.setproctitle("worker:#{@process_name}") if @process_name

      begin
        block.call

      rescue Fluent::ConfigError
        $log.error "config error", file: @config_path, error: $!.to_s
        $log.debug_backtrace
        unless @log.stdout?
          console = Fluent::Log.new(STDOUT, @log_level).enable_debug
          console.error "config error", file: @config_path, error: $!.to_s
          console.debug_backtrace
        end

      rescue
        $log.error "unexpected error", error: $!.to_s
        $log.error_backtrace
        unless @log.stdout?
          console = Fluent::Log.new(STDOUT, @log_level).enable_debug
          console.error "unexpected error", error: $!.to_s
          console.error_backtrace
        end
      end

      exit! 1
    end

    def install_supervisor_signal_handlers
      trap :INT do
        $log.debug "fluentd supervisor process get SIGINT"
        supervisor_sigint_handler
      end

      trap :TERM do
        $log.debug "fluentd supervisor process get SIGTERM"
        supervisor_sigterm_handler
      end

      trap :HUP do
        $log.debug "fluentd supervisor process get SIGHUP"
        $log.info "restarting"
        supervisor_sighup_handler
      end

      trap :USR1 do
        $log.debug "fluentd supervisor process get SIGUSR1"
        supervisor_sigusr1_handler
      end
    end

    def supervisor_sigint_handler
      @finished = true
      if pid = @main_pid
        # kill processes only still exists
        unless Process.waitpid(pid, Process::WNOHANG)
          begin
            Process.kill(:INT, pid)
          rescue Errno::ESRCH
            # ignore processes already died
          end
        end
      end
    end

    def supervisor_sigterm_handler
      @finished = true
      if pid = @main_pid
        # kill processes only still exists
        unless Process.waitpid(pid, Process::WNOHANG)
          begin
            Process.kill(:TERM, pid)
          rescue Errno::ESRCH
            # ignore processes already died
          end
        end
      end
    end

    def supervisor_sighup_handler
      # Creating new thread due to mutex can't lock
      # in main thread during trap context
      Thread.new {
        read_config
        set_system_config
        if pid = @main_pid
          Process.kill(:TERM, pid)
          # don't resuce Erro::ESRSH here (invalid status)
        end
      }.run
    end

    def supervisor_sigusr1_handler
      @log.reopen!
      if pid = @main_pid
        Process.kill(:USR1, pid)
        # don't resuce Erro::ESRSH here (invalid status)
      end
    end

    def supervisor_dump_config_handler
      $log.info @conf.to_s
    end

    def supervisor_get_dump_config_handler
      {conf: @conf.to_s}
    end

    def read_config
      $log.info "reading config file", path: @config_path
      @config_fname = File.basename(@config_path)
      @config_basedir = File.dirname(@config_path)
      @config_data = File.read(@config_path)
      if @inline_config == '-'
        @config_data << "\n" << STDIN.read
      elsif @inline_config
        @config_data << "\n" << @inline_config.gsub("\\n","\n")
      end
      @conf = Fluent::Config.parse(@config_data, @config_fname, @config_basedir, @use_v1_config)
    end

    def set_system_config
      @system_config = SystemConfig.create(@conf) # @conf is set in read_config
      @system_config.apply(self)
    end

    def run_configure
      Fluent::Engine.run_configure(@conf)
    end

    def change_privilege
      if @chgroup
        etc_group = Supervisor.get_etc_group(@chgroup)
        Process::GID.change_privilege(etc_group.gid)
      end

      if @chuser
        etc_pw = Supervisor.get_etc_passwd(@chuser)
        user_groups = [etc_pw.gid]
        Etc.setgrent
        Etc.group { |gr| user_groups << gr.gid if gr.mem.include?(etc_pw.name) } # emulate 'id -G'

        Process.groups = Process.groups | user_groups
        Process::UID.change_privilege(etc_pw.uid)
      end
    end

    def init_engine
      Fluent::Engine.init(@system_config)

      @libs.each {|lib|
        require lib
      }

      @plugin_dirs.each {|dir|
        if Dir.exist?(dir)
          dir = File.expand_path(dir)
          Fluent::Engine.load_plugin_dir(dir)
        end
      }
    end

    def install_main_process_signal_handlers
      # Strictly speaking, these signal handling is not thread safe.
      # But enough safe to limit twice call of Fluent::Engine.stop.

      trap :INT do
        $log.debug "fluentd main process get SIGINT"
        unless @finished
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

      trap :HUP do
        # TODO
        $log.debug "fluentd main process get SIGHUP"
      end

      trap :USR1 do
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
    end

    def run_engine
      Fluent::Engine.run
    end
  end
end
