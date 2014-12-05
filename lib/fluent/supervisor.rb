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

require 'fluent/load'
require 'etc'

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
        :config_path => Fluent::DEFAULT_CONFIG_PATH,
        :plugin_dirs => [Fluent::DEFAULT_PLUGIN_DIR],
        :log_level => Fluent::Log::LEVEL_INFO,
        :log_path => nil,
        :daemonize => nil,
        :libs => [],
        :setup_path => nil,
        :chuser => nil,
        :chgroup => nil,
        :suppress_interval => 0,
        :suppress_repeated_stacktrace => true,
        :without_source => false,
        :use_v1_config => true,
        :supervise => true,
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
      @libs = opt[:libs]
      @plugin_dirs = opt[:plugin_dirs]
      @chgroup = opt[:chgroup]
      @chuser = opt[:chuser]

      @log_level = opt[:log_level]
      @suppress_interval = opt[:suppress_interval]
      @suppress_config_dump = opt[:suppress_config_dump]
      @without_source = opt[:without_source]

      log_opts = {:suppress_repeated_stacktrace => opt[:suppress_repeated_stacktrace]}
      @log = LoggerInitializer.new(@log_path, @log_level, @chuser, @chgroup, log_opts)
      @finished = false
      @main_pid = nil
    end

    def start
      @log.init
      read_config
      apply_system_config

      dry_run if @dry_run
      start_daemonize if @daemonize
      if @supervise
        install_supervisor_signal_handlers
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

    def supervise(&block)
      start_time = Time.now

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
      ecode = $?.to_i

      $log.info "process finished", :code=>ecode

      if !@finished && Time.now - start_time < 1
        $log.warn "process died within 1 second. exit."
        exit ecode
      end
    end

    def main_process(&block)
      begin
        block.call

      rescue Fluent::ConfigError
        $log.error "config error", :file=>@config_path, :error=>$!.to_s
        $log.debug_backtrace
        unless @log.stdout?
          console = Fluent::Log.new(STDOUT, @log_level).enable_debug
          console.error "config error", :file=>@config_path, :error=>$!.to_s
          console.debug_backtrace
        end

      rescue
        $log.error "unexpected error", :error=>$!.to_s
        $log.error_backtrace
        unless @log.stdout?
          console = Fluent::Log.new(STDOUT, @log_level).enable_debug
          console.error "unexpected error", :error=>$!.to_s
          console.error_backtrace
        end
      end

      exit! 1
    end

    def install_supervisor_signal_handlers
      trap :INT do
        $log.debug "fluentd supervisor process get SIGINT"
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

      trap :TERM do
        $log.debug "fluentd supervisor process get SIGTERM"
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

      trap :HUP do
        $log.debug "fluentd supervisor process get SIGHUP"
        $log.info "restarting"
        if pid = @main_pid
          Process.kill(:TERM, pid)
          # don't resuce Erro::ESRSH here (invalid status)
        end
      end

      trap :USR1 do
        $log.debug "fluentd supervisor process get SIGUSR1"
        @log.reopen!
        if pid = @main_pid
          Process.kill(:USR1, pid)
          # don't resuce Erro::ESRSH here (invalid status)
        end
      end
    end

    def read_config
      $log.info "reading config file", :path => @config_path
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

    class SystemConfig
      include Configurable

      config_param :log_level, :default => nil do |level|
        Log.str_to_level(level)
      end
      config_param :suppress_repeated_stacktrace, :bool, :default => nil
      config_param :emit_error_log_interval, :time, :default => nil
      config_param :suppress_config_dump, :bool, :default => nil
      config_param :without_source, :bool, :default => nil

      def initialize(conf)
        super()
        configure(conf)
      end

      def apply(supervisor)
        system = self
        supervisor.instance_eval {
          @log.level = @log_level = system.log_level unless system.log_level.nil?
          @suppress_interval = system.emit_error_log_interval unless system.emit_error_log_interval.nil?
          @suppress_config_dump = system.suppress_config_dump unless system.suppress_config_dump.nil?
          @suppress_repeated_stacktrace = system.suppress_repeated_stacktrace unless system.suppress_repeated_stacktrace.nil?
          @without_source = system.without_source unless system.without_source.nil?
        }
      end
    end

    # TODO: this method should be moved to SystemConfig class method
    def apply_system_config
      systems = @conf.elements.select { |e|
        e.name == 'system'
      }
      return if systems.empty?
      raise ConfigError, "<system> is duplicated. <system> should be only one" if systems.size > 1

      SystemConfig.new(systems.first).apply(self)
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
      init_opts = {:suppress_interval => @suppress_interval, :suppress_config_dump => @suppress_config_dump, :without_source => @without_source}
      Fluent::Engine.init(init_opts)

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
