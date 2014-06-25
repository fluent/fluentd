#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent
  class Supervisor
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
            chuid = @chuser ? `id -u #{@chuser}`.to_i : nil
            chgid = @chgroup ? `id -g #{@chgroup}`.to_i : nil
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
    end

    def initialize(opt)
      @config_path = opt[:config_path]
      @log_path = opt[:log_path]
      @log_level = opt[:log_level]
      @daemonize = opt[:daemonize]
      @chgroup = opt[:chgroup]
      @chuser = opt[:chuser]
      @libs = opt[:libs]
      @plugin_dirs = opt[:plugin_dirs]
      @inline_config = opt[:inline_config]
      @suppress_interval = opt[:suppress_interval]
      @dry_run = opt[:dry_run]
      @suppress_config_dump = opt[:suppress_config_dump]
      @use_v1_config = opt[:use_v1_config]
      @usespawn = opt[:usespawn]
      @signame = opt[:signame]

      $platformwin = false
      if RUBY_PLATFORM.downcase =~ /mswin(?!ce)|mingw|cygwin|bccwin/
        $platformwin = true
      end
      if $platformwin
        require 'fluent/win32api_syncobj'
        ruby_path = Fluent::Win32Dll.getmodulefilename
        @rubybin_dir = ruby_path[0, ruby_path.rindex("/")]
        @winosvi = Fluent::Win32Base.getversionex
      end
      log_opts = {:suppress_repeated_stacktrace => opt[:suppress_repeated_stacktrace]}
      @log = LoggerInitializer.new(@log_path, @log_level, @chuser, @chgroup, log_opts)
      @finished = false
      @main_pid = nil
    end

    def start
      require 'fluent/load'
      @log.init

      dry_run if @dry_run
      start_daemonize if @daemonize
      unless $platformwin
        install_supervisor_signal_handlers
      else
        if @usespawn == 0
          install_supervisor_winsigint_handler
          install_supervisor_signal_handlers
        end
      end

      until @finished
        supervise do
          read_config
          change_privilege
          init_engine
          unless $platformwin
            install_main_process_signal_handlers
          else
            install_main_process_winsigint_handler
            install_main_process_signal_handlers
          end
          run_configure
          finish_daemonize if @daemonize
          run_engine
          exit 0
        end
        $log.error "fluentd main process died unexpectedly. restarting." unless @finished
      end
    end

    private

    def dry_run
      read_config
      change_privilege
      init_engine
      install_main_process_signal_handlers
      run_configure
      exit 0
    rescue => e
      $log.error "Dry run failed: #{e}"
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
      $log.info "is windows platform : #{$platformwin}"

      unless $platformwin
        @main_pid = fork do
          main_process(&block)
        end
      else
        if @usespawn == 0
          fluentd_spawn_cmd = @rubybin_dir+"/ruby.exe '"+@rubybin_dir+"/fluentd' "
          $fluentdargv.each{|a|
            fluentd_spawn_cmd << (a + " ")
          }
          fluentd_spawn_cmd << "-u"
          $log.info "spawn command to main (windows) : " + fluentd_spawn_cmd
          @main_pid = Process.spawn(fluentd_spawn_cmd)
        else
          main_process(&block)
        end
      end

      if @daemonize && @wait_daemonize_pipe_w
        STDIN.reopen("/dev/null")
        STDOUT.reopen("/dev/null", "w")
        STDERR.reopen("/dev/null", "w")
        @wait_daemonize_pipe_w.close
        @wait_daemonize_pipe_w = nil
      end

      unless $platformwin
        Process.waitpid(@main_pid)
        @main_pid = nil
      else
        if @usespawn == 0
          Process.waitpid(@main_pid)
        end
      end

      ecode = $?.to_i

      if $platformwin
        @th_sv.join
      end

      $log.info "process finished", :code=>ecode

      if !@finished && Time.now - start_time < 1
        $log.warn "process died within 1 second. exit."
        exit ecode
      end
    end

    def main_process(&block)
      begin
        block.call
        if $platformwin
          @th_ma.join
        end

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
        unless $platformwin
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
        else
          @winsigintevt.signal_on
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

      unless $platformwin
        trap :HUP do
          $log.debug "fluentd supervisor process get SIGHUP"
          $log.info "restarting"
          if pid = @main_pid
            Process.kill(:TERM, pid)
            # don't resuce Erro::ESRSH here (invalid status)
          end
        end
      end

      unless $platformwin
        trap :USR1 do
          $log.debug "fluentd supervisor process get SIGUSR1"
          @log.reopen!
          if pid = @main_pid
            Process.kill(:USR1, pid)
            # don't resuce Erro::ESRSH here (invalid status)
          end
        end
      end
    end

    def read_config
      $log.info "reading config file", :path=>@config_path
      @config_fname = File.basename(@config_path)
      @config_basedir = File.dirname(@config_path)
      @config_data = File.read(@config_path)
      if @inline_config == '-'
        @config_data << "\n" << STDIN.read
      elsif @inline_config
        @config_data << "\n" << @inline_config.gsub("\\n","\n")
      end
    end

    def run_configure
      Fluent::Engine.parse_config(@config_data, @config_fname, @config_basedir, @use_v1_config)
    end

    def change_privilege
      if @chgroup
        chgid = @chgroup.to_i
        if chgid.to_s != @chgroup
          chgid = `id -g #{@chgroup}`.to_i
          if $?.to_i != 0
            exit 1
          end
        end
        Process::GID.change_privilege(chgid)
      end

      if @chuser
        chuid = @chuser.to_i
        if chuid.to_s != @chuser
          chuid = `id -u #{@chuser}`.to_i
          if $?.to_i != 0
            exit 1
          end
        end

        user_groups = `id -G #{@chuser}`.split.map(&:to_i)
        if $?.to_i != 0
          exit 1
        end

        Process.groups = Process.groups | user_groups
        Process::UID.change_privilege(chuid)
      end
    end

    def init_engine
      require 'fluent/load'
      Fluent::Engine.init
      if @suppress_interval
        Fluent::Engine.suppress_interval(@suppress_interval)
      end

      Fluent::Engine.suppress_config_dump = @suppress_config_dump

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
        unless $platformwin
          unless @finished
            @finished = true
            $log.debug "getting start to shutdown main process"
            Fluent::Engine.stop
          end
        else
          @winsigintevt.signal_on
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

      unless $platformwin
        trap :HUP do
          # TODO
          $log.debug "fluentd main process get SIGHUP"
        end
      end

      unless $platformwin
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
    end

    def run_engine
      Fluent::Engine.run
    end

    #
    require 'Win32API'
    require 'fluent/win32api_syncobj.rb'

    def install_supervisor_winsigint_handler
      @winintname = "fluentdwinsigint_#{Process.pid}"
      if @usespawn == 0 && @signame != nil
        @winintname = @signame
      end
      @th_sv = Thread.new do 
        @winsigintevt = Win32SyncObj.createevent(1,0,@winintname)
        @winsigintevt.wait(0)
        @winsigintevt.close
        winsigint_supervisor 
      end

    end

    def install_main_process_winsigint_handler
      @th_ma = Thread.new do
        @winsigintevt = Win32SyncObj.createevent(1,0,"fluentdwinsigint_#{Process.pid}")
        @winsigintevt.wait(0)
        @winsigintevt.close
        winsigint_main_process 
      end
      $log.debug "install_main_process_winsigint_handler***** installed main winsiginthandler"
    end

    def winsigint_supervisor
      @finished = true
      if pid = @main_pid
        unless Process.waitpid(pid, Process::WNOHANG)
          sigx = (@winosvi[1] >= 6 && @winosvi[2] >=2) ? (:INT) : (:KILL)
          begin
            Process.kill(sigx, pid)
          rescue Errno::ESRCH
            # ignore processes already died
          end
        end
      end
    end

    def winsigint_main_process
      unless @finished
        @finished = true
        $log.debug "getting start to shutdown main process"
        Fluent::Engine.stop
      end
    end
  end
end
