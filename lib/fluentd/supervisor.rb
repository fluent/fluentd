#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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


  #def self.run(conf=nil, &block)
  #  Supervisor.run(conf, &block)
  #end

  class Supervisor
    def self.run(conf=nil, &block)
      new(conf, &block).run
    end

    def initialize(conf=nil, &block)
      if conf
        if conf.is_a?(Config)
          block = Proc.new { conf }
        else
          block = Proc.new { Config.parse(conf, '(data)') }
        end
      end
      @config_load_proc = block

      @detach = false
      @finish = false
      @finish_flag = BlockingFlag.new

      # initial logger
      @log = Logger.new(STDERR)
    end

    def configure(conf)
      @child_detach_wait = conf['child_detach_wait'] || 10.0
      @child_restart_wait = conf['child_restart_wait'] || 1.0
      @child_restart_limit = conf['child_restart_limit'] || nil
    end

    def run
      conf = @config_load_proc.call
      configure(conf)

      @pid = start_server

      install_signal_handlers

      until @finish_flag.set?
        @finish_flag.wait(0.5)

        if try_waitpid
          if @finish
            break
          end
          # child process died unexpectedly; reboot process
          @pid = reboot_server
        end
      end

      # check detach
      if @detach
        wait_time = Time.now + @child_detach_wait

        while (w = wait_time - Time.now) > 0
          sleep [0.5, w].min
          if try_waitpid
            break
          end
        end
      end
    end

    def stop(immediate)
      @finish = true
      send_signal(immediate ? :QUIT : :TERM)
    end

    def restart(immediate)
      send_signal(immediate ? :USR1 : :HUP)
    end

    def logrotate
      send_signal(:USR2)
    end

    def detach
      @detach = true
      @finish_flag.set!
      send_signal(:INT)
    end

    private
    def start_server
      # TODO supervisor <-> fluentd-engine heartbeat
      pid = fork do
        $0 = "fluentd-server"
        server = Server.new(&@config_load_proc)
        server.run
        exit! 0
      end
      return pid
    end

    def reboot_server
      restart_limit = @child_restart_limit

      until @finish_flag.set?
        @finish_flag.wait(@child_restart_wait)

        begin
          return start_server
        rescue
          # TODO log
        end

        # TODO child_restart_limit
      end
      return nil
    end

    def try_waitpid
      return true unless @pid

      begin
        pid, status = Process.waitpid2(@pid, Process::WNOHANG)
      rescue Errno::ECHILD
        pid = 0
      end
      if pid
        @pid = nil
        return pid
      else
        return nil
      end
    end

    def send_signal(sig)
      begin
        Process.kill(sig, @pid) if @pid
      rescue Errno::ESRCH, Errno::EPERM
      end
    end

    def install_signal_handlers(&block)
      SignalQueue.start do |sig|
        sig.trap :TERM do
          stop(false)
        end

        sig.trap :INT do
          detach
        end

        sig.trap :QUIT do
          stop(true)
        end

        sig.trap :USR1 do
          restart(true)
        end

        sig.trap :HUP do
          restart(false)
        end

        sig.trap :USR2 do
          logrotate
        end

        trap :CHLD, "SIG_DFL"
      end
    end
  end

end
