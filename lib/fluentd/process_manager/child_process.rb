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
  module ProcessManager

    class ChildProcess
      def initialize(manager, conf, processor_id)
        @manager = manager
        @conf = conf
        @log = conf.logger
        @processor_id = processor_id
        @agents = []
        @local_process = false

        @last_heartbeat_time = Time.now.to_i
        @kill_start_time = nil
        @last_kill_time = nil
        @kill_immediate = false
      end

      attr_reader :processor_id
      attr_accessor :last_heartbeat_time

      def assign_agent(agent)
        @agents << agent
      end

      def init_exchange
        @local_ipx = InterProcessExchange.new(@processor_id)
        return @local_ipx
      end

      def local_process?
        @local_process
      end

      def start
        @pid = @manager.fork_processor(self) do |hmc,smc|
          @local_process = true
          $0 = "fluentd-processor:#{@processor_id}"
          @log.info "running fluentd-processor:#{@processor_id} pid=#{Process.pid}"
          m = Main.new(@agents, @local_ipx, hmc, smc)
          m.configure(@conf)
          m.run
          exit 0
        end
      end

      def stop(immediate)
        if immediate && !@kill_immediate
          @kill_immediate = true  # escalation
        elsif @kill_start_time
          return
        end

        now = Time.now.to_i
        kill_child(now, nil)
      end

      def join
        while @pid
          sleep 0.5
        end
      end

      def try_join(kill_interval, graceful_kill_limit, heartbeat_limit)
        pid = @pid
        return false unless pid

        begin
          unless Process.waitpid(pid, Process::WNOHANG)
            maintain_child(kill_interval, graceful_kill_limit, heartbeat_limit)
            return true
          end
        rescue Errno::ECHILD
          # TODO? SIGCHLD is trapped in Supervisor#install_signal_handlers
        end

        @log.info "Processor exited pid=#{@pid} processor=#{@processor_id}"
        @pid = nil

        return nil
      end

      def maintain_child(kill_interval, graceful_kill_limit, heartbeat_limit)
        now = Time.now.to_i
        if @kill_start_time
          if @last_kill_time + kill_interval <= now
            kill_child(now, graceful_kill_limit)
          end
        else
          if now - @last_heartbeat_time > heartbeat_limit
            @log.fatal "heartbeat broke out process=#{@processor_id}"
            stop(true)
          end
        end
      end

      def kill_child(now, graceful_kill_limit)
        pid = @pid
        return unless pid

        @kill_start_time ||= now
        @last_kill_time = now

        begin
          if @kill_immediate || (graceful_kill_limit && @kill_start_time + graceful_kill_limit < now)
            @log.info "sending SIGKILL to pid=#{pid} process=#{@processor_id} for immediate stop"
            Process.kill(:KILL, pid)
          else
            @log.info "sending SIGTERM to pid=#{pid} process=#{@processor_id} for graceful stop"
            Process.kill(:TERM, pid)
          end
        rescue Errno::ESRCH, Errno::EPERM
          @log.warn "kill failed pid=#{@pid} process=#{@processor_id}: #{$!}"
        end
      end

      def open_remote_self(local_self, tag)
        @local_ipx.open_remote_self(local_self, tag)
      end


      class Main
        def initialize(agents, local_ipx, hmc, smc)
          @agents = agents
          @local_ipx = local_ipx
          @hmc = hmc
          @smc = smc
          @finish_flag = BlockingFlag.new
          @heartbeat_time = Time.now.to_i
          @suicide_start_time = nil

          @started_agents = []
        end

        def configure(conf)
          @log = conf.logger

          # TODO configure
          @child_heartbeat_interval = 1
        end

        def run
          install_signal_handlers

          if @local_ipx
            @local_ipx.start
            # TODO FIXME wait other process starts to not cause ENOENT error on InterProcessExchange#open_remote_self
            sleep 0.5
          end

          @agents.each {|agent|
            agent.start
            @started_agents << agent
          }

          # send heartbeat messages via SocketManager
          run_parent_heartbeat

          if @local_ipx
            @local_ipx.join
          end

        rescue
          @log.error $!
          @log.error_backtrace
        end

        def run_parent_heartbeat
          until @finish_flag.set?
            @finish_flag.wait(0.5)

            now = Time.now.to_i
            if @heartbeat_time + @child_heartbeat_interval < now
              @heartbeat_time = now

              begin
                @hmc.heartbeat
              rescue
                unless @suicide_start_time
                  @log.fatal "Can't reach to the parent process: #{$!} pid=#{Process.pid}"
                  @suicide_start_time = now
                end
                if @suicide_start_time + 60 < now
                  @log.info "sending SIGKILL to self for immediate stop pid=#{Process.pid}"
                  Process.kill(:KILL, Process.pid)
                else
                  @log.info "sending SIGTERM to self for immediate stop pid=#{Process.pid}"
                  Process.kill(:TERM, Process.pid)
                end

              end
            end
          end
        end

        def stop
          @finish_flag.set!

          @started_agents.each {|agent|
            agent.stop
          }
          @agents.each {|agent|
            agent.shutdown
          }

          if @local_ipx
            @local_ipx.stop_service
          end
        end

        def logrotate
          @log.reopen! if @log
        end

        def install_signal_handlers(&block)
          SignalQueue.start do |sig|
            sig.trap :TERM do
              stop
            end

            sig.trap :INT do
              stop
            end

            trap :QUIT, 'SIG_DFL'

            sig.trap :USR1 do
              stop
            end

            sig.trap :HUP do
              stop
            end

            sig.trap :USR2 do
              logrotate
            end

            trap :CHLD, "SIG_DFL"
          end
        end
      end
    end

  end
end

