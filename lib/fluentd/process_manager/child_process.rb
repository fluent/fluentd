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
      def initialize(manager, processor_id)
        @manager = manager
        @processor_id = processor_id
        @agents = []
        @local_process = false

        @kill_start_time = nil
        @last_kill_time = nil
        @kill_immediate = false
      end

      attr_reader :processor_id

      def assign_agent(agent)
        @agents << agent
      end

      def init_exchange(config)
        @local_ipx = InterProcessExchange.new(config, @processor_id)
        return @local_ipx
      end

      def local_process?
        @local_process
      end

      def start
        @pid = @manager.fork_processor(self) do |smc|
          @local_process = true
          $0 = "fluentd-processor:#{@processor_id}"
          puts "running fluentd-processor:#{@processor_id} pid=#{Process.pid}"
          Main.run(@agents, @local_ipx, smc)
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

      def try_join(kill_interval, graceful_kill_limit)
        pid = @pid
        return false unless pid

        begin
          unless Process.waitpid(pid, Process::WNOHANG)
            maintain_child(kill_interval, graceful_kill_limit)
            return true
          end
        rescue Errno::ECHILD
          # TODO? SIGCHLD is trapped in Supervisor#install_signal_handlers
        end

        # TODO @log.info "Processor exited pid=#{@pid}"
        @pid = nil

        return nil
      end

      def maintain_child(kill_interval, graceful_kill_limit)
        if @kill_start_time
          now = Time.now.to_i
          if @last_kill_time + kill_interval <= now
            kill_child(now, graceful_kill_limit)
          end
        end
      end

      def kill_child(now, graceful_kill_limit)
        pid = @pid
        return unless pid

        begin
          if @kill_immediate || (graceful_kill_limit && @kill_start_time + graceful_kill_limit < now)
            # TODO @log.debug "sending SIGKILL to pid=#{pid} for immediate stop"
            puts "sending SIGKILL to pid=#{pid} for immediate stop"
            Process.kill(:KILL, pid)
          else
            # TODO @log.debug "sending SIGTERM to pid=#{pid} for graceful stop"
            puts "sending SIGTERM to pid=#{pid} for graceful stop"
            Process.kill(:TERM, pid)
          end
        rescue Errno::ESRCH, Errno::EPERM
          puts "killfailed: #{@pid}"
          # TODO log?
        end
        @last_kill_time = now
        @kill_start_time ||= now
      end

      def open_remote_self(local_self, tag)
        @local_ipx.open_remote_self(local_self, tag)
      end


      class Main
        def self.run(*args)
          new(*args).run
        end

        def initialize(agents, local_ipx, smc)
          @agents = agents
          @local_ipx = local_ipx
          @finish_flag = BlockingFlag.new
          @smc = smc

          @started_agents = []
        end

        def run
          install_signal_handlers

          if @local_ipx
            @local_ipx.start
            sleep 0.5  # TODO wait other process starts
          end

          @agents.each {|agent|
            agent.start
            @started_agents << agent
          }

          if @local_ipx
            @local_ipx.join
          end

          until @finish_flag.set?
            @finish_flag.wait(0.5)
          end

        rescue
          puts $!
          $!.backtrace.each {|bt| puts bt }
          # TODO log
        end

        def stop
          @started_agents.each {|agent|
            agent.stop
          }
          @agents.each {|agent|
            agent.shutdown
          }

          if @local_ipx
            @local_ipx.stop_service
          end

          @finish_flag.set!
        end

        def logrotate
          # TODO
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

