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

require 'fluent/plugin_support/thread'
require 'fluent/plugin_support/timer'

module Fluent
  module PluginSupport
    module ChildProcess
      include Fluent::PluginSupport::Thread
      include Fluent::PluginSupport::Timer

      CHILD_PROCESS_LOOP_CHECK_INTERVAL = 0.2 # sec
      CHILD_PROCESS_DEFAULT_KILL_TIMEOUT = 60 # sec

      def child_process_execute(command, arguments: nil, subprocess_name: nil, read: true, write: true, encoding: 'utf-8', interval: nil, immediate: false, &block)
        raise "BUG: illegal specification to disable both of input and output file handle" if !read && !write
        raise "BUG: block not specified which receive i/o object" unless block_given?

        if interval
          if immediate
            child_process_execute_once(command, arguments, subprocess_name, read, write, encoding, &block)
          end
          timer_execute(interval: interval, repeat: true) do
            child_process_execute_once(command, arguments, subprocess_name, read, write, encoding, &block)
          end
        else
          child_process_execute_once(command, arguments, subprocess_name, read, write, encoding, &block)
        end
      end

      def initialize
        super
      end

      def start
        super
        @_child_process_processes = {} # pid => thread
      end

      def stop
        super
      end

      def shutdown
        @_child_process_processes.keys.each do |pid|
          process_alive = true
          begin
            if Process.waitpid(pid, Process::WNOHANG)
              Process.kill(:TERM, pid)
              process_alive = false
            end
          rescue
            # Errno::ECHILD, Errno::ESRCH, Errno::EPERM
            # child process already doesn't exist
            process_alive = false
          end
          unless process_alive
            begin
              @_child_process_processes[pid][:io].close
            rescue
              # ignore
            end
            @_child_process_processes.delete(pid)
          end
        end

        super
      end

      def close
        super

        @_child_process_processes.keys.each do |pid|
          io = @_child_process_processes[pid][:io]
          begin
            io.close
          rescue => e
            # just ignore
          end
        end
      end

      def terminate
        @_child_process_processes.keys.each do |pid|
          begin
            Process.kill(:KILL, pid)
          rescue
            # ignore even if process already doesn't exist
          end
          @_child_process_processes.delete(pid)
        end

        super
      end

      # for private use
      def child_process_execute_once(command, arguments, subprocess_name, read, write, encoding, &block)
        ext_enc = encoding
        int_enc = "utf-8"
        mode = case
               when read && write then "r+"
               when read then "r"
               when write then "w"
               else "r" # but it is not for any bytes
               end
        cmd = command
        if arguments # if arguments is nil, then command is executed by shell
          cmd = []
          if subprocess_name
            cmd.push [command, subprocess_name]
          else
            cmd.push command
          end
          cmd += arguments
        end
        io = IO.popen(cmd, mode, external_encoding: ext_enc, internal_encoding: int_enc)
        pid = io.pid

        thread = thread_create do
          block.call(io) # TODO: rescue and log? or kill entire process?

          # child process probablly ended if sequence reaches here
          begin
            while thread_current_running? && Process.waitpid(pid, Process::WNOHANG)
              sleep CHILD_PROCESS_LOOP_CHECK_INTERVAL
            end
            if Process.waitpid(pid, Process::WNOHANG)
              Process.kill(:TERM, pid)
            end
          rescue Errno::ECHILD, Errno::ESRCH => e
            # child process already died
          end
          begin
            io.close
          rescue => e
            # ignore all errors because io already isn't connected anywhere
          end
          @_child_process_processes.delete(pid)
        end

        @_child_process_processes[pid] = {thread: thread, io: io}
      end
    end
  end
end
