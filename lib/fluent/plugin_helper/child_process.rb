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

require 'fluent/plugin_helper/thread'
require 'fluent/plugin_helper/timer'

module Fluent
  module PluginHelper
    module ChildProcess
      include Fluent::PluginHelper::Thread
      include Fluent::PluginHelper::Timer

      CHILD_PROCESS_LOOP_CHECK_INTERVAL = 0.2 # sec
      CHILD_PROCESS_DEFAULT_KILL_TIMEOUT = 60 # sec

      # stop     : [-]
      # shutdown : send TERM to all child processes
      # close    : close all I/O objects for child processes
      # terminate: send TERM again and again if processes stil exist, and KILL after timeout

      def child_process_execute(title, command, arguments: nil, subprocess_name: nil, read: true, write: true, encoding: 'utf-8', interval: nil, immediate: false, &block)
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: both of input and output are disabled" if !read && !write
        raise ArgumentError, "BUG: block not specified which receive i/o object" unless block_given?
        raise ArgumentError, "BUG: block must have an argument for io" unless block.arity == 1

        if interval
          if immediate
            child_process_execute_once(title, command, arguments, subprocess_name, read, write, encoding, &block)
          end
          timer_execute(:child_process_execute, interval, repeat: true) do
            child_process_execute_once(title, command, arguments, subprocess_name, read, write, encoding, &block)
          end
        else
          child_process_execute_once(title, command, arguments, subprocess_name, read, write, encoding, &block)
        end
      end

      def initialize
        super
        # plugins MAY configure this parameter
        @_child_process_kill_timeout = CHILD_PROCESS_DEFAULT_KILL_TIMEOUT
      end

      def start
        super
        @_child_process_processes = {} # pid => thread
      end

      def shutdown
        @_child_process_processes.keys.each do |pid|
          begin
            if Process.waitpid(pid, Process::WNOHANG)
              @_child_process_processes[pid].alive = false
            else
              Process.kill(:TERM, pid) rescue nil
            end
          rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM => e
            @_child_process_processes[pid].alive = false
          end
        end

        super
      end

      def close
        super

        @_child_process_processes.keys.each do |pid|
          io = @_child_process_processes[pid].io
          io.close rescue nil
        end
      end

      def terminate
        alive_process_exist = true
        timeout = Time.now + @_child_process_kill_timeout

        while alive_process_exist
          @_child_process_processes.keys.each do |pid|
            process = @_child_process_processes[pid]
            next unless process.alive
            begin
              if Process.waitpid(pid, Process::WNOHANG)
                process.alive = false
              else
                Process.kill(:TERM, pid) rescue nil
              end
            rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM => e
              process.alive = false
            end
          end

          alive_process_found = false
          @_child_process_processes.each_pair do |pid, process|
            alive_process_found = true if process.alive
          end

          if alive_process_found
            sleep CHILD_PROCESS_LOOP_CHECK_INTERVAL
          else
            alive_process_exist = false
          end
        end

        @_child_process_processes.keys.each do |pid|
          begin
            Process.kill(:KILL, pid) unless Process.waitpid(pid, Process::WNOHANG)
          rescue => e
            # ignore all errors
          end
          @_child_process_processes.delete(pid)
        end

        super
      end

      ProcessInfo = Struct.new(:thread, :io, :alive)

      def child_process_execute_once(title, command, arguments, subprocess_name, read, write, encoding, &block)
        ext_enc = encoding
        int_enc = "utf-8"
        mode = case
               when read && write then "r+"
               when read then "r"
               when write then "w"
               else
                 raise "BUG: both read and write are false is banned before here"
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
        log.debug "Executing command", title: title, command: command, arguments: arguments, read: read, write: write
        io = IO.popen(cmd, mode, external_encoding: ext_enc, internal_encoding: int_enc)
        pid = io.pid

        m = Mutex.new
        m.lock
        thread = thread_create do
          m.lock # run after plugin thread get pid, thread instance and i/o
          with_error = false
          m.unlock
          begin
            block.call(io)
          rescue EOFError => e
            log.debug "Process exit and I/O closed", title: title, pid: pid, command: command, arguments: arguments
          rescue IOError => e
            if e.message == 'stream closed'
              log.debug "Process I/O stream closed", title: title, pid: pid, command: command, arguments: arguments
            else
              log.error "Unexpected I/O error for child process", title: title, pid: pid, command: command, arguments: arguments, error_class: e.class, error: e
            end
          rescue => e
            log.warn "Unexpected error while processing I/O for child process", title: title, pid: pid, command: command, error_class: e.class, error: e
          end
        end
        @_child_process_processes[pid] = ProcessInfo.new(thread, io, true)
        m.unlock
        pid
      end
    end
  end
end
