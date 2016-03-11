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
      # close    : close all I/O objects for child processes, send TERM and then KILL after timeout
      # terminate: [-]

      attr_reader :_child_process_processes # for tests

      def child_process_execute(
          title, command,
          arguments: nil, subprocess_name: nil, interval: nil, immediate: false, parallel: false,
          read: true, write: true, internal_encoding: 'utf-8', external_encoding: 'utf-8', &block
      )
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: arguments required if subprocess name is replaced" if subprocess_name && !arguments
        raise ArgumentError, "BUG: both of input and output are disabled" if !read && !write
        raise ArgumentError, "BUG: block not specified which receive i/o object" unless block_given?
        raise ArgumentError, "BUG: block must have an argument for io" unless block.arity == 1

        running = false
        callback = ->(io) {
          running = true
          begin
            block.call(io)
          ensure
            running = false
          end
        }

        if immediate || !interval
          child_process_execute_once(title, command, arguments, subprocess_name, read, write, internal_encoding, external_encoding, &callback)
        end

        if interval
          timer_execute(:child_process_execute, interval, repeat: true) do
            if !parallel && running
              log.warn "previous child process is still running. skipped.", title: title, command: command, arguments: arguments, interval: interval, parallel: parallel
            else
              child_process_execute_once(title, command, arguments, subprocess_name, read, write, internal_encoding, external_encoding, &callback)
            end
          end
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

        # close_write -> kill processes -> close_read
        # * if ext process continues to write data to it's stdout, io.close_read blocks
        #   so killing process MUST be done before closing io for read

        @_child_process_processes.keys.each do |pid|
          io = @_child_process_processes[pid].io
          io.close_write rescue nil
        end

        timeout = Time.now + @_child_process_kill_timeout

        while true
          break if Time.now > timeout

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
          break unless alive_process_found

          sleep CHILD_PROCESS_LOOP_CHECK_INTERVAL
        end

        @_child_process_processes.keys.each do |pid|
          begin
            Process.kill(:KILL, pid) unless Process.waitpid(pid, Process::WNOHANG)
          rescue => e
            # ignore all errors
          end
          @_child_process_processes.delete(pid)
        end

        @_child_process_processes.keys.each do |pid|
          io = @_child_process_processes[pid].io
          io.close_read rescue nil
        end
      end

      ProcessInfo = Struct.new(:thread, :io, :alive)

      def child_process_execute_once(title, command, arguments, subprocess_name, read, write, internal_encoding, external_encoding, &block)
        mode = case
               when read && write then "r+"
               when read then "r"
               when write then "w"
               else
                 raise "BUG: both read and write are false is banned before here"
               end
        cmd = command
        if arguments || subprocess_name # if arguments is nil, then command is executed by shell
          cmd = []
          if subprocess_name
            cmd.push [command, subprocess_name]
          else
            cmd.push command
          end
          cmd += (arguments || [])
        end
        log.debug "Executing command", title: title, command: command, arguments: arguments, read: read, write: write
        io = IO.popen(cmd, mode, external_encoding: external_encoding, internal_encoding: internal_encoding)
        pid = io.pid

        m = Mutex.new
        m.lock
        thread = thread_create :child_process_callback do
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
          io.close rescue nil
          @_child_process_processes.delete(pid)
        end
        @_child_process_processes[pid] = ProcessInfo.new(thread, io, true)
        m.unlock
        pid
      end
    end
  end
end
