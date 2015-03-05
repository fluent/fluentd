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

require 'fluent/configurable'
require 'fluent/plugin_support/thread'

module Fluent
  module PluginSupport
    module ChildProcess
      include Fluent::PluginSupport::Thread

      CHILD_PROCESS_LOOP_CHECK_INTERVAL = 0.2

      ChildProcessStat = Struct.new(:processes, :kill_timeout, :periodics, :loop, :shutdown, :mutex)
      PeriodicProc = Struct.new(:spec, :block, :interval, :last_executed)

      def initialize
        super
      end

      def start
        super

        @child_process = ChildProcessStat.new
        @child_process.processes = {} # pid => thread
        @child_process.kill_timeout = 60
        @child_process.periodics = []
        @child_process.loop = nil
        @child_process.shutdown = false

        @child_process.mutex = Mutex.new
      end

      def shutdown
        @child_process.shutdown = true
        if @child_process.loop
          @child_process.loop.join
        end

        @child_process.processes.keys.each do |pid|
          begin
            Process.kill(:TERM, pid)
          rescue
            # Errno::ECHILD, Errno::ESRCH, Errno::EPERM
          end
        end
        @child_process.processes.keys.each do |pid|
          thread = @child_process.processes[pid]
          thread.join(@child_process.kill_timeout)
          @child_process.processes.delete(pid) unless thread.alive?
        end

        @child_process.processes.keys.each do |pid|
          thread = @child_process.processes[pid]
          begin
            Process.kill(:KILL, pid)
          rescue
            # Errno::ECHILD, Errno::ESRCH, Errno::EPERM
          end
          thread.join
        end

        super
      end

      def child_process(command, arguments: nil, subprocess_name: nil, read: true, write: true, encoding: 'utf-8', interval: nil, immediate: false, &block)
        raise "BUG: illegal specification to disable both of input and output file handle" if !read && !write
        raise "BUG: block not specified which receive i/o object" unless block_given?
        if interval
          child_process_execute_loop

          periodic = PeriodicProc.new
          periodic.spec = [command, arguments, subprocess_name, read, write, encoding]
          periodic.block = block
          periodic.interval = interval
          periodic.last_executed = if immediate
                                     Time.now - (interval + 1)
                                   else
                                     Time.now
                                   end
          @child_process.mutex.synchronize do
            @child_process.periodics.push(periodic)
          end
        else
          child_process_once(command, arguments, subprocess_name, read, write, encoding, &block)
        end
      end

      # for private use
      def child_process_once(command, arguments, subprocess_name, read, write, encoding, &block)
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
          Process.waitpid(pid)
          @child_process.processes.delete(pid)
        end
        @child_process.processes[pid] = thread
      end

      # for private use
      def child_process_execute_loop
        return if @child_process.loop

        thread = thread_create do
          while sleep(CHILD_PROCESS_LOOP_CHECK_INTERVAL)
            break if @child_process.shutdown
            now = Time.now
            periodics = @child_process.mutex.synchronize do
              @child_process.periodics.dup
            end
            periodics.each do |periodic|
              if now - periodic.last_executed >= periodic.interval
                spec = periodic.spec
                block = periodic.block
                child_process_once(*spec, &block)
              end
            end
          end
        end
      end
    end
  end
end
