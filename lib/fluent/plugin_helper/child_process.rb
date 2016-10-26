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

require 'open3'
require 'timeout'

module Fluent
  module PluginHelper
    module ChildProcess
      include Fluent::PluginHelper::Thread
      include Fluent::PluginHelper::Timer

      CHILD_PROCESS_LOOP_CHECK_INTERVAL = 0.2 # sec
      CHILD_PROCESS_DEFAULT_EXIT_TIMEOUT = 10 # sec
      CHILD_PROCESS_DEFAULT_KILL_TIMEOUT = 60 # sec

      MODE_PARAMS = [:read, :write, :stderr, :read_with_stderr]
      STDERR_OPTIONS = [:discard, :connect]

      # stop     : mark callback thread as stopped
      # shutdown : close write IO to child processes (STDIN of child processes), send TERM (KILL for Windows) to all child processes
      # close    : send KILL to all child processes
      # terminate: [-]

      attr_reader :_child_process_processes # for tests

      def child_process_running?
        # checker for code in callback of child_process_execute
        ::Thread.current[:_fluentd_plugin_helper_child_process_running] || false
      end

      def child_process_id
        ::Thread.current[:_fluentd_plugin_helper_child_process_pid]
      end

      def child_process_exit_status
        ::Thread.current[:_fluentd_plugin_helper_child_process_exit_status]
      end

      def child_process_execute(
          title, command,
          arguments: nil, subprocess_name: nil, interval: nil, immediate: false, parallel: false,
          mode: [:read, :write], stderr: :discard, env: {}, unsetenv: false, chdir: nil,
          internal_encoding: 'utf-8', external_encoding: 'ascii-8bit', scrub: true, replace_string: nil,
          &block
      )
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: arguments required if subprocess name is replaced" if subprocess_name && !arguments

        raise ArgumentError, "BUG: invalid mode specification" unless mode.all?{|m| MODE_PARAMS.include?(m) }
        raise ArgumentError, "BUG: read_with_stderr is exclusive with :read and :stderr" if mode.include?(:read_with_stderr) && (mode.include?(:read) || mode.include?(:stderr))
        raise ArgumentError, "BUG: invalid stderr handling specification" unless STDERR_OPTIONS.include?(stderr)

        raise ArgumentError, "BUG: block not specified which receive i/o object" unless block_given?
        raise ArgumentError, "BUG: number of block arguments are different from size of mode" unless block.arity == mode.size

        running = false
        callback = ->(*args) {
          running = true
          begin
            block.call(*args)
          ensure
            running = false
          end
        }

        if immediate || !interval
          child_process_execute_once(title, command, arguments, subprocess_name, mode, stderr, env, unsetenv, chdir, internal_encoding, external_encoding, scrub, replace_string, &callback)
        end

        if interval
          timer_execute(:child_process_execute, interval, repeat: true) do
            if !parallel && running
              log.warn "previous child process is still running. skipped.", title: title, command: command, arguments: arguments, interval: interval, parallel: parallel
            else
              child_process_execute_once(title, command, arguments, subprocess_name, mode, stderr, env, unsetenv, chdir, internal_encoding, external_encoding, scrub, replace_string, &callback)
            end
          end
        end
      end

      def initialize
        super
        # plugins MAY configure this parameter
        @_child_process_exit_timeout = CHILD_PROCESS_DEFAULT_EXIT_TIMEOUT
        @_child_process_kill_timeout = CHILD_PROCESS_DEFAULT_KILL_TIMEOUT
        @_child_process_mutex = Mutex.new
      end

      def start
        super
        @_child_process_processes = {} # pid => ProcessInfo
      end

      def stop
        @_child_process_mutex.synchronize{ @_child_process_processes.keys }.each do |pid|
          process_info = @_child_process_processes[pid]
          if process_info
            process_info.thread[:_fluentd_plugin_helper_child_process_running] = false
          end
        end
      end

      def shutdown
        @_child_process_mutex.synchronize{ @_child_process_processes.keys }.each do |pid|
          process_info = @_child_process_processes[pid]
          next if !process_info || !process_info.writeio_in_use
          begin
            Timeout.timeout(@_child_process_exit_timeout) do
              process_info.writeio.close
            end
          rescue Timeout::Error
            log.debug "External process #{process_info.title} doesn't exist after STDIN close in timeout #{@_child_process_exit_timeout}sec"
          end

          child_process_kill(process_info)
        end

        super
      end

      def close
        while (pids = @_child_process_mutex.synchronize{ @_child_process_processes.keys }).size > 0
          pids.each do |pid|
            process_info = @_child_process_processes[pid]
            if !process_info || !process_info.alive
              @_child_process_mutex.synchronize{ @_child_process_processes.delete(pid) }
              next
            end

            process_info.killed_at ||= Time.now # for illegular case (e.g., created after shutdown)
            next if Time.now < process_info.killed_at + @_child_process_kill_timeout

            child_process_kill(process_info, force: true)
            @_child_process_mutex.synchronize{ @_child_process_processes.delete(pid) }
          end

          sleep CHILD_PROCESS_LOOP_CHECK_INTERVAL
        end

        super
      end

      def child_process_kill(process_info, force: false)
        if !process_info || !process_info.alive
          return
        end

        process_info.killed_at = Time.now unless force

        begin
          pid, status = Process.waitpid2(process_info.pid, Process::WNOHANG)
          if pid && status
            process_info.thread[:_fluentd_plugin_helper_child_process_exit_status] = status
            process_info.alive = false
          end
        rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM
          process_info.alive = false
        rescue
          # ignore
        end
        if !process_info.alive
          return
        end

        begin
          signal = (Fluent.windows? || force) ? :KILL : :TERM
          Process.kill(signal, process_info.pid)
          if force
            process_info.alive = false
          end
        rescue Errno::ECHILD, Errno::ESRCH
          process_info.alive = false
        end
      end

      ProcessInfo = Struct.new(:title, :thread, :pid, :readio, :readio_in_use, :writeio, :writeio_in_use, :stderrio, :stderrio_in_use, :wait_thread, :alive, :killed_at)

      def child_process_execute_once(
          title, command, arguments, subprocess_name, mode, stderr, env, unsetenv, chdir,
          internal_encoding, external_encoding, scrub, replace_string, &block
      )
        spawn_args = if arguments || subprocess_name
                       [ env, (subprocess_name ? [command, subprocess_name] : command), *(arguments || []) ]
                     else
                       [ env, command ]
                     end
        spawn_opts = {
          unsetenv_others: unsetenv,
        }
        if chdir
          spawn_opts[:chdir] = chdir
        end

        encoding_options = {}
        if scrub
          encoding_options[:invalid] = encoding_options[:undef] = :replace
          if replace_string
            encoding_options[:replace] = replace_string
          end
        end

        log.debug "Executing command", title: title, spawn: spawn_args, mode: mode, stderr: stderr

        readio = writeio = stderrio = wait_thread = nil
        readio_in_use = writeio_in_use = stderrio_in_use = false

        if !mode.include?(:stderr) && !mode.include?(:read_with_stderr) && stderr != :discard # connect
          writeio, readio, wait_thread = *Open3.popen2(*spawn_args, spawn_opts)
        elsif mode.include?(:read_with_stderr)
          writeio, readio, wait_thread = *Open3.popen2e(*spawn_args, spawn_opts)
        else
          writeio, readio, stderrio, wait_thread = *Open3.popen3(*spawn_args, spawn_opts)
          if !mode.include?(:stderr) # stderr == :discard
            stderrio.reopen(IO::NULL)
          end
        end

        if mode.include?(:write)
          writeio.set_encoding(external_encoding, internal_encoding, encoding_options)
          writeio_in_use = true
        end
        if mode.include?(:read) || mode.include?(:read_with_stderr)
          readio.set_encoding(external_encoding, internal_encoding, encoding_options)
          readio_in_use = true
        end
        if mode.include?(:stderr)
          stderrio.set_encoding(external_encoding, internal_encoding, encoding_options)
          stderrio_in_use = true
        end

        pid = wait_thread.pid # wait_thread => Process::Waiter

        io_objects = []
        mode.each do |m|
          io_objects << case m
                        when :read then readio
                        when :write then writeio
                        when :read_with_stderr then readio
                        when :stderr then stderrio
                        else
                          raise "BUG: invalid mode must be checked before here: '#{m}'"
                        end
        end

        m = Mutex.new
        m.lock
        thread = thread_create :child_process_callback do
          m.lock # run after plugin thread get pid, thread instance and i/o
          m.unlock
          begin
            @_child_process_processes[pid].alive = true
            block.call(*io_objects)
          rescue EOFError => e
            log.debug "Process exit and I/O closed", title: title, pid: pid, command: command, arguments: arguments
          rescue IOError => e
            if e.message == 'stream closed'
              log.debug "Process I/O stream closed", title: title, pid: pid, command: command, arguments: arguments
            else
              log.error "Unexpected I/O error for child process", title: title, pid: pid, command: command, arguments: arguments, error: e
            end
          rescue => e
            log.warn "Unexpected error while processing I/O for child process", title: title, pid: pid, command: command, error: e
          end
          process_info = @_child_process_mutex.synchronize do
            process_info = @_child_process_processes[pid]
            @_child_process_processes.delete(pid)
            process_info
          end
          child_process_kill(process_info, force: true) if process_info && process_info.alive && ::Thread.current[:_fluentd_plugin_helper_child_process_running]
        end
        thread[:_fluentd_plugin_helper_child_process_running] = true
        thread[:_fluentd_plugin_helper_child_process_pid] = pid
        pinfo = ProcessInfo.new(title, thread, pid, readio, readio_in_use, writeio, writeio_in_use, stderrio, stderrio_in_use, wait_thread, false, nil)
        @_child_process_mutex.synchronize do
          @_child_process_processes[pid] = pinfo
        end
        m.unlock
        pid
      end
    end
  end
end
