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
require 'fluent/clock'

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

      def child_process_exist?(pid)
        pinfo = @_child_process_processes[pid]
        return false unless pinfo

        return false if pinfo.exit_status

        true
      end

      # on_exit_callback = ->(status){ ... }
      # status is an instance of Process::Status
      # On Windows, exitstatus=0 and termsig=nil even when child process was killed.
      def child_process_execute(
          title, command,
          arguments: nil, subprocess_name: nil, interval: nil, immediate: false, parallel: false,
          mode: [:read, :write], stderr: :discard, env: {}, unsetenv: false, chdir: nil,
          internal_encoding: 'utf-8', external_encoding: 'ascii-8bit', scrub: true, replace_string: nil,
          wait_timeout: nil, on_exit_callback: nil,
          &block
      )
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: arguments required if subprocess name is replaced" if subprocess_name && !arguments

        mode ||= []
        mode = [] unless block
        raise ArgumentError, "BUG: invalid mode specification" unless mode.all?{|m| MODE_PARAMS.include?(m) }
        raise ArgumentError, "BUG: read_with_stderr is exclusive with :read and :stderr" if mode.include?(:read_with_stderr) && (mode.include?(:read) || mode.include?(:stderr))
        raise ArgumentError, "BUG: invalid stderr handling specification" unless STDERR_OPTIONS.include?(stderr)

        raise ArgumentError, "BUG: number of block arguments are different from size of mode" if block && block.arity != mode.size

        running = false
        callback = ->(*args) {
          running = true
          begin
            block && block.call(*args)
          ensure
            running = false
          end
        }

        retval = nil
        execute_child_process = ->(){
          child_process_execute_once(
            title, command, arguments,
            subprocess_name, mode, stderr, env, unsetenv, chdir,
            internal_encoding, external_encoding, scrub, replace_string,
            wait_timeout, on_exit_callback,
            &callback
          )
        }

        if immediate || !interval
          retval = execute_child_process.call
        end

        if interval
          timer_execute(:child_process_execute, interval, repeat: true) do
            if !parallel && running
              log.warn "previous child process is still running. skipped.", title: title, command: command, arguments: arguments, interval: interval, parallel: parallel
            else
              execute_child_process.call
            end
          end
        end

        retval # nil if interval
      end

      def initialize
        super
        # plugins MAY configure this parameter
        @_child_process_exit_timeout = CHILD_PROCESS_DEFAULT_EXIT_TIMEOUT
        @_child_process_kill_timeout = CHILD_PROCESS_DEFAULT_KILL_TIMEOUT
        @_child_process_mutex = Mutex.new
        @_child_process_processes = {} # pid => ProcessInfo
      end

      def stop
        @_child_process_mutex.synchronize{ @_child_process_processes.keys }.each do |pid|
          process_info = @_child_process_processes[pid]
          if process_info
            process_info.thread[:_fluentd_plugin_helper_child_process_running] = false
          end
        end

        super
      end

      def shutdown
        @_child_process_mutex.synchronize{ @_child_process_processes.keys }.each do |pid|
          process_info = @_child_process_processes[pid]
          next if !process_info
          process_info.writeio && process_info.writeio.close rescue nil
        end

        super

        @_child_process_mutex.synchronize{ @_child_process_processes.keys }.each do |pid|
          process_info = @_child_process_processes[pid]
          next if !process_info
          child_process_kill(process_info)
        end

        exit_wait_timeout = Fluent::Clock.now + @_child_process_exit_timeout
        while Fluent::Clock.now < exit_wait_timeout
          process_exists = false
          @_child_process_mutex.synchronize{ @_child_process_processes.keys }.each do |pid|
            unless @_child_process_processes[pid].exit_status
              process_exists = true
              break
            end
          end
          if process_exists
            sleep CHILD_PROCESS_LOOP_CHECK_INTERVAL
          else
            break
          end
        end
      end

      def close
        while true
          pids = @_child_process_mutex.synchronize{ @_child_process_processes.keys }
          break if pids.size < 1

          living_process_exist = false
          pids.each do |pid|
            process_info = @_child_process_processes[pid]
            next if !process_info || process_info.exit_status

            living_process_exist = true

            process_info.killed_at ||= Fluent::Clock.now # for illegular case (e.g., created after shutdown)
            timeout_at = process_info.killed_at + @_child_process_kill_timeout
            now = Fluent::Clock.now
            next if now < timeout_at

            child_process_kill(process_info, force: true)
          end

          break if living_process_exist

          sleep CHILD_PROCESS_LOOP_CHECK_INTERVAL
        end

        super
      end

      def terminate
        @_child_process_processes = {}

        super
      end

      def child_process_kill(pinfo, force: false)
        return if !pinfo
        pinfo.killed_at = Fluent::Clock.now unless force

        pid = pinfo.pid
        begin
          if !pinfo.exit_status && child_process_exist?(pid)
            signal = (Fluent.windows? || force) ? :KILL : :TERM
            Process.kill(signal, pinfo.pid)
          end
        rescue Errno::ECHILD, Errno::ESRCH
          # ignore
        end
      end

      ProcessInfo = Struct.new(
        :title,
        :thread, :pid,
        :readio, :readio_in_use, :writeio, :writeio_in_use, :stderrio, :stderrio_in_use,
        :wait_thread, :alive, :killed_at, :exit_status,
        :on_exit_callback, :on_exit_callback_mutex,
      )

      def child_process_execute_once(
          title, command, arguments, subprocess_name, mode, stderr, env, unsetenv, chdir,
          internal_encoding, external_encoding, scrub, replace_string, wait_timeout, on_exit_callback, &block
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
        end

        if mode.include?(:write)
          writeio.set_encoding(external_encoding, internal_encoding, encoding_options)
          writeio_in_use = true
        else
          writeio.reopen(IO::NULL) if writeio
        end
        if mode.include?(:read) || mode.include?(:read_with_stderr)
          readio.set_encoding(external_encoding, internal_encoding, encoding_options)
          readio_in_use = true
        else
          readio.reopen(IO::NULL) if readio
        end
        if mode.include?(:stderr)
          stderrio.set_encoding(external_encoding, internal_encoding, encoding_options)
          stderrio_in_use = true
        else
          stderrio.reopen(IO::NULL) if stderrio && stderrio == :discard
        end

        pid = wait_thread.pid # wait_thread => Process::Waiter

        io_objects = []
        mode.each do |m|
          io_obj = case m
                   when :read then readio
                   when :write then writeio
                   when :read_with_stderr then readio
                   when :stderr then stderrio
                   else
                     raise "BUG: invalid mode must be checked before here: '#{m}'"
                   end
          io_objects << io_obj
        end

        m = Mutex.new
        m.lock
        thread = thread_create :child_process_callback do
          m.lock # run after plugin thread get pid, thread instance and i/o
          m.unlock
          begin
            @_child_process_processes[pid].alive = true
            block.call(*io_objects) if block_given?
            writeio.close if writeio
          rescue EOFError => e
            log.debug "Process exit and I/O closed", title: title, pid: pid, command: command, arguments: arguments
          rescue IOError => e
            if e.message == 'stream closed' || e.message == 'closed stream' # "closed stream" is of ruby 2.1
              log.debug "Process I/O stream closed", title: title, pid: pid, command: command, arguments: arguments
            else
              log.error "Unexpected I/O error for child process", title: title, pid: pid, command: command, arguments: arguments, error: e
            end
          rescue Errno::EPIPE => e
            log.debug "Broken pipe, child process unexpectedly exits", title: title, pid: pid, command: command, arguments: arguments
          rescue => e
            log.warn "Unexpected error while processing I/O for child process", title: title, pid: pid, command: command, error: e
          end

          if wait_timeout
            if wait_thread.join(wait_timeout) # Thread#join returns nil when limit expires
              # wait_thread successfully exits
              @_child_process_processes[pid].exit_status = wait_thread.value
            else
              log.warn "child process timed out", title: title, pid: pid, command: command, arguments: arguments
              child_process_kill(@_child_process_processes[pid], force: true)
              @_child_process_processes[pid].exit_status = wait_thread.value
            end
          else
            @_child_process_processes[pid].exit_status = wait_thread.value # with join
          end
          process_info = @_child_process_mutex.synchronize{ @_child_process_processes.delete(pid) }

          cb = process_info.on_exit_callback_mutex.synchronize do
            cback = process_info.on_exit_callback
            process_info.on_exit_callback = nil
            cback
          end
          if cb
            cb.call(process_info.exit_status) rescue nil
          end
        end
        thread[:_fluentd_plugin_helper_child_process_running] = true
        thread[:_fluentd_plugin_helper_child_process_pid] = pid
        pinfo = ProcessInfo.new(
          title, thread, pid,
          readio, readio_in_use, writeio, writeio_in_use, stderrio, stderrio_in_use,
          wait_thread, false, nil, nil, on_exit_callback, Mutex.new
        )

        @_child_process_mutex.synchronize do
          @_child_process_processes[pid] = pinfo
        end
        m.unlock
        pid
      end
    end
  end
end
