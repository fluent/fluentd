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

require 'cool.io'
require 'fluent/plugin/input'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    class WorkerPool
      class WorkerStopped < StandardError; end
      Task = Struct.new(:key, :bytes, :payload)
      Result = Struct.new(:task, :value, :error)

      def initialize(num_threads:, task_limit:, byte_limit:, thread_create:, notify:, &process)
        unless num_threads > 0 && task_limit > 0 && byte_limit > 0
          raise ArgumentError, 'worker and queue limits must be positive'
        end
        raise ArgumentError, 'processor is required' unless process

        @num_threads = num_threads
        @task_limit = task_limit
        @byte_limit = byte_limit
        @thread_create = thread_create
        @notify = notify
        @process = process
        @mutex = Mutex.new
        @jobs = Queue.new
        @results = Queue.new
        @active = {}
        @bytes = 0
        @threads = []
        @state = :new
        @failure = nil
      end

      def start
        @mutex.synchronize do
          raise 'worker pool already started' unless @state == :new
          @state = :starting
        end
        begin
          @num_threads.times do |index|
            @threads << @thread_create.call(:"in_tail_worker_#{index}") { run(index) }
          end
          @mutex.synchronize do
            raise(@failure || WorkerStopped.new('pool stopped during start')) unless @state == :starting
            @state = :running
          end
          self
        rescue
          stop
          join
          raise
        end
      end

      # Capacity remains reserved until emit and position handling acknowledge
      # the result, including while a parsed batch waits in the result queue.
      def submit(key, payload, bytes:)
        raise ArgumentError, 'batch bytes must be nonnegative' if bytes < 0

        @mutex.synchronize do
          return false unless @state == :running
          return false if @active.key?(key) || @active.size >= @task_limit
          return false if @bytes + bytes > @byte_limit

          task = Task.new(key, bytes, payload).freeze
          @active[key] = task
          @bytes += bytes
          @jobs << task
          true
        end
      end

      def next_result
        @results.pop(true)
      rescue ThreadError
        nil
      end

      def acknowledge(result)
        @mutex.synchronize do
          task = result.task
          unless @active[task.key].equal?(task)
            raise ArgumentError, 'result is no longer active'
          end

          @active.delete(task.key)
          @bytes -= task.bytes
        end
      end

      def statistics
        @mutex.synchronize { { tasks: @active.size, bytes: @bytes, state: @state }.freeze }
      end

      def failure
        @mutex.synchronize { @failure }
      end

      def stop
        @mutex.synchronize do
          @state = :stopping unless @state == :failed
          @jobs.close
        end
      end

      # The source joins after draining results; the event loop never joins.
      def join(timeout: nil)
        deadline = timeout && Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
        @threads.each do |thread|
          remaining = deadline && [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
          return false unless thread.join(remaining)
        end
        true
      end

      private

      def run(index)
        normal_exit = false
        while (task = @jobs.pop)
          value = nil
          error = nil
          completed = false
          begin
            value = @process.call(index, task.payload)
            completed = true
          rescue Exception => e
            # Deliver task failures to the owner instead of losing an active
            # file or terminating a helper thread with abort_on_exception.
            error = e
            completed = true
          ensure
            error = WorkerStopped.new('worker stopped during a task') unless completed
            publish(Result.new(task, value, error).freeze)
          end
        end
        normal_exit = true
      ensure
        fail_pool(WorkerStopped.new('worker exited unexpectedly')) unless normal_exit
      end

      def publish(result)
        @results << result
        begin
          @notify.call
        rescue Exception => error
          fail_pool(error)
        end
      end

      def fail_pool(error)
        @mutex.synchronize do
          @failure ||= error
          @state = :failed
          @jobs.close
        end
        begin
          while (task = @jobs.pop(true))
            @results << Result.new(task, nil, error).freeze
          end
        rescue ThreadError
          # All queued jobs have been accounted for.
        end
        begin
          @notify.call
        rescue Exception => notification_error
          @mutex.synchronize { @failure ||= notification_error }
        end
      end
    end

    # Captures the built-in hook implementations once and checks whether a
    # TailInput instance resolves each hook to the same method body. A mismatch
    # means worker execution must fall back to the synchronous compatibility
    # path; this deliberately does not try to infer safety from method owners.
    class WorkerCompatibility
      HOOKS = %i[
        receive_lines flush_buffer convert_line_to_event parse_singleline
        parse_multilines setup_watcher io_handler build_line_feeder
      ].freeze

      def initialize(base_class)
        @hooks = HOOKS.to_h { |name| [name, base_class.instance_method(name)] }.freeze
      end

      def compatible?(plugin)
        incompatible_hooks(plugin).empty?
      end

      def incompatible_hooks(plugin)
        @hooks.filter_map do |name, built_in|
          name unless plugin.singleton_class.instance_method(name) == built_in
        rescue NameError
          name
        end
      end
    end

    class WorkerCompletionWatcher < Coolio::AsyncWatcher
      def initialize(&on_completion)
        super()
        @on_completion = on_completion
      end

      def signal
        @writer.write_nonblock("\0")
        true
      rescue IO::WaitWritable
        # A full pipe already has a notification pending on the event loop.
        false
      end

      def on_signal
        @on_completion.call
      end

      def close
        detach if attached?
        @reader.close unless @reader.closed?
        @writer.close unless @writer.closed?
      end
    end
  end
end
