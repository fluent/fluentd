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

require 'fluent/plugin/output'

module Fluent
  module Plugin
    class BufferedOutput < Output
      def initialize
        super
      end

      config_section :buffer, param_name: :buffer_config, required: false, multi: false, final: true do
        desc 'The buffer type (memory, file)'
        config_argument :type, :string, default: "memory"

        # Buffer and its chunk configurations are defined in each buffer plugins
        # This plugin defines options for how to flush/retry

        config_param :flush_at_shutdown, :bool, default: false

        desc 'The interval between data flushes.'
        config_param :flush_interval, :time, default: 60
        config_param :flush_thread_interval, :float, default: 1.0
        config_param :flush_burst_interval, :float, default: 1.0
        config_param :flush_immediately, :bool, default: false

        desc 'The number of threads to flush the buffer.'
        config_param :flush_threads, :integer, default: 1

        desc 'If true, plugins ignore retry_limit_* options and will retry flushing forever.'
        config_param :retry_forever, :bool, default: false
        desc 'The maximum seconds to retry to flush while failing until plugins discard buffer chunks.'
        config_param :retry_limit_time, :time, default: 72 * 60 * 60 # 72hours -- 17 times exponential backoff
        desc 'The maximum times to retry to flush while failing until plugins discard buffer chunks.'
        config_param :retry_limit_times, :integer, default: nil

        # Percentage of retry_limit_time to swtich to use secondary
        #  expornential backoff sequence will be initialized at the time of this threshold
        config_param :retry_secondary_threshold, :integer, default: 80

        config_param :retry_type, :enum, list: [:expbackoff, :periodic], default: :expbackoff
        ### Periodic -> fixed :retry_wait
        ### Exponencial backoff: k is number of retry times
        # c: constant factor, @retry_wait
        # b: base factor, @retry_backoff_base
        # k: times
        # total retry time: c + c * b^1 + (...) + c*b^k = c*b^(k+1) - 1
        desc 'The constant factor of exponencial backoff for retries.'
        config_param :retry_wait, :time, default: 1 # initial retry wait time & constant factor of exponencial backoff
        desc 'The base number of exponencial backoff for retries.'
        config_param :retry_backoff_base, :float, default: 2 # base of exponencial backoff
        desc 'The maximum interval seconds for exponencial backoff between retries while failing.'
        config_param :retry_max_interval, :time, default: nil
      end

      config_section :secondary, param_name: :secondary_config, required: false, multi: false, final: true do
        # to check there're <secondary> section at most once

        config_section :buffer, required: false, multi: false do
          # this section must not be specified in configuration files
        end
      end

      def configure(conf)
        super

        c = @buffer_config

        @buffer = Fluent::Plugin.new_buffer(c.type)
        @buffer.configure(conf)

        @flush_at_shutdown = c.flush_at_shutdown

        @flush_interval = c.flush_interval
        @flush_thread_interval = c.flush_thread_interval
        @flush_burst_interval = c.flush_burst_interval
        @flush_immediately = c.flush_immediately

        @flush_threads = c.flush_threads

        if @secondary_config
          if @secondary_config.buffer
            raise Fluent::ConfigError, "invalid <buffer> section configured in <secondary> section"
          end
          subconf = conf.elements.select {|e| e.name == 'secondary' }.first
          # assume that secondary plugin is a same plugin to primary one if type isn't specified
          subtype = subconf['@type'] || subconf['type'] || conf['@type'] || conf['type']
          @secondary = Plugin.new_output(subtype)
          @secondary.router = router
          @secondary.configure(subconf)
          @secondary.check_as_secondary(self)
        end

        @delayed_purge_enabled = false
        @delayed_purge_timeout = nil
      end

      def enable_delayed_purge(timeout:)
        unless self.respond_to?(:try_write)
          raise Fluent::ConfigError, "BUG: try_write method required to enable delayed purge"
        end
        @delayed_purge_enabled = true
        @delayed_purge_timeout = timeout
      end

      def check_as_secondary(primary)
        if primary.class != self.class
          log.warn "type of secondary output should be same as primary output", primary: primary.class.to_s, secondary: self.class.to_s
        end
      end

      def start
        @num_errors = 0
        @emit_count = 0

        @retry = nil # state machine
        @retry_mutex = Mutex.new

        if @delayed_purge_timeout
          @dequeued_chunks = [] # for delayed purge: array of DequeuedChunkInfo
          @dequeued_chunks_mutex = Mutex.new
        end

        # PluginSupport::Thread stops ealier than expected for OutputThread, which works for `before_shutdown` after `stop`
        @writers = (1..@flush_threads).map{ OutputThread.new(self, @log) }

        @buffer.start
        @secondary.start if @secondary

        @writers.each {|writer| writer.start }
        @writer_current_position = 0
        @writers_size = @writers.size
      end

      def stop
        super
        @writers.each {|writer| writer.stop }
        @secondary.stop if @secondary
        @buffer.stop
      end

      def before_shutdown
        super
        if @flush_at_shutdown
          force_flush
        end
      end

      def shutdown
        super
        @writers.each {|writer| writer.shutdown }
        @secondary.shutdown if @secondary
        @buffer.shutdown
      end

      def close
        super
        @writers.each {|writer| writer.close }
        @secondary.close if @secondary
        @buffer.close
      end

      def terminate
        super
        @writers.each {|writer| writer.terminate }
        @secondary.terminate if @secondary
        @buffer.terminate
      end

      # metadata() should be used only by myself
      def metadata(*args)
        @cached_metadata ||= @buffer.metadata()
      end

      # this should be overridden by child buffered_output class
      def cached_metadata
        [ metadata() ]
      end

      def emit(tag, es)
        meta = metadata()
        @emit_count += 1
        data = format_stream(tag, es)
        @buffer.emit(meta, data)

        if @flush_immediately
          @buffer.enqueue_chunk(meta)
        end
        if !@retry && @buffer.enqueued?(meta)
          submit_flush
        end
      end

      def format_stream(tag, es)
        out = ''
        es.each do |time, record|
          out << format(tag, time, record)
        end
        out
      end

      def format(tag, time, record)
        raise NotImplementedError, "BUG: buffered output plugins MUST implement this method"
      end

      ### Implement try_write if plugin does commit_flush under its own responsibility
      # def try_write(chunk)

      def write(chunk)
        raise NotImplementedError, "BUG: buffered output plugins MUST implement this method"
      end

      def submit_flush
        # Without locks: it is rough but enough to select "next" writer selection
        @writer_current_position = (@writer_current_position + 1) % @writers_size
        @writers[@writer_current_position].submit_flush
      end

      def commit_write(chunk_id)
        if @delayed_purge_enabled
          @dequeued_chunks_mutex.synchronize do
            @dequeued_chunks.delete_if { |info| info.chunk_id == chunk_id }
          end
        else
          @buffer.purge_chunk(chunk_id)
        end
        @retry_mutex.synchronize do
          if @retry # success to flush chunks
            log.warn "retry succeeded.", plugin_id: plugin_id
            @retry = nil
            @num_errors = 0
          end
        end
      end

      def try_rollback_write
        @dequeued_chunks_mutex.synchronize do
          now = Time.now
          while !@dequeued_chunks.empty? && @dequeued_chunks.first.time + @delayed_purge_timeout > now
            info = @delayed_purge_timeout.shift
            if @buffer.takeback_chunk(info.chunk_id)
              log.warn "failed to flush chunk, timeout to commit.", plugin_id: plugin_id, chunk_id: chunk_id, flushed_at: info.time
              log.warn_backtrace e.backtrace
            end
          end
        end
      end

      def enqueue_buffer(force: false)
        if force
          @buffer.enqueue_all
        else
          cached_metadata.each do |meta|
            if @buffer.staged_chunk_test(meta){ |chunk| chunk.modified_at + @flush_interval > Fluent::Engine.now }
              @buffer.enqueue_chunk(meta)
            end
          end
        end
      end

      def force_flush
        enqueue_buffer(force: true)
        submit_flush
      end

      def next_flush_time
        if @buffer.queued?
          @retry_mutex.synchronize do
            @retry ? @retry.next_time : Time.now + @flush_burst_interval
          end
        else
          Time.now + @flush_thread_interval
        end
      end

      def try_flush
        enqueue_buffer unless @buffer.queued?
        return unless @buffer.queued?

        chunk = @buffer.dequeue_chunk
        return unless chunk

        output = self
        if @retry_mutex.synchronize{ @retry && @retry.secondary? }
          output = @secondary
        end
        begin
          if @delayed_purge_enabled && output.respond_to?(:try_write) # secondary plugin may not support try_write method
            output.try_write(chunk)
            @dequeued_chunks_mutex.synchronize do
              @dequeued_chunks << DequeuedChunkInfo.new(chunk.unique_id, Time.now)
            end
          else
            # output plugin without delayed purge
            chunk_id = chunk.unique_id
            output.write(chunk)
            commit_write(chunk_id)
          end
        rescue => e
          @retry_mutex.synchronize do
            if @retry
              @retry.step
              @num_errors = @retry.times
              if @retry.limit?
                records = @buffer.queued_records
                log.error "failed to flush the buffer, and hit limit for retries. dropping all chunks in buffer queue.", records: records, error_class: e.class, error: e, plugin_id: plugin_id
                log.error_backtrace e.backtrace
                @buffer.clear!
                @retry = nil
              elsif !@retry.secondary?
                log.warn "failed to flush the buffer.", next_retry: @retry.next_time, error_class: e.class, error: e, plugin_id: plugin_id
                log.warn_backtrace e.backtrace
              else # secondary
                log.warn "failed to flush the buffer with secondary output.", next_retry: @retry.next_time, error_class: e.class, error: e, plugin_id: plugin_id
                log.warn_backtrace e.backtrace
              end
            else
              @retry = RetryStateMachine.create(@buffer_config, @secondary)
              @num_errors = @retry.times # == 1
              log.warn "failed to flush the buffer.", next_retry: @retry.next_time, error_class: e.class, error: e, plugin_id: plugin_id
              log.warn_backtrace e.backtrace
            end
          end
        end
      end

      DequeuedChunkInfo = Struct.new(:chunk_id, :time)

      class RetryStateMachine
        attr_reader :next_time, :times

        def self.create(conf, secondary)
          case conf.retry_type
          when :expbackoff
            ExponencialBackOffRetry.new(conf, secondary)
          when :periodic
            PeriodicRetry.new(conf, secondary)
          else
            raise "BUG: unknown retry_type specified: '#{conf.retry_type}'"
          end
        end

        def initialize(conf, secondary)
          @start = Time.now
          @times = 1
          @next_time = nil # should be initialized for first retry by child class

          @forever = conf.retry_forever
          @limit_time = Time.now + conf.retry_limit_time
          @limit_times = conf.retry_limit_times

          @current = :primary
          @secondary = secondary
          @secondary_threshold = conf.retry_secondary_threshold
          if @secondary
            @secondary_transition = Time.now + @limit_time * @secondary_threshold / 100
            @secondary_transition_times = nil
          end
        end

        def calc_next_time
          if @forever || !@secondary # primary
            naive = naive_next_time(@times)
            if naive >= @limit_time
              @limit_time
            else
              naive
            end
          elsif @current == :primary && @secondary
            naive = naive_next_time(@times)
            if naive >= @secondary_transition
              @secondary_transition
            else
              naive
            end
          elsif @current == :secondary
            naive = naive_next_time(@times - @secondary_transition_times)
            if naive >= @limit_time
              @limit_time
            else
              naive
            end
          else
            raise "BUG: it's out of design"
          end
        end

        def naive_next_time(retry_times)
          raise NotImplementedError
        end

        def secondary?
          @secondary && @current == :secondary
        end

        def step(next_time)
          @times += 1
          @next_time = calc_next_time
          if @secondary && @next_time >= @secondary_transition
            @current = :secondary
            @secondary_transition_times = @times - 1
          end
        end

        def limit?
          if @forever
            false
          else
            @next_time >= @limit_time || @times >= @limit_times
          end
        end
      end

      class ExponencialBackOffRetry < RetryStateMachine
        def initialize(conf, secondary)
          super
          @constant_factor = conf.retry_wait
          @backoff_base = conf.retry_backoff_base
          @max_interval = conf.retry_max_interval

          @next_time = Time.now + @constant_factor
        end

        def naive_next_time(retry_next_times)
          interval = @constant_factor * ( @backoff_base ** ( retry_next_times - 1 ) )
          if @max_interval && interval > @max_interval
            @max_interval
          else
            ### why rand only for wait.finite?
            # retry_wait = wait.finite? ? wait + (rand * (wait / 4.0) - (wait / 8.0)) : wait
            interval
          end
        end
      end

      class PeriodicRetry < RetryStateMachine
        def initialize(conf, secondary)
          super
          @retry_wait = conf.retry_wait
          @next_time = Time.now + @retry_wait
        end

        def naive_next_time(retry_next_times)
          @retry_wait
        end
      end

      class OutputThread
        def initialize(output, log)
          @output = output
          @log = log
          # If the given clock_id is not supported, Errno::EINVAL is raised.
          @clock = Process::CLOCK_MONOTONIC rescue Process::CLOCK_MONOTONIC_RAW
        end

        def start
          @finish = false
          @next_time = Process.clock_gettime(@clock) + 1.0
          @thread = Thread.new(&method(:run))
        end

        def stop
        end

        def shutdown
          @finish = true
          @thread.run
          @thread.join
        end

        def close
        end

        def terminate
          @thread = nil
          @finish = nil
          @next_time = nil
        end

        def submit_flush
          @next_time = 0
          @thread.run
        end

        def run
          Thread.current.abort_on_exception = true
          begin
            until @finish
              time = Process.clock_gettime(clock_id)
              interval = @next_time - time

              if @delayed_purge_enabled && @dequeued_chunks_mutex.synchronize{ !@dequeued_chunks.empty? }
                @output.try_rollback_write
              end

              if @next_time < time
                @output.try_flush
                interval = @output.next_flush_time.to_f - Time.now.to_f
                @next_time = Process.clock_gettime(clock_id) + interval
              end

              sleep interval
            end
          rescue => e
            # normal errors are rescued by output plugins in #try_flush
            # so this rescue section is for critical & unrecoverable errors
            @log.error "error on output thread", error_class: e.class.to_s, error: e.to_s
            @log.error_backtrace
            raise
          end
        end
      end
    end
  end
end
