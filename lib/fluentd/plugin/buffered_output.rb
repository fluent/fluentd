#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
  module Plugin

    class BufferedOutput < Output
      class FlushThread
        def initialize(try_flush)
          @try_flush = try_flush
          @mutex = Mutex.new
          @cond = ConditionVariable.new
          @finished = false
        end

        def start
          @thread = Thread.new(&method(:run))
        end

        def shutdown
          @mutex.synchronize do
            @finished = true
            @cond.broadcast
          end
          if @thread
            @thread.join
            @thread = nil
          end
        end

        private

        def run
          @mutex.synchronize do
            until @finished
              wait = @try_flush.call
              @cond.wait(@mutex, wait) if wait > 0
            end
          end
        end
      end

      def initialize
        super
        @threads = []
        @flush_mutex = Mutex.new
        @flush_time = 0
        @flush_error_history = []
      end

      config_param :flush_interval, :time, :default => 10
      config_param :flush_retry_limit, :integer, :default => 11
      config_param :flush_retry_wait, :time, :default => 1
      config_param :flush_threads, :integer, :default => 2
      config_param :buffer_type, :string, :default => 'memory'

      def configure(conf)
        super

        @buffer = Engine.plugins.new_buffer(@buffer_type)
        @buffer.configure(conf)

        # TODO @secondary

        #if @secondary && !is_a?(ObjectBufferedOutput)
        ## TODO warn
        #end
      end

      def start
        @flush_threads.times do
          @threads << FlushThread.new(method(:try_flush))
        end

        @threads.each {|t|
          t.start
        }

        @buffer.start
        super
      end

      def stop
        @buffer.stop if @buffer
        super
      end

      def shutdown
        @buffer.shutdown if @buffer
        @threads.reverse_each {|t|
          t.shutdown
        }
        super
      end

      def emit(tag, time, record)
        emits(tag, SingleEventCollection.new(time, record))
      end

      def emits(tag, es)
        @buffer.open do |a|
          es.each {|time,record|
            handle_error(tag, time, record) do
              data = format(tag, time, record)
              key = buffer_key(tag, time, record)
              a.append(key, data)
            end
          }
        end
        nil
      end

      def buffer_key(tag, time, record)
        ''
      end

      def format(tag, time, record)
        raise NoMethodError, "#{self.class}#format(tag, time, record) is not implemented"
      end

      def write(chunk)
        raise NoMethodError, "#{self.class}#write(chunk) is not implemented"
      end

      private

      class ScopedMutex
        def initialize(mutex)
          @mutex = mutex
          @locked = false
        end

        def lock
          @mutex.lock
          @locked = true
        end

        def unlock
          @mutex.unlock
          @locked = false
        end

        def ensure_locked
          lock unless @locked
        end

        def ensure_unlocked
          unlock if @locked
        end
      end

      def acquire_chunk(&block)
        @buffer.enqueue_chunk(@buffer.keys.first)
        @buffer.acquire(&block)
      end

      def try_flush
        now = Time.now.to_i

        return @flush_time - now if @flush_time > now

        flush_lock = ScopedMutex.new(@flush_mutex)
        flush_lock.lock
        begin
          return @flush_time - now if @flush_time > now

          acquired = false
          acquire_chunk {|chunk|
            acquired = true

            retry_count = @flush_error_history.size
            if retry_count == 0
              # allow parallel flush only if this trial is not in retrying mode
              flush_lock.unlock
            end

            if @secondary && retry_count > @flush_retry_limit
              error = secondary_flush_chunk(chunk)
            else
              error = flush_chunk(chunk)
            end
            now = Time.now.to_i

            if error
              flush_lock.ensure_locked
              @flush_error_history << error
              retry_wait = flush_failed(error)
              @flush_time = now + retry_wait

            elsif retry_count > 0
              # retry succeeded; locked section
              #@log.warn "retry succeeded.", :instance=>object_id
              @flush_error_history.clear
            end

            # leave @flush_time as is to run next try_flush immediately
          }

          unless acquired  # buffer queue is empty
            # flush_lock is locked
            @flush_time = now + @flush_interval
          end

          # try next flush soon while buffer is not empty
          return @flush_time - now

        ensure
          flush_lock.ensure_unlocked
        end
      end

      def flush_chunk(chunk)
        begin
          write(chunk)
          return nil
        rescue => e
          return e
        end
      end

      def secondary_flush_chunk(chunk)
        begin
          if is_a?(ObjectBufferedOutput)
            @secondary.emits(chunk.tag, chunk)
          else
            @secondary.write(chunk)
          end
          return nil
        rescue => e
          return e
        end
      end

      def flush_failed(error)
        # error handling
        retry_count = @flush_error_history.size - 1

        if retry_count < @flush_retry_limit
          Engine.log.warn "temporarily failed to flush the buffer, next retry will be at #{Time.at(@flush_time)}.", :error=>error.to_s, :instance=>object_id
          Engine.log.warn_backtrace error.backtrace
          return calc_retry_wait_time(retry_count)

        elsif @secondary
          if retry_count == @flush_retry_limit
            Engine.log.warn "failed to flush the buffer.", :error=>error.to_s, :instance=>object_id
            Engine.log.warn "retry count exceededs limit. falling back to secondary output."
            Engine.log.warn_backtrace error.backtrace
            return 0  # retry immediately

          elsif retry_count <= @flush_retry_limit + @secondary_limit
            Engine.log.warn "failed to flush the buffer, next retry will be with secondary output at #{Time.at(@flush_time)}.", :error=>error.to_s, :instance=>object_id
            Engine.log.warn_backtrace error.backtrace
            return calc_retry_wait_time(retry_count - @flush_retry_limit - 1)

          else
            Engine.log.warn "failed to flush the buffer.", :error=>error.to_s, :instance=>object_id
            Engine.log.warn "secondary retry count exceededs limit."
            Engine.log.warn_backtrace error.backtrace
            write_abort
            @flush_error_history.clear
            return @flush_interval
          end

        else
          Engine.log.warn "failed to flush the buffer.", :error=>error.to_s, :instance=>object_id
          Engine.log.warn "retry count exceededs limit."
          #@log.warn_backtrace error.backtrace
          write_abort
          @flush_error_history.clear
          return @flush_interval
        end
      end

      def write_abort
        Engine.log.error "throwing away old logs."
        begin
          #@buffer.clear
        rescue
          #Engine.log.error "unexpected error while aborting", :error=>$!.to_s
          Engine.log.error_backtrace
        end
      end

      def calc_retry_wait_time(retry_count)
        @flush_retry_wait * (2 ** retry_count)
      end
    end

  end
end
