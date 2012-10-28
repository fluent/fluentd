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
  module Outputs

    class BufferedOutput < BasicOutput
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
          @finished = true
          if @thread
            signal!
            @thread.join
          end
        end

        def signal!
          @mutex.synchronize do
            @cond.broadcast
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
        @flush_threads = []
        @flush_mutex = Mutex.new
        #@flush_ready = true
        @flush_time = 0
        @flush_interval = 10
        @flush_error_history = []
        @flush_retry_limit = 11
        @flush_retry_wait = 1
      end

      def configure(conf)
        # TODO
        @buffer = Plugins::MemoryBuffer.new
        @flush_interval = 1
      end

      def start
        10.times do
          @flush_threads << FlushThread.new(method(:try_flush))
        end

        @flush_threads.each {|t|
          t.start
        }

        @buffer.start
      end

      def stop
        @buffer.stop if @buffer
      end

      def shutdown
        @buffer.shutdown if @buffer
        @flush_threads.reverse_each {|t|
          t.shutdown
        }
      end

      def open(tag, &block)
        @buffer.open(tag, &block)
      end

      #def write(tag, chunk)
      #end

      private

      class EnsureMutex
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

        def ensure_lock
          lock unless @locked
        end

        def ensure_unlock
          unlock if @locked
        end
      end

      def try_flush
        now = Time.now.to_i

        return @flush_time - now if @flush_time > now

        flush_lock = EnsureMutex.new(@flush_mutex)
        flush_lock.lock
        begin
          return @flush_time - now if @flush_time > now

          acquired = false
          @buffer.acquire {|tag,chunk|
            acquired = true

            # allow parallel flush only if this trial is not in retrying mode
            retrying = !@flush_error_history.empty?
            unless retrying
              flush_lock.unlock
            end

            error = flush_chunk(tag, chunk)
            now = Time.now.to_i

            if error
              flush_lock.ensure_lock
              @flush_error_history << error
              retry_wait = flush_failed
              @flush_time = now + retry_wait
            elsif retrying
              # retry succeeded; locked section
              #@log.warn "retry succeeded.", :instance=>object_id
              @flush_error_history.clear
              @flush_time = now + @flush_interval
            end
          }

          unless acquired  # buffer queue is empty
            # flush_lock is locked
            @flush_time = now + @flush_interval
          end

          # try next flush soon while buffer is not empty
          return @flush_time - now

        ensure
          flush_lock.ensure_unlock
        end
      end

      def flush_chunk(tag, chunk)
        begin
          write(tag, chunk)
          return nil
        rescue => e
          return e
        end
      end

      def flush_failed
        # error handling
        error_count = @flush_error_history.size - 1

        if error_count < @flush_retry_limit
          #$log.warn "temporarily failed to flush the buffer, next retry will be at #{Time.at(@next_retry_time)}.", :error=>e.to_s, :instance=>object_id
          #$log.warn_backtrace e.backtrace
          return calc_retry_wait_time(error_count)

        elsif @secondary
          if error_count == @flush_retry_limit
            #$log.warn "failed to flush the buffer.", :error=>e.to_s, :instance=>object_id
            #$log.warn "retry count exceededs limit. falling back to secondary output."
            #$log.warn_backtrace e.backtrace
            return 0  # retry immediately

          elsif error_count <= @flush_retry_limit + @secondary_limit
            #$log.warn "failed to flush the buffer, next retry will be with secondary output at #{Time.at(@next_retry_time)}.", :error=>e.to_s, :instance=>object_id
            $log.warn_backtrace e.backtrace
            return calc_retry_wait_time(error_count - @flush_retry_limit - 1)

          else
            #$log.warn "failed to flush the buffer.", :error=>e.to_s, :instance=>object_id
            #$log.warn "secondary retry count exceededs limit."
            $log.warn_backtrace e.backtrace
            write_abort
            @flush_error_history.clear
            return @flush_interval
          end

        else
          #$log.warn "failed to flush the buffer.", :error=>e.to_s, :instance=>object_id
          #$log.warn "retry count exceededs limit."
          #@log.warn_backtrace e.backtrace
          write_abort
          @flush_error_history.clear
          return @flush_interval
        end
      end

      def write_abort
        #$log.error "throwing away old logs."
        begin
          #@buffer.clear!
        rescue
          ##$log.error "unexpected error while aborting", :error=>$!.to_s
          $log.error_backtrace
        end
      end

      def calc_retry_wait_time(error_count)
        @flush_retry_wait * (2 ** error_count)
      end
    end

  end
end
