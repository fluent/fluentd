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
module Fluentd
  module Plugin

    require 'serverengine'

    class BufferedOutput < Output
      def initialize
        super
        @threads = []
        @next_flush_time = 0
        @flush_error_history = []
        @flush_retry_mutex = Mutex.new
      end

      config_param :buffer_type, :string, default: 'memory'

      config_param :flush_interval, :time, default: 10
      config_param :flush_minimum_interval, :time, default: 0

      config_param :flush_retry_wait, :time, default: 1
      config_param :flush_retry_limit, :integer, default: 11

      config_param :flush_threads, :integer, default: 2

      config_param :flush_at_shutdown, :boolean, default: nil

      def configure(conf)
        # backward compatibility
        conf['flush_threads'] ||= conf['num_threads']

        super

        @buffer = Engine.plugins.new_buffer(@buffer_type)
        @buffer.configure(conf)

        if @flush_at_shutdown.nil? && !@buffer.persistent?
          @flush_at_shutdown = true
        end

        # TODO @secondary
        #if @secondary && !is_a?(ObjectBufferedOutput)
        ## TODO warn
        #end
      end

      def start
        @next_flush_time = Time.now.to_i + @flush_interval
        @flush_threads.times do
          @threads << FlushThread.new(&method(:try_flush))
        end
        @buffer.start
        super
      end

      def shutdown
        @threads.reverse_each {|t| t.stop }
        @threads.reverse_each {|t| t.join }
        super
      end

      def close
        super
        @buffer.shutdown
        # TODO flush_at_shutdown
      end

      def emit(tag, time, record)
        emits(tag, SingleEventCollection.new(time, record))
      end

      def emits(tag, es)
        @buffer.open do |a|
          es.each {|time,record|
            key = buffer_key(tag, time, record)
            data = format(tag, time, record)
            a.append(key, data)
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

      class FlushThread
        MAX_BLOCKING_TIME = 1

        def initialize(&try_flush)
          @stop_flag = ServerEngine::BlockingFlag.new
          @try_flush = try_flush
          @thread = Thread.new(&method(:run))
        end

        def stop
          @stop_flag.set!
        end

        def join
          @thread.join
        end

        def run
          until @stop_flag.set?
            wait = @try_flush.call
            if wait > 0
              @stop_flag.wait_for_set([wait, MAX_BLOCKING_TIME].min)
            end
          end
        end
      end

      def try_flush
        unless @flush_error_history.empty?
          # allow parallel flush only if this trial is not in retrying mode
          retry_lock = @flush_retry_mutex.lock
          retry_count = @flush_error_history.size
          if retry_count == 0
            retry_lock.unlock
            retry_count = nil
          end
        end

        begin
          now = Time.now.to_i
          if @next_flush_time > now
            return @next_flush_time - now
          end

          has_next = @buffer.acquire {|chunk|
            if @secondary && retry_count && retry_count > @flush_retry_limit
              # write to @secondary if retry_count > @flush_retry_limit
              flush_chunk_secondary(chunk)
            else
              flush_chunk(chunk)
            end
          }

          if retry_count
            # retry succeeded
            log.warn "retry succeeded.", instance: object_id
            @flush_error_history.clear
          end

          now = Time.now.to_i
          if has_next
            wait_time = @flush_minimum_interval
          else
            wait_time = @flush_interval
          end

          # return wait time of this thread
          @next_flush_time = now + wait_time
          return wait_time

        rescue => error
          # flush error
          retry_lock ||= @flush_retry_mutex.lock
          @flush_error_history << error

          wait_time = flush_failed(error)
          @next_flush_time = now + wait_time
          return wait_time

        ensure
          retry_lock.unlock if retry_lock
        end

        # try_flush should never raise exceptions
      end

      def flush_chunk(chunk)
        write(chunk)
      end

      def flush_chunk_secondary(chunk)
        if is_a?(ObjectBufferedOutput)
          @secondary.emits(chunk.tag, chunk)
        else
          @secondary.write(chunk)
        end
      end

      def flush_failed(error)
        # error handling
        retry_count = @flush_error_history.size - 1

        if retry_count < @flush_retry_limit
          wait_time = calc_retry_wait_time(retry_count)
          log.warn "temporarily failed to flush the buffer, next retry will be at #{Time.at(@next_flush_time + wait_time)}.", error: error.to_s, instance: object_id
          log.warn_backtrace error.backtrace
          return wait_time

        elsif @secondary
          if retry_count == @flush_retry_limit
            log.warn "failed to flush the buffer.", error: error.to_s, instance: object_id
            log.warn "retry count exceededs limit. falling back to secondary output."
            log.warn_backtrace error.backtrace
            return 0  # retry immediately

          elsif retry_count <= @flush_retry_limit + @secondary_limit
            wait_time = calc_retry_wait_time(retry_count - @flush_retry_limit - 1)
            log.warn "failed to flush the buffer, next retry will be with secondary output at #{Time.at(@next_flush_time + wait_time)}.", error: error.to_s, instance: object_id
            log.warn_backtrace error.backtrace
            return wait_time

          else
            log.warn "failed to flush the buffer.", error: error.to_s, instance: object_id
            log.warn "secondary retry count exceededs limit."
            log.warn_backtrace error.backtrace
            write_abort
            @flush_error_history.clear
            return @flush_interval
          end

        else
          log.warn "failed to flush the buffer.", error: error.to_s, instance: object_id
          log.warn "retry count exceededs limit."
          log.warn_backtrace error.backtrace
          write_abort
          @flush_error_history.clear
          return @flush_interval
        end
      end

      def write_abort
        log.error "throwing away old logs."
        begin
          #@buffer.clear
          # TODO
        rescue
          log.error "unexpected error while aborting", error: $!.to_s
          log.error_backtrace
        end
      end

      def calc_retry_wait_time(retry_count)
        @flush_retry_wait * (2 ** retry_count)
      end
    end

  end
end
