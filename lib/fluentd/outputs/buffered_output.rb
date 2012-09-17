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
        @flush_ready = true
        @flush_time = 0
        @flush_interval = 10
        @flush_error_history = []
        @flush_retry_limit = 10
        @flush_retry_wait = 1
      end

      def configure(conf)
        # TODO
        @buffer = Plugins::MemoryBuffer.new
      end

      def start
        @flush_threads << FlushThread.new(method(:try_flush))

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

      def try_flush
        if !@flush_ready && @flush_time > (now = Time.now.to_i)
          return
        end

        @flush_mutex.lock
        locked_mutex = @flush_mutex
        begin
          if !@flush_ready && @flush_time > (now || Time.now.to_i)
            return
          end

          if @buffer.acquire {|tag,chunk|
              # allow parallel flush only if it's not on retrying mode
              no_error = @flush_error_history.empty?
              if no_error
                @flush_ready = true
              else
                @flush_ready = false
              end

              @flush_mutex.unlock
              locked_mutex = nil

              error = nil
              begin
                write(tag, chunk)
              rescue => e
                error = e
              end

              if error
                @flush_mutex.lock
                locked_mutex = @flush_mutex

                if no_error
                  # first error
                  @flush_error_history = [e]
                else
                  @flush_error_history << e
                end
                @flush_ready = false
                flush_failed(error)

              elsif !no_error
                # retry succeeded
                @flush_error_history.clear
                #@log.warn "retry succeeded.", :instance=>object_id
                @flush_ready = true
              end
            }

          else
            # buffer queue is empty
            @flush_ready = false
          end

          if @flush_ready
            return 0
          else
            wait = next_flush_wait
            @flush_time = (now ||= Time.now.to_i) + wait
            return wait
          end

        ensure
          locked_mutex.unlock if locked_mutex
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

      def flush_failed(e)
        error_count = @flush_error_history.size - 1

        if error_count < @flush_retry_limit
          #$log.warn "failed to flush the buffer, retrying.", :error=>e.to_s, :instance=>object_id
          #$log.warn_backtrace e.backtrace

        #elsif @secondary
        #  if error_count == @flush_retry_limit
        #    $log.warn "failed to flush the buffer.", :error=>e.to_s, :instance=>object_id
        #    $log.warn "retry count exceededs limit. falling back to secondary output."
        #    $log.warn_backtrace e.backtrace
        #    @flush_ready = true  # retry immediately
        #
        #  elsif error_count <= @flush_retry_limit + @secondary_limit
        #    $log.warn "failed to flush the buffer, retrying secondary.", :error=>e.to_s, :instance=>object_id
        #    $log.warn_backtrace e.backtrace
        #
        #  else
        #    $log.warn "failed to flush the buffer.", :error=>e.to_s, :instance=>object_id
        #    $log.warn "secondary retry count exceededs limit."
        #    $log.warn_backtrace e.backtrace
        #    write_abort
        #    @flush_error_history.clear
        #  end

        else
          #@log.warn "failed to flush the buffer.", :error=>e.to_s, :instance=>object_id
          #@log.warn "retry count exceededs limit."
          #@log.warn_backtrace e.backtrace
          write_abort
          @flush_error_history.clear
        end
      end

      def next_flush_wait
        esz = @flush_error_history.size
        if esz == 0
          @flush_interval
        else
          @flush_retry_wait * (2 ** (esz-1))
        end
        # TODO retry pattern
        #if @flush_error_history.size <= @flush_retry_limit
        #  @flush_retry_wait * (2 ** (@flush_error_history.size-1))
        #else
        #  # secondary retry
        #  @flush_retry_wait * (2 ** (@flush_error_history.size-2-@flush_retry_limit))
        #end
      end
    end

  end
end
