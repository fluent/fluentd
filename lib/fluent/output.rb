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

require 'thread'

require 'fluent/config'
require 'fluent/configurable'
require 'fluent/engine'
require 'fluent/log'
require 'fluent/plugin'
require 'fluent/status'
require 'fluent/timezone'

module Fluent
  class OutputChain
    def initialize(array, tag, es, chain=NullOutputChain.instance)
      @array = array
      @tag = tag
      @es = es
      @offset = 0
      @chain = chain
    end

    def next
      if @array.length <= @offset
        return @chain.next
      end
      @offset += 1
      result = @array[@offset-1].emit(@tag, @es, self)
      result
    end
  end

  class CopyOutputChain < OutputChain
    def next
      if @array.length <= @offset
        return @chain.next
      end
      @offset += 1
      es = @array.length > @offset ? @es.dup : @es
      result = @array[@offset-1].emit(@tag, es, self)
      result
    end
  end

  class NullOutputChain
    require 'singleton'
    include Singleton

    def next
    end
  end


  class Output
    include Configurable
    include PluginId
    include PluginLoggerMixin

    attr_accessor :router

    def initialize
      super
    end

    def configure(conf)
      super

      if label_name = conf['@label']
        label = Engine.root_agent.find_label(label_name)
        @router = label.event_router
      elsif @router.nil?
        @router = Engine.root_agent.event_router
      end
    end

    def start
    end

    def shutdown
    end

    #def emit(tag, es, chain)
    #end

    def secondary_init(primary)
      if primary.class != self.class
        $log.warn "type of secondary output should be same as primary output", primary: primary.class.to_s, secondary: self.class.to_s
      end
    end
  end

  class OutputThread
    def initialize(output)
      @output = output
      @finish = false
      @next_time = Time.now.to_f + 1.0
    end

    def configure(conf)
    end

    def start
      @mutex = Mutex.new
      @cond = ConditionVariable.new
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @finish = true
      @mutex.synchronize {
        @cond.signal
      }
      Thread.pass
      @thread.join
    end

    def submit_flush
      @mutex.synchronize {
        @next_time = 0
        @cond.signal
      }
      Thread.pass
    end

    private
    def run
      @mutex.lock
      begin
        until @finish
          time = Time.now.to_f

          if @next_time <= time
            @mutex.unlock
            begin
              @next_time = @output.try_flush
            ensure
              @mutex.lock
            end
            next_wait = @next_time - Time.now.to_f
          else
            next_wait = @next_time - time
          end

          cond_wait(next_wait) if next_wait > 0
        end
      ensure
        @mutex.unlock
      end
    rescue
      $log.error "error on output thread", error: $!.to_s
      $log.error_backtrace
      raise
    ensure
      @mutex.synchronize {
        @output.before_shutdown
      }
    end

    def cond_wait(sec)
      @cond.wait(@mutex, sec)
    end
  end


  class BufferedOutput < Output
    def initialize
      super
      @next_flush_time = 0
      @last_retry_time = 0
      @next_retry_time = 0
      @num_errors = 0
      @num_errors_lock = Mutex.new
      @secondary_limit = 8
      @emit_count = 0
    end

    desc 'The buffer type (memory, file)'
    config_param :buffer_type, :string, default: 'memory'
    desc 'The interval between data flushes.'
    config_param :flush_interval, :time, default: 60
    config_param :try_flush_interval, :float, default: 1
    desc 'If true, the value of `retry_value` is ignored and there is no limit'
    config_param :disable_retry_limit, :bool, default: false
    desc 'The limit on the number of retries before buffered data is discarded'
    config_param :retry_limit, :integer, default: 17
    desc 'The initial intervals between write retries.'
    config_param :retry_wait, :time, default: 1.0
    desc 'The maximum intervals between write retries.'
    config_param :max_retry_wait, :time, default: nil
    desc 'The number of threads to flush the buffer.'
    config_param :num_threads, :integer, default: 1
    desc 'The threshold to show slow flush logs'
    config_param :slow_flush_log_threshold, :float, default: 20.0
    desc 'The interval between data flushes for queued chunk.'
    config_param :queued_chunk_flush_interval, :time, default: 1

    def configure(conf)
      super

      @retry_wait = @retry_wait.to_f # converted to Float for calc_retry_wait
      @buffer = Plugin.new_buffer(@buffer_type)
      @buffer.configure(conf)

      if @buffer.respond_to?(:enable_parallel)
        if @num_threads == 1
          @buffer.enable_parallel(false)
        else
          @buffer.enable_parallel(true)
        end
      end

      @writers = (1..@num_threads).map {
        writer = OutputThread.new(self)
        writer.configure(conf)
        writer
      }

      if sconf = conf.elements.select {|e| e.name == 'secondary' }.first
        type = sconf['@type'] || conf['@type'] || sconf['type'] || conf['type']
        @secondary = Plugin.new_output(type)
        @secondary.router = router
        @secondary.configure(sconf)

        if secondary_limit = conf['secondary_limit']
          @secondary_limit = secondary_limit.to_i
          if @secondary_limit < 0
            raise ConfigError, "invalid parameter 'secondary_limit #{secondary_limit}'"
          end
        end

        @secondary.secondary_init(self)
      end

      Status.register(self, "queue_size") { @buffer.queue_size }
      Status.register(self, "emit_count") { @emit_count }
    end

    def start
      @next_flush_time = Time.now.to_f + @flush_interval
      @buffer.start
      @secondary.start if @secondary
      @writers.each {|writer| writer.start }
      @writer_current_position = 0
      @writers_size = @writers.size
    end

    def shutdown
      @writers.each {|writer| writer.shutdown }
      @secondary.shutdown if @secondary
      @buffer.shutdown
    end

    def emit(tag, es, chain, key="")
      @emit_count += 1
      data = format_stream(tag, es)
      if @buffer.emit(key, data, chain)
        submit_flush
      end
    end

    def submit_flush
      # Without locks: it is rough but enough to select "next" writer selection
      @writer_current_position = (@writer_current_position + 1) % @writers_size
      @writers[@writer_current_position].submit_flush
    end

    def format_stream(tag, es)
      out = ''
      es.each {|time,record|
        out << format(tag, time, record)
      }
      out
    end

    #def format(tag, time, record)
    #end

    #def write(chunk)
    #end

    def enqueue_buffer(force = false)
      @buffer.keys.each { |key|
        @buffer.push(key)
      }
    end

    def try_flush
      time = Time.now.to_f

      empty = @buffer.queue_size == 0
      if empty && @next_flush_time < (now = Time.now.to_f)
        @buffer.synchronize do
          if @next_flush_time < now
            enqueue_buffer
            @next_flush_time = now + @flush_interval
            empty = @buffer.queue_size == 0
          end
        end
      end
      if empty
        return time + @try_flush_interval
      end

      begin
        retrying = !@num_errors.zero?

        if retrying
          @num_errors_lock.synchronize do
            if retrying = !@num_errors.zero? # re-check in synchronize
              if @next_retry_time >= time
                # allow retrying for only one thread
                return time + @try_flush_interval
              end
              # assume next retry failes and
              # clear them if when it succeeds
              @last_retry_time = time
              @num_errors += 1
              @next_retry_time += calc_retry_wait
            end
          end
        end

        chunk_write_start = Time.now

        if @secondary && !@disable_retry_limit && @num_errors > @retry_limit
          has_next = flush_secondary(@secondary)
        else
          has_next = @buffer.pop(self)
        end

        elapsed_time = Time.now - chunk_write_start
        if elapsed_time > @slow_flush_log_threshold
          $log.warn "buffer flush took longer time than slow_flush_log_threshold:",
                    plugin_id: plugin_id, elapsed_time: elapsed_time, slow_flush_log_threshold: @slow_flush_log_threshold
        end

        # success
        if retrying
          @num_errors = 0
          # Note: don't notify to other threads to prevent
          #       burst to recovered server
          $log.warn "retry succeeded.", plugin_id: plugin_id
        end

        if has_next
          return Time.now.to_f + @queued_chunk_flush_interval
        else
          return time + @try_flush_interval
        end

      rescue => e
        if retrying
          error_count = @num_errors
        else
          # first error
          error_count = 0
          @num_errors_lock.synchronize do
            if @num_errors.zero?
              @last_retry_time = time
              @num_errors += 1
              @next_retry_time = time + calc_retry_wait
            end
          end
        end

        if @disable_retry_limit || error_count < @retry_limit
          $log.warn "temporarily failed to flush the buffer.", next_retry: Time.at(@next_retry_time), error_class: e.class.to_s, error: e.to_s, plugin_id: plugin_id
          $log.warn_backtrace e.backtrace

        elsif @secondary
          if error_count == @retry_limit
            $log.warn "failed to flush the buffer.", error_class: e.class.to_s, error: e.to_s, plugin_id: plugin_id
            $log.warn "retry count exceededs limit. falling back to secondary output."
            $log.warn_backtrace e.backtrace
            retry  # retry immediately
          elsif error_count <= @retry_limit + @secondary_limit
            $log.warn "failed to flush the buffer, next retry will be with secondary output.", next_retry: Time.at(@next_retry_time), error_class: e.class.to_s, error: e.to_s, plugin_id: plugin_id
            $log.warn_backtrace e.backtrace
          else
            $log.warn "failed to flush the buffer.", error_class: e.class, error: e.to_s, plugin_id: plugin_id
            $log.warn "secondary retry count exceededs limit."
            $log.warn_backtrace e.backtrace
            write_abort
            @num_errors = 0
          end

        else
          $log.warn "failed to flush the buffer.", error_class: e.class.to_s, error: e.to_s, plugin_id: plugin_id
          $log.warn "retry count exceededs limit."
          $log.warn_backtrace e.backtrace
          write_abort
          @num_errors = 0
        end

        return @next_retry_time
      end
    end

    def force_flush
      @num_errors_lock.synchronize do
        @next_retry_time = Time.now.to_f - 1
      end
      @buffer.synchronize {
        enqueue_buffer(true)
      }
      submit_flush
    end

    def before_shutdown
      begin
        @buffer.before_shutdown(self)
      rescue
        $log.warn "before_shutdown failed", error: $!.to_s
        $log.warn_backtrace
      end
    end

    def calc_retry_wait
      # TODO retry pattern
      wait = if @disable_retry_limit || @num_errors <= @retry_limit
               @retry_wait * (2 ** (@num_errors - 1))
             else
               # secondary retry
               @retry_wait * (2 ** (@num_errors - 2 - @retry_limit))
             end
      retry_wait = wait.finite? ? wait + (rand * (wait / 4.0) - (wait / 8.0)) : wait
      @max_retry_wait ? [retry_wait, @max_retry_wait].min : retry_wait
    end

    def write_abort
      $log.error "throwing away old logs."
      begin
        @buffer.clear!
      rescue
        $log.error "unexpected error while aborting", error: $!.to_s
        $log.error_backtrace
      end
    end

    def flush_secondary(secondary)
      @buffer.pop(secondary)
    end
  end


  class ObjectBufferedOutput < BufferedOutput
    def initialize
      super
    end

    def emit(tag, es, chain)
      @emit_count += 1
      data = es.to_msgpack_stream
      key = tag
      if @buffer.emit(key, data, chain)
        submit_flush
      end
    end

    module BufferedEventStreamMixin
      include Enumerable

      def repeatable?
        true
      end

      def each(&block)
        msgpack_each(&block)
      end

      def to_msgpack_stream
        read
      end
    end

    def write(chunk)
      chunk.extend(BufferedEventStreamMixin)
      write_objects(chunk.key, chunk)
    end
  end


  class TimeSlicedOutput < BufferedOutput
    require 'fluent/timezone'

    def initialize
      super
      @localtime = true
      #@ignore_old = false   # TODO
    end

    desc 'The time format used as part of the file name.'
    config_param :time_slice_format, :string, default: '%Y%m%d'
    desc 'The amount of time Fluentd will wait for old logs to arrive.'
    config_param :time_slice_wait, :time, default: 10*60
    desc 'Parse the time value in the specified timezone'
    config_param :timezone, :string, default: nil
    config_set_default :buffer_type, 'file'  # overwrite default buffer_type
    config_set_default :buffer_chunk_limit, 256*1024*1024  # overwrite default buffer_chunk_limit
    config_set_default :flush_interval, nil
    config_set_default :slow_flush_log_threshold, 40.0

    attr_accessor :localtime
    attr_reader :time_slicer # for test

    def configure(conf)
      super

      if conf['utc']
        @localtime = false
      elsif conf['localtime']
        @localtime = true
      end

      if conf['timezone']
        @timezone = conf['timezone']
        Fluent::Timezone.validate!(@timezone)
      end

      if @timezone
        @time_slicer = Timezone.formatter(@timezone, @time_slice_format)
      elsif @localtime
        @time_slicer = Proc.new {|time|
          Time.at(time).strftime(@time_slice_format)
        }
      else
        @time_slicer = Proc.new {|time|
          Time.at(time).utc.strftime(@time_slice_format)
        }
      end

      @time_slice_cache_interval = time_slice_cache_interval
      @before_tc = nil
      @before_key = nil

      if @flush_interval
        if conf['time_slice_wait']
          $log.warn "time_slice_wait is ignored if flush_interval is specified: #{conf}"
        end
        @enqueue_buffer_proc = Proc.new do
          @buffer.keys.each {|key|
            @buffer.push(key)
          }
        end

      else
        @flush_interval = [60, @time_slice_cache_interval].min
        @enqueue_buffer_proc = Proc.new do
          nowslice = @time_slicer.call(Engine.now - @time_slice_wait)
          @buffer.keys.each {|key|
            if key < nowslice
              @buffer.push(key)
            end
          }
        end
      end
    end

    def emit(tag, es, chain)
      @emit_count += 1
      formatted_data = {}
      es.each {|time,record|
        begin
          tc = time / @time_slice_cache_interval
          if @before_tc == tc
            key = @before_key
          else
            @before_tc = tc
            key = @time_slicer.call(time)
            @before_key = key
          end
        rescue => e
          @router.emit_error_event(tag, Engine.now, {'time' => time, 'record' => record}, e)
          next
        end

        formatted_data[key] ||= ''
        formatted_data[key] << format(tag, time, record)
      }
      formatted_data.each { |key, data|
        if @buffer.emit(key, data, chain)
          submit_flush
        end
      }
    end

    def enqueue_buffer(force = false)
      if force
        @buffer.keys.each { |key|
          @buffer.push(key)
        }
      else
        @enqueue_buffer_proc.call
      end
    end

    #def format(tag, event)
    #end

    private
    def time_slice_cache_interval
      if @time_slicer.call(0) != @time_slicer.call(60-1)
        return 1
      elsif @time_slicer.call(0) != @time_slicer.call(60*60-1)
        return 30
      elsif @time_slicer.call(0) != @time_slicer.call(24*60*60-1)
        return 60*30
      else
        return 24*60*30
      end
    end
  end


  class MultiOutput < Output
    #def outputs
    #end
  end
end
