#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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

class NullOutputChain
  require 'singleton'
  include Singleton

  def next
  end
end


class Output
  def initialize
  end

  def configure(conf)
  end

  def start
  end

  def shutdown
  end

  #def emit(tag, es, chain)
  #end

  def secondary_init(primary)
    if primary.class != self.class
        $log.warn "type of secondary output should be same as primary output", :primary=>primary.class.to_s, :secondary=>self.class.to_s
    end
  end
end


class OutputThread
  def initialize(output)
    @output = output
    @error_history = []
    @flush_now = false
    @finish = false

    @flush_interval = 60  # TODO default
    @retry_limit = 17  # TODO default
    @retry_wait = 1.0  # TODO default

    @secondary = nil
    @secondary_limit = 8  # TODO default
  end

  attr_accessor :flush_interval, :retry_limit, :retry_wait, :secondary_limit

  def configure(conf)
    if flush_interval = conf['flush_interval']
      @flush_interval = Config.time_value(flush_interval).to_i
    end

    if retry_limit = conf['retry_limit']
      @retry_limit = retry_limit.to_i
      if @retry_limit < 0
        raise ConfigError, "invalid parameter 'retry_limit #{retry_limit}'"
      end
    end

    if retry_wait = conf['retry_wait']
      @retry_wait = Config.time_value(retry_wait)
    end

    # TODO configure retry_pattern

    if econf = conf.elements.select {|e| e.name == 'secondary' }.first
      type = econf['type'] || conf['type']
      @secondary = Plugin.new_output(type)
      @secondary.configure(econf)

      if secondary_limit = conf['secondary_limit']
        @secondary_limit = secondary_limit.to_i
        if @secondary_limit < 0
          raise ConfigError, "invalid parameter 'secondary_limit #{secondary_limit}'"
        end
      end

      @secondary.secondary_init(@output)
    end
  end

  def start
    @mutex = Mutex.new
    @cond = ConditionVariable.new
    @last_try = Engine.now
    #calc_next_time()
    # TODO
    @next_time = @last_try
    @thread = Thread.new(&method(:run))
    @secondary.start if @secondary
  end

  def shutdown
    @finish = true
    @mutex.synchronize {
      @cond.signal
    }
    @secondary.shutdown if @secondary
    Thread.pass
    @thread.join
  end

  def submit_flush
    @mutex.synchronize {
      @flush_now = true
      @cond.signal
    }
    Thread.pass
  end

  private
  def run
    @mutex.lock
    begin
      until @finish

        now = Engine.now

        if @next_time <= now ||
            (@flush_now && @error_history.empty?)

          @mutex.unlock
          begin
            try_flush(now)
          ensure
            @mutex.lock
          end

          now = @last_try = Engine.now
          @flush_now = false
        end

        next_wait = calc_next_time() - now
        if next_wait > 0
          cond_wait(next_wait)
        end

      end
    ensure
      @mutex.unlock
    end

  rescue
    $log.error "error on output thread", :error=>$!.to_s
    $log.error_backtrace
    raise
  ensure
    @mutex.synchronize {
      @output.before_shutdown
    }
  end

  def calc_next_time
    # TODO retry pattern
    if @error_history.empty?
      @next_time = @last_try + @flush_interval
    elsif @error_history.size <= @retry_limit
      @next_time = @last_try + @retry_wait * (2 ** (@error_history.size-1))
    else
      @next_time = @last_try + @retry_wait * (2 ** (@error_history.size-2-@retry_limit))
    end
  end

  if ConditionVariable.new.method(:wait).arity == 1
    $log.warn "WARNING: Running on Ruby 1.8. Ruby 1.9 is recommended."
    require 'timeout'
    def cond_wait(sec)
      Timeout.timeout(sec) {
        @cond.wait(@mutex)
      }
    rescue Timeout::Error
    end
  else
    def cond_wait(sec)
      @cond.wait(@mutex, sec)
    end
  end

  def try_flush(time)
    while true

      begin
        if @error_history.size > @retry_limit
          has_next = @output.flush_secondary(@secondary)
        else
          has_next = @output.try_flush
        end

        @error_history.clear
        next if has_next

      rescue
        $log.warn "failed to flush the buffer", :error=>$!.to_s
        $log.warn_backtrace

        error_count = @error_history.size
        @error_history << time

        if error_count < @retry_limit
          $log.info "retrying."

        elsif @secondary
          if error_count == @retry_limit
            $log.warn "retry count exceededs limit. falling back to secondary output."
            next  # retry immediately
          elsif error_count <= @retry_limit + @secondary_limit
            $log.info "retrying secondary."
          else
            $log.warn "secondary retry count exceededs limit."
            write_abort
          end

        else
          $log.warn "retry count exceededs limit."
          write_abort
        end

      end

      break
    end
  end

  def write_abort
    $log.error "throwing away old logs."
    begin
      @output.write_abort
    rescue
      $log.error "unexpected error while aborting", :error=>$!.to_s
      $log.error_backtrace
    end
    @error_history.clear
  end
end


class BufferedOutput < Output
  def initialize
    @buffer_type = 'memory'
  end

  attr_accessor :buffer_type

  def configure(conf)
    if buffer_type = conf['buffer_type']
      @buffer_type = buffer_type
    end
    @buffer = Plugin.new_buffer(@buffer_type)
    @buffer.configure(conf)

    @writer = OutputThread.new(self)
    @writer.configure(conf)
  end

  def start
    @buffer.start
    @writer.start
  end

  def shutdown
    @writer.shutdown
    @buffer.shutdown
  end

  def emit(tag, es, chain, key="")
    data = format_stream(tag, es)
    if @buffer.emit(key, data, chain)
      submit_flush
    end
  end

  def submit_flush
    @writer.submit_flush
  end

  def format_stream(tag, es)
    out = ''
    es.each {|event|
      out << format(tag, event)
    }
    out
  end

  #def format(tag, event)
  #end

  #def write(chunk)
  #end

  def try_flush
    if @buffer.queue_size == 0
      @buffer.synchronize do
        @buffer.keys.each {|key|
          @buffer.push(key)
        }
      end
    end
    @buffer.pop(self)
  end

  def before_shutdown
    begin
      @buffer.before_shutdown(self)
    rescue
      $log.warn "before_shutdown failed", :error=>$!.to_s
      $log.warn_backtrace
    end
  end

  def write_abort
    @buffer.clear!
  end

  def flush_secondary(secondary)
    @buffer.pop(secondary)
  end
end


class TimeSlicedOutput < BufferedOutput
  def initialize
    super
    @time_format = '%Y%m%d'
    @time_wait = 10*60  # TODO default value
    @localtime = true
    @buffer_type = 'file'  # overwrite default buffer_type
    #@ignore_old = false   # TODO
    # TODO @flush_interval = 60  # overwrite default flush_interval
  end

  attr_accessor :time_format, :time_wait, :localtime

  def configure(conf)
    super

    # TODO timezone
    if conf['utc']
      @localtime = false
    elsif conf['localtime']
      @localtime = true
    end

    if time_format = conf['time_format']
      @time_format = time_format
    end

    if time_wait = conf['time_wait']
      @time_wait = Config.time_value(time_wait)
    end

    if @localtime
      @time_slicer = Proc.new {|time|
        Time.at(time).strftime(@time_format)
      }
    else
      @time_slicer = Proc.new {|time|
        Time.at(time).utc.strftime(@time_format)
      }
    end

    @time_slice_cache_interval = time_slice_cache_interval
    @before_tc = nil
    @before_key = nil
  end

  def emit(tag, es, chain)
    es.each {|e|
      tc = e.time / @time_slice_cache_interval
      if @before_tc == tc
        key = @before_key
      else
        @before_tc = tc
        key = @time_slicer.call(e.time)
        @before_key = key
      end
      data = format(tag, e)
      if @buffer.emit(key, data, chain)
        submit_flush
      end
    }
  end

  def try_flush
    nowslice = @time_slicer.call(Engine.now.to_i - @time_wait)
    @buffer.synchronize do
      @buffer.keys.each {|key|
        if key < nowslice
          @buffer.push(key)
        end
      }
    end
    @buffer.pop(self)
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

