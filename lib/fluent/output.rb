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
end


class OutputThread
  def initialize(output)
    @output = output
    @error_history = []
    @flush_now = false
    @finish = false

    @flush_interval = 60  # TODO default
    @retry_limit = 10  # TODO default
    @retry_wait = 1.0  # TODO default
  end

  attr_accessor :flush_interval, :retry_limit, :retry_pattern, :retry_wait

  def configure(conf)
    if flush_interval = conf['flush_interval']
      @flush_interval = Config.time_value(flush_interval).to_i
    end

    if retry_limit = conf['retry_limit']
      @retry_limit = retry_limit.to_i
    end

    if retry_wait = conf['retry_wait']
      @retry_wait = Config.time_value(retry_wait)
    end

    # TODO configure retry_pattern
  end

  def start
    @mutex = Mutex.new
    @cond = ConditionVariable.new
    @last_try = Engine.now
    #calc_next_time()
    # TODO
    @next_time = @last_try
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
    $log.error "error on output thread: ", $!
    $log.error_backtrace
    raise
  ensure
    @mutex.synchronize {
      @output.before_shutdown
    }
  end

  def calc_next_time
    if @error_history.empty?
      @next_time = @last_try + @flush_interval
    else
      # TODO retry pattern
      @next_time = @last_try + @retry_wait * (2 ** (@error_history.size-1))
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
        has_next = @output.try_flush
        @error_history.clear
        next if has_next

      rescue
        $log.warn "failed to flush the buffer: ", $!
        $log.warn_backtrace

        @error_history << time
        if @error_history.size > @retry_limit
          $log.warn "retry count exceededs limit. aborted."
          write_abort
        else
          $log.info "retrying."
        end
      end

      break
    end
  end

  def write_abort
    # TODO clear buffer? configurable?
    begin
      @output.write_abort
    rescue
      $log.warn "clear buffer failed: ", $!
      $log.warn_backtrace
    end
    @error_history.clear
  end
end


class BufferedOutput < Output
  def initialize
  end

  def configure(conf)
    buffer_type = conf['buffer_type'] || 'memory'
    @buffer = Plugin.new_buffer(buffer_type)
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

  def emit(tag, es, chain)
    key = ""
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
    @buffer.synchronize do
      @buffer.keys.each {|key|
        @buffer.push(key)
      }
    end
    @buffer.pop(self)
  end

  def before_shutdown
    begin
      @buffer.before_shutdown(self)
    rescue
      $log.warn "before_shutdown failed: ", $!
      $log.warn_backtrace
    end
  end

  def write_abort
    @buffer.clear!
  end
end


end

