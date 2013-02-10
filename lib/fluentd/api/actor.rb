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

  class Future
    def initialize
      @set = false
      @result = nil
      @exception = nil
      @mutex = Mutex.new
      @cond = ConditionVariable.new
    end

    def set?
      @set
    end

    def set!(result, exception=nil)
      @mutex.synchronize do
        if @set
          raise "already set"  # TODO error class
        end
        @result = result
        @exception = exception
        @set = true
      end
    end

    def value
      join
      if @exception
        raise @exception
      end
      return @result
    end

    def join
      @mutex.synchronize do
        until @set
          @cond.wait(@mutex)
        end
      end
      self
    end
  end


  class AsyncExecutor
    def initialize
      @queue = Queue.new
    end

    def submit(callable)
      f = Future.new

      task = Proc.new do
        begin
          result = callable.call
        rescue Exception => exception
        end
        f.set!(result, exception)
      end

      @queue.push(task)

      return f
    end

    def run_once
      while task = @queue.pop(true)
        task.call
      end
      nil
    end
  end

  class ThreadExecutor
    def submit(callable)
      f = Future

      Thread.new do
        begin
          result = callable.call
        rescue Exception => exception
        end
        f.set!(result, exception)
      end

      return f
    end
  end

  class CachedThreadPoolExecutor
    # TODO
  end

  class BasicActor
    require 'timers'
    require 'nio'

    class IOKey
      def initialize(actor, monitor, io)
        @actor = actor
        @monitor = monitor
        @io = io
      end

      attr_reader :io

      def detach
        @actor.detach_io(@monitor)
      end
    end

    class TimerKey
      def initialize(actor, timer)
        @actor = actor
        @timer = timer
      end

      def detach
        @actor.detach_timer(@timer)
      end
    end

    def initialize
      @timers = Timers.new
      @selector = NIO::Selector.new
      @async = AsyncExecutor.new
      @parallel = ThreadExecutor.new
    end

    def start
      @thread = Thread.new(&method(:run))
      nil
    end

    def shutdown
      async {
        unless @selector.closed?
          @selector.close
          @selector.wakeup
        end
      }.join
      if @thread
        @thread.join
        @thread = nil
      end
      nil
    end

    def run
      until @selector.closed?
        run_once
      end
    end

    def run_once
      set = @selector.select(wait_interval)

      set.each {|monitor|
        begin
          monitor.call  # `call` is defined at watch_io method
        rescue
          handle_io_error(monitor.io, $!)
        end

        if monitor.io.closed?
          monitor.close
        end
      }

      @timers.fire
      @async.run_once

      nil
    end

    def handle_io_error(io, error)
      # TODO log?
      io.close unless io.closed?
    end

    def wait_interval
      @timers.wait_interval
    end

    def watch_readable(io, &block)
      watch_io(io, :r, &block)
    end

    def watch_writable(io, &block)
      watch_io(io, :w, &block)
    end

    def watch_io(io, r_or_w, &block)
      monitor = @selector.register(io, :r)
      monitor.define_singleton_method(:call) {
        block.yield(io)
      }
      return IOKey.new(self, monitor, io)
    end

    def timer(interval_sec, &block)
      timer = @timers.every(interval_sec, &block)
      return TimerKey.new(self, timer)
    end

    def after(sec, &block)
      timer = @timers.after(sec, &block)
      return TimerKey.new(self, timer)
    end

    def detach(key)
      return nil unless key
      key.detach
    end

    def detach_io(monitor)
      monitor.close
      nil
    end

    def detach_timer(timer)
      @timers.delete(timer)
      nil
    end

    def async(&block)
      f = @async.submit(block)
      @selector.wakeup
      return f
    end

    def parallel(&block)
      @parallel.submit(block)
    end
  end


  module SocketActorMixIn
    def create_tcp_thread_server(bind, port, &block)
      listen_tcp(bind, port) {|s|
        begin
          io = s.accept_nonblock
          parallel(io, &block)
        rescue Errno::EAGAIN, Errno::EINTR
        end
      }
    end

    def create_unix_thread_server(bind, port, &block)
      listen_unix(bind, port) {|s|
        begin
          io = s.accept_nonblock
          parallel(io, &block)
        rescue Errno::EAGAIN, Errno::EINTR
        end
      }
    end

    def listen_tcp(bind, port, &block)
      io = TCPServer.new(bind, port)
      watch_readable(io, &block)
    end

    def listen_unix(path, &block)
      io = UNIXServer.new(bind, port)
      watch_readable(io, &block)
    end

    def listen_udp(bind, port, &block)
      io = UDPServer.new(bind, port)
      watch_readable(io, &block)
    end
  end


  class Actor < BasicActor
    include SocketActorMixIn
  end


  module ActorAgentMixin
    def initialize
      @actor = Actor.new
      super
    end

    attr_reader :actor

    def start
      super
      @actor.start
    end

    def shutdown
      @actor.shutdown
      super
    end
  end

end
