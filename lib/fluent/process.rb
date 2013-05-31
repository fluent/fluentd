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


class DetachProcessManager
  require 'singleton'
  include Singleton

  class Broker
    def initialize
    end

    def engine
      Engine
    end
  end

  def initialize
    require 'drb'
    DRb.start_service(create_drb_uri, Broker.new)
    @parent_uri = DRb.uri
  end

  def fork(delegate_object)
    ipr, ipw = IO.pipe  # child Engine.emit_stream -> parent Engine.emit_stream
    opr, opw = IO.pipe  # parent target.emit -> child target.emit

    pid = Process.fork
    if pid
      # parent process
      ipw.close
      opr.close
      forward_thread = process_parent(ipr, opw, pid, delegate_object)
      return pid, forward_thread
    end

    # child process
    ipr.close
    opw.close
    forward_thread = process_child(ipw, opr, delegate_object)
    return nil, forward_thread
  end

  private
  def read_header(ipr)
    sz = ipr.read(4).unpack('N')[0]
    ipr.read(sz)
  end

  def send_header(ipw, data)
    ipw.write [data.bytesize].pack('N')
    ipw.write data
    ipw.flush
  end

  def create_drb_uri
    "drbunix:"  # TODO
  end

  private
  def process_child(ipw, opr, delegate_object)
    DRb.start_service(create_drb_uri, delegate_object)
    child_uri = DRb.uri

    send_header(ipw, child_uri)

    # override target.emit_stream to write event stream to the pipe
    fwd = new_forwarder(ipw, 0.5)  # TODO interval
    Engine.define_singleton_method(:emit_stream) do |tag,es|
      fwd.emit(tag, es)
    end

    # read event stream from the pipe and forward to target.emit
    forward_thread = Thread.new(opr, delegate_object, &method(:output_forward_main))

    # override global methods to call parent process
    override_shared_methods(@parent_uri)

    return forward_thread
  end

  def override_shared_methods(parent_uri)
    broker = DRbObject.new_with_uri(parent_uri)
    shared_methods.each {|(broker_accessor,target,name)|
      remote = broker.send(broker_accessor)
      target.define_singleton_method(name) do |*args,&block|
        remote.send(name, *args, &block)
      end
    }
  end

  def shared_methods
    [
      #[:engine, Engine, :flush!],
      #[:engine, Engine, :stop],
    ]
  end

  def process_parent(ipr, opw, pid, delegate_object)
    child_uri = read_header(ipr)

    # read event stream from the pipe and forward to Engine.emit_stream
    forward_thread = Thread.new(ipr, pid, &method(:input_forward_main))

    # note: don't override methods in parent process
    #       because another process may fork after overriding
    #override_delegate_methods(delegate_object, child_uri)

    # return forwarder for DetachProcessMixin to
    # override target.emit and write event stream to the pipe
    fwd = new_forwarder(opw, 0.5)  # TODO interval
    # note: override emit method on DetachProcessMixin
    forward_thread.define_singleton_method(:forwarder) do
      fwd
    end

    return forward_thread
  end

  #def override_delegate_methods(target, child_uri)
  #  remote = DRbObject.new_with_uri(child_uri)
  #  delegate_methods(target).each {|name|
  #    target.define_singleton_method(name) do |*args,&block|
  #      remote.send(name, *args, &block)
  #    end
  #  }
  #end
  #
  #def delegate_methods(target)
  #  target.methods - Object.public_instance_methods
  #end

  def output_forward_main(opr, target)
    read_event_stream(opr) {|tag,es|
      # FIXME error handling
      begin
        target.emit(tag, es, NullOutputChain.instance)
      rescue
        $log.warn "failed to emit", :error=>$!.to_s, :pid=>Process.pid
        $log.warn_backtrace
      end
    }
  rescue
    $log.error "error on output process forwarding thread", :error=>$!.to_s, :pid=>Process.pid
    $log.error_backtrace
    raise
  end

  def input_forward_main(ipr, pid)
    read_event_stream(ipr) {|tag,es|
      # FIXME error handling
      begin
        Engine.emit_stream(tag, es)
      rescue
        $log.warn "failed to emit", :error=>$!.to_s, :pid=>Process.pid
        $log.warn_backtrace
      end
    }
  rescue
    $log.error "error on input process forwarding thread", :error=>$!.to_s, :pid=>Process.pid
    $log.error_backtrace
    raise
  end

  def read_event_stream(r, &block)
    u = MessagePack::Unpacker.new(r)
    cached_unpacker = $use_msgpack_5 ? nil : MessagePack::Unpacker.new
    begin
      #buf = ''
      #map = {}
      #while true
      #  r.readpartial(64*1024, buf)
      #  u.feed_each(buf) {|tag,ms|
      #    if msbuf = map[tag]
      #      msbuf << ms
      #    else
      #      map[tag] = ms
      #    end
      #  }
      #  unless map.empty?
      #    map.each_pair {|tag,ms|
      #      es = MessagePackEventStream.new(ms, cached_unpacker)
      #      block.call(tag, es)
      #    }
      #    map.clear
      #  end
      #end
      u.each {|tag,ms|
        es = MessagePackEventStream.new(ms, cached_unpacker)
        block.call(tag, es)
      }
    rescue EOFError
    ensure
      r.close
    end
  end

  def new_forwarder(w, interval)
    if interval < 0.2  # TODO interval
      Forwarder.new(w)
    else
      DelayedForwarder.new(w, interval)
    end
  end

  class Forwarder
    def initialize(w)
      @w = w
    end

    def emit(tag, es)
      ms = es.to_msgpack_stream
      #[tag, ms].to_msgpack(@w)  # not thread safe
      @w.write [tag, ms].to_msgpack
    end
  end

  class DelayedForwarder
    def initialize(w, interval)
      @w = w
      @interval = interval
      @buffer = {}
      Thread.new(&method(:run))
    end

    def emit(tag, es)
      if ms = @buffer[tag]
        ms << es.to_msgpack_stream
      else
        @buffer[tag] = es.to_msgpack_stream
      end
    end

    def run
      while true
        sleep @interval
        @buffer.keys.each {|tag|
          if ms = @buffer.delete(tag)
            [tag, ms].to_msgpack(@w)
            #@w.write [tag, ms].to_msgpack
          end
        }
      end
    rescue
      $log.error "error on forwerder thread", :error=>$!.to_s
      $log.error_backtrace
      raise
    end
  end

  class MultiForwarder
    def initialize(forwarders)
      @forwarders = forwarders
      @rr = 1
    end

    def emit(tag, es)
      forwarder = @forwarders[@rr]
      @rr = (@rr + 1) % @forwarders.length
      forwarder.emit(tag, es)
    end
  end
end


module DetachProcessImpl
  def on_detach_process(i)
  end

  protected
  def detach_process_impl(num, &block)
    children = []

    num.times do |i|
      pid, forward_thread = DetachProcessManager.instance.fork(self)

      if pid
        # parent process
        $log.info "detached process", :class=>self.class, :pid=>pid
        children << [pid, forward_thread]
        next
      end

      # child process
      begin
        on_detach_process(i)

        block.call

        # disable Engine.stop called by signal handler
        Engine.define_singleton_method(:stop) do
          # do nothing
        end

        # override signal handlers called by parent process
        fin = FinishWait.new
        trap :INT do
          fin.stop
        end
        trap :TERM do
          fin.stop
        end
        #forward_thread.join  # TODO this thread won't stop because parent doesn't close pipe
        fin.wait

        exit! 0
      ensure
        $log.error "unknown error while shutting down this child process", :error=>$!.to_s, :pid=>Process.pid
        $log.error_backtrace
      end

      exit! 1
    end

    # parent process
    # override shutdown method to kill child processes
    define_singleton_method(:shutdown) do
      children.each {|pair|
        begin
          pid = pair[0]
          forward_thread = pair[1]
          if pid
            Process.kill(:TERM, pid)
            forward_thread.join   # wait until child closes pipe
            Process.waitpid(pid)
            pair[0] = nil
          end
        rescue
          $log.error "unknown error while shutting down remote child process", :error=>$!.to_s
          $log.error_backtrace
        end
      }
    end

    # override target.emit and write event stream to the pipe
    forwarders = children.map {|pair| pair[1].forwarder }
    if forwarders.length > 1
      # use roundrobin
      fwd = DetachProcessManager::MultiForwarder.new(forwarders)
    else
      fwd = forwarders[0]
    end
    define_singleton_method(:emit) do |tag,es,chain|
      chain.next
      fwd.emit(tag, es)
    end
  end

  class FinishWait
    def initialize
      @finished = false
      @mutex = Mutex.new
      @cond = ConditionVariable.new
    end

    def wait
      @mutex.synchronize do
        until @finished
          @cond.wait(@mutex, 1.0)
        end
      end
    end

    def stop
      return if @finished
      @finished = true
      @mutex.synchronize do
        @cond.broadcast
      end
    end

    def finished?
      @finished
    end
  end
end


module DetachProcessMixin
  include DetachProcessImpl

  def configure(conf)
    super

    if detach_process = conf['detach_process']
      b3v = Config.bool_value(detach_process)
      case b3v
      when nil
        num = detach_process.to_i
        if num > 1
          $log.warn "'detach_process' parameter supports only 1 process on this plugin: #{conf}"
        elsif num > 0
          @detach_process = true
        elsif detach_process =~ /0+/
          @detach_process = false
        else
          @detach_process = true
        end
      when true
        @detach_process = true
      when false
        @detach_process = false
      end
    end
  end

  def detach_process(&block)
    if @detach_process
      detach_process_impl(1, &block)
    else
      block.call
    end
  end
end


module DetachMultiProcessMixin
  include DetachProcessImpl

  def initialize
    @detach_process_num = 2
    super
  end

  def configure(conf)
    super

    if detach_process = conf['detach_process']
      b3v = Config.bool_value(detach_process)
      case b3v
      when nil
        num = detach_process.to_i
        if num > 0
          @detach_process = true
          @detach_process_num = num
        elsif detach_process =~ /0+/
          @detach_process = false
        else
          @detach_process = true
        end
      when true
        @detach_process = true
      when false
        @detach_process = false
      end
    end
  end

  protected
  def detach_multi_process(&block)
    if @detach_process
      detach_process_impl(@detach_process_num, &block)
    else
      block.call
    end
  end
end


end

