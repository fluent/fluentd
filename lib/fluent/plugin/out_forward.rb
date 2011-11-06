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


class ForwardOutput < BufferedOutput
  Plugin.register_output('forward', self)

  def initialize
    super
    require 'socket'
    require 'fileutils'
    @nodes = {}  #=> {sockaddr => Node}
  end

  config_param :send_timeout, :time, :default => 60
  config_param :heartbeat_interval, :time, :default => 1
  config_param :recover_wait, :time, :default => 10
  config_param :hard_timeout, :time, :default => nil
  config_param :phi_threshold, :integer, :default => 8
  attr_reader :nodes

  def configure(conf)
    super

    @hard_timeout ||= @send_timeout

    recover_sample_size = @recover_wait / @heartbeat_interval

    conf.elements.each {|e|
      next if e.name != "server"

      host = e['host']
      port = e['port']
      port = port ? port.to_i : 24224

      weight = e['weight']
      weight = weight ? weight.to_i : 60

      name = e['name']
      unless name
        name = "#{host}:#{port}"
      end

      failure = FailureDetector.new(@heartbeat_interval.to_f/2, @hard_timeout)
      sockaddr = Socket.pack_sockaddr_in(port, host)
      port, host = Socket.unpack_sockaddr_in(sockaddr)
      @nodes[sockaddr] = Node.new(name, host, port, weight, failure,
                                  @phi_threshold, recover_sample_size)
      $log.info "adding forwarding server '#{name}'", :host=>host, :port=>port, :weight=>weight
    }
  end

  def start
    super

    @weight_array = []
    gcd = @nodes.values.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
    @nodes.each_value {|n|
      (n.weight / gcd).times {
        @weight_array << n
      }
    }
    @weight_array.sort_by { rand }

    @rr = 0

    @loop = Coolio::Loop.new

    @usock = UDPSocket.new
    @hb = HeartbeatHandler.new(@usock, method(:on_heartbeat))
    @loop.attach(@hb)

    @timer = HeartbeatRequestTimer.new(@heartbeat_interval, method(:on_timer))
    @loop.attach(@timer)

    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @finished = true
    @usock.close
    @loop.stop
    @thread.join
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  # override BufferedOutput#emit
  def emit(tag, es, chain)
    data = es.to_msgpack_stream
    if @buffer.emit(tag, data, chain)  # use key = tag
      submit_flush
    end
  end

  def write(chunk)
    wlen = @weight_array.length
    wlen.times do
      node = @weight_array[@rr]
      @rr = (@rr + 1) % wlen

      if node.available?
        send_data(node, chunk)
        return
      end
    end

    raise "no nodes are available"  # TODO message
  end

  private
  # MessagePack FixArray length = 2
  FORWARD_HEADER = [0x92].pack('C')

  def send_data(node, chunk)
    sock = connect(node)
    begin
      opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
      sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

      opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
      sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

      # beginArray(2)
      sock.write FORWARD_HEADER

      # writeRaw(tag)
      sock.write chunk.key.to_msgpack  # tag

      # beginRaw(size)
      sz = chunk.size
      #if sz < 32
      #  # FixRaw
      #  sock.write [0xa0 | sz].pack('C')
      #elsif sz < 65536
      #  # raw 16
      #  sock.write [0xda, sz].pack('Cn')
      #else
        # raw 32
        sock.write [0xdb, sz].pack('CN')
      #end

      # writeRawBody(packed_es)
      chunk.write_to(sock)
    ensure
      sock.close
    end
  end

  def connect(node)
    # TODO unix socket?
    TCPSocket.new(node.host, node.port)
  end

  class HeartbeatRequestTimer < Coolio::TimerWatcher
    def initialize(interval, callback)
      super(interval, true)
      @callback = callback
    end

    def on_timer
      @callback.call
    end
  end

  def on_timer
    return if @finished
    @nodes.each_pair {|sockaddr,n|
      n.tick
      @usock.send "", 0, sockaddr
    }
  end

  class HeartbeatHandler < Coolio::IO
    def initialize(io, callback)
      super(io)
      @io = io
      @callback = callback
    end

    def on_readable
      msg, addr = @io.recvfrom(1024)
      host = addr[3]
      port = addr[1]
      sockaddr = Socket.pack_sockaddr_in(port, host)
      @callback.call(sockaddr, msg)
    end
  end

  def on_heartbeat(sockaddr, msg)
    if node = @nodes[sockaddr]
      $log.trace "heartbeat from '#{node.name}'", :host=>node.host, :port=>node.port
      node.heartbeat
    end
  end

  class Node
    def initialize(name, host, port, weight, failure,
                   phi_threshold, recover_sample_size)
      @name = name
      @host = host
      @port = port
      @weight = weight
      @failure = failure
      @phi_threshold = phi_threshold
      @recover_sample_size = recover_sample_size
      @available = true
    end

    attr_reader :name, :host, :port
    attr_reader :weight
    attr_writer :available

    def available?
      @available
    end

    def tick
      now = Time.now.to_f
      phi = @failure.phi(now)
      $log.trace "phi '#{@name}'", :host=>@host, :port=>@port, :phi=>phi
      if phi > @phi_threshold
        $log.info "detached forwarding server '#{@name}'", :host=>@host, :port=>@port, :phi=>phi
        @available = false
        @failure.clear
      end
    end

    def heartbeat
      now = Time.now.to_f
      @failure.add(now)
      if !@available && @failure.sample_size > @recover_sample_size &&
            @failure.phi(now) <= @phi_threshold
        $log.info "recovered forwarding server '#{@name}'", :host=>@host, :port=>@port
        @available = true
      end
    end

    def to_msgpack(out = '')
      [@host, @port, @weight, @available].to_msgpack(out)
    end
  end

  class FailureDetector
    PHI_FACTOR = 1.0 / Math.log(10.0)
    SAMPLE_SIZE = 1000

    def initialize(init_int, hard_timeout)
      @last = 0
      @init_int = init_int
      @hard_timeout = hard_timeout
      @window = []
      @failure = false
    end

    def add(now)
      if @last > 0
        int = now - @last
      else
        int = @init_int
      end
      if int <= @hard_timeout
        @window << int
        @window.shift if @window.length > SAMPLE_SIZE
        @last = now
        true
      else
        false
      end
    end

    def phi(now)
      size = @window.size
      return 0.0 if size == 0
      t = now - @last
      mean = @window.inject(0) {|r,v| r + v } / size
      return PHI_FACTOR * t / mean
    end

    def sample_size
      @window.size
    end

    def clear
      @window.clear
      @last = 0
    end
  end

  ## TODO
  #class RPC
  #  def initialize(this)
  #    @this = this
  #  end
  #
  #  def list_nodes
  #    @this.nodes
  #  end
  #
  #  def list_fault_nodes
  #    list_nodes.select {|n| !n.available? }
  #  end
  #
  #  def list_available_nodes
  #    list_nodes.select {|n| n.available? }
  #  end
  #
  #  def add_node(name, host, port, weight)
  #  end
  #
  #  def recover_node(host, port)
  #  end
  #
  #  def remove_node(host, port)
  #  end
  #end
end


end

