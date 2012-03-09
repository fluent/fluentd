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


class ForwardOutput < ObjectBufferedOutput
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
  config_param :hard_timeout, :time, :default => 60
  config_param :phi_threshold, :integer, :default => 8
  attr_reader :nodes

  # backward compatibility
  config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
  config_param :host, :string, :default => nil

  def configure(conf)
    super

    # backward compatibility
    if host = conf['host']
      $log.warn "'host' option in forward output is obsoleted. Use '<server> host xxx </server>' instead."
      port = conf['port']
      port = port ? port.to_i : DEFAULT_LISTEN_PORT
      e = conf.add_element('server')
      e['host'] = host
      e['port'] = port.to_s
    end

    recover_sample_size = @recover_wait / @heartbeat_interval

    conf.elements.each {|e|
      next if e.name != "server"

      host = e['host']
      port = e['port']
      port = port ? port.to_i : DEFAULT_LISTEN_PORT

      weight = e['weight']
      weight = weight ? weight.to_i : 60

      standby = !!e['standby']

      name = e['name']
      unless name
        name = "#{host}:#{port}"
      end

      failure = FailureDetector.new(@heartbeat_interval, @hard_timeout, Time.now.to_i.to_f)
      sockaddr = Socket.pack_sockaddr_in(port, host)
      port, host = Socket.unpack_sockaddr_in(sockaddr)
      @nodes[sockaddr] = Node.new(name, host, port, weight, standby, failure,
                                  @phi_threshold, recover_sample_size)
      $log.info "adding forwarding server '#{name}'", :host=>host, :port=>port, :weight=>weight
    }
  end

  def start
    super

    @rand_seed = Random.new.seed
    rebuild_weight_array
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
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @thread.join
    @usock.close
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

  def write_objects(tag, es)
    wlen = @weight_array.length
    wlen.times do
      node = @weight_array[@rr]
      @rr = (@rr + 1) % wlen

      if node.available?
        send_data(node, tag, es)
        return
      end
    end

    raise "no nodes are available"  # TODO message
  end

  private
  def rebuild_weight_array
    standby_nodes, regular_nodes = @nodes.values.partition {|n|
      n.standby?
    }

    lost_weight = 0
    regular_nodes.each {|n|
      unless n.available?
        lost_weight += n.weight
      end
    }
    $log.debug "rebuilding weight array", :lost_weight=>lost_weight

    if lost_weight > 0
      standby_nodes.each {|n|
        if n.available?
          regular_nodes << n
          $log.info "using standby node #{n.host}:#{n.port}", :weight=>n.weight
          lost_weight -= n.weight
          break if lost_weight <= 0
        end
      }
    end

    weight_array = []
    gcd = regular_nodes.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
    regular_nodes.each {|n|
      (n.weight / gcd).times {
        weight_array << n
      }
    }

    r = Random.new(@rand_seed)
    weight_array.sort_by! { r.rand }

    @weight_array = weight_array
  end

  # MessagePack FixArray length = 2
  FORWARD_HEADER = [0x92].pack('C')

  def send_data(node, tag, es)
    sock = connect(node)
    begin
      opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
      sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

      opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
      sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

      # beginArray(2)
      sock.write FORWARD_HEADER

      # writeRaw(tag)
      sock.write tag.to_msgpack  # tag

      # beginRaw(size)
      sz = es.size
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
      es.write_to(sock)
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
    rescue
      # TODO log?
    end
  end

  def on_timer
    return if @finished
    @nodes.each_pair {|sockaddr,n|
      if n.tick
        rebuild_weight_array
      end
      begin
        #$log.trace "sending heartbeat #{n.host}:#{n.port}"
        @usock.send "", 0, sockaddr
      rescue
        # TODO log
        $log.debug "failed to send heartbeat packet to #{Socket.unpack_sockaddr_in(sockaddr).reverse.join(':')}", :error=>$!.to_s
      end
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
    rescue
      # TODO log?
    end
  end

  def on_heartbeat(sockaddr, msg)
    if node = @nodes[sockaddr]
      #$log.trace "heartbeat from '#{node.name}'", :host=>node.host, :port=>node.port
      if node.heartbeat
        rebuild_weight_array
      end
    end
  end

  class Node
    def initialize(name, host, port, weight, standby, failure,
                   phi_threshold, recover_sample_size)
      @name = name
      @host = host
      @port = port
      @weight = weight
      @standby = standby
      @failure = failure
      @phi_threshold = phi_threshold
      @recover_sample_size = recover_sample_size
      @available = true
    end

    attr_reader :name, :host, :port, :weight
    attr_writer :weight, :standby, :available

    def available?
      @available
    end

    def standby?
      @standby
    end

    def tick
      now = Time.now.to_f
      if !@available
        if @failure.hard_timeout?(now)
          @failure.clear
        end
        return nil
      end

      if @failure.hard_timeout?(now)
        $log.info "detached forwarding server '#{@name}'", :host=>@host, :port=>@port, :hard_timeout=>true
        @available = false
        @failure.clear
        return true
      end

      phi = @failure.phi(now)
      #$log.trace "phi '#{@name}'", :host=>@host, :port=>@port, :phi=>phi
      if phi > @phi_threshold
        $log.info "detached forwarding server '#{@name}'", :host=>@host, :port=>@port, :phi=>phi
        @available = false
        @failure.clear
        return true
      else
        return false
      end
    end

    def heartbeat
      now = Time.now.to_f
      @failure.add(now)
      #$log.trace "heartbeat from '#{@name}'", :host=>@host, :port=>@port, :available=>@available, :sample_size=>@failure.sample_size
      if !@available && @failure.sample_size > @recover_sample_size
        $log.info "recovered forwarding server '#{@name}'", :host=>@host, :port=>@port
        @available = true
        return true
      else
        return nil
      end
    end

    def to_msgpack(out = '')
      [@host, @port, @weight, @available].to_msgpack(out)
    end
  end

  class FailureDetector
    PHI_FACTOR = 1.0 / Math.log(10.0)
    SAMPLE_SIZE = 1000

    def initialize(heartbeat_interval, hard_timeout, init_last)
      @heartbeat_interval = heartbeat_interval
      @last = init_last
      @hard_timeout = hard_timeout

      # microsec
      @init_gap = (heartbeat_interval * 1e6).to_i
      @window = [@init_gap]
    end

    def hard_timeout?(now)
      now - @last > @hard_timeout
    end

    def add(now)
      if @window.empty?
        @window << @init_gap
        @last = now
      else
        gap = now - @last
        @window << (gap * 1e6).to_i
        @window.shift if @window.length > SAMPLE_SIZE
        @last = now
      end
    end

    def phi(now)
      size = @window.size
      return 0.0 if size == 0

      # Calculate weighted moving average
      mean_usec = 0
      fact = 0
      @window.each_with_index {|gap,i|
        mean_usec += gap * (1+i)
        fact += (1+i)
      }
      mean_usec = mean_usec / fact

      # Normalize arrive intervals into 1sec
      mean = (mean_usec.to_f / 1e6) - @heartbeat_interval + 1

      # Calculate phi of the phi accrual failure detector
      t = now - @last
      phi = PHI_FACTOR * t / mean

      return phi
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

