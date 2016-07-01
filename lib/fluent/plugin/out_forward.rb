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

require 'base64'
require 'socket'
require 'fileutils'

require 'cool.io'

require 'fluent/output'
require 'fluent/config/error'

module Fluent
  class ForwardOutputError < StandardError
  end

  class ForwardOutputResponseError < ForwardOutputError
  end

  class ForwardOutputConnectionClosedError < ForwardOutputError
  end

  class ForwardOutputACKTimeoutError < ForwardOutputResponseError
  end

  class ForwardOutput < ObjectBufferedOutput
    Plugin.register_output('forward', self)

    LISTEN_PORT = 24224

    def initialize
      super
      require 'fluent/plugin/socket_util'
      @nodes = []  #=> [Node]
      @loop = nil
      @thread = nil
      @finished = false
    end

    desc 'The timeout time while connecting'
    config_param :connection_hard_timeout, :time, default: nil # specifying 0 explicitly means not to disconnect stuck connection forever
    desc 'The timeout time when sending event logs.'
    config_param :send_timeout, :time, default: 60
    desc 'The transport protocol to use for heartbeats.(udp,tcp,none)'
    config_param :heartbeat_type, default: :tcp do |val|
      case val.downcase
      when 'tcp'
        :tcp
      when 'udp'
        :udp
      when 'none'
        :none
      else
        raise ConfigError, "forward output heartbeat type should be 'tcp', 'udp', or 'none'"
      end
    end
    desc 'The interval of the heartbeat packer.'
    config_param :heartbeat_interval, :time, default: 1
    desc 'The wait time before accepting a server fault recovery.'
    config_param :recover_wait, :time, default: 10
    desc 'The hard timeout used to detect server failure.'
    config_param :hard_timeout, :time, default: 60
    desc 'Set TTL to expire DNS cache in seconds.'
    config_param :expire_dns_cache, :time, default: nil  # 0 means disable cache
    desc 'The threshold parameter used to detect server faults.'
    config_param :phi_threshold, :integer, default: 16
    desc 'Use the "Phi accrual failure detector" to detect server failure.'
    config_param :phi_failure_detector, :bool, default: true

    # if any options added that requires extended forward api, fix @extend_internal_protocol

    desc 'Change the protocol to at-least-once.'
    config_param :require_ack_response, :bool, default: false  # require in_forward to respond with ack
    desc 'This option is used when require_ack_response is true.'
    config_param :ack_response_timeout, :time, default: 190  # 0 means do not wait for ack responses
    desc 'Reading data size from server'
    config_param :read_length, :size, default: 512 # 512bytes
    desc 'The interval while reading data from server'
    config_param :read_interval_msec, :integer, default: 50 # 50ms
    # Linux default tcp_syn_retries is 5 (in many environment)
    # 3 + 6 + 12 + 24 + 48 + 96 -> 189 (sec)
    desc 'Enable client-side DNS round robin.'
    config_param :dns_round_robin, :bool, default: false # heartbeat_type 'udp' is not available for this

    config_section :security, required: false, multi: false do
      desc 'The hostname'
      config_param :self_hostname, :string
      desc 'Shared key for authentication'
      config_param :shared_key, :string, secret: true
    end

    config_section :server, param_name: :servers do
      desc "The IP address or host name of the server."
      config_param :host, :string
      desc "The name of the server. Used in log messages."
      config_param :name, :string, default: nil
      desc "The port number of the host."
      config_param :port, :integer, default: LISTEN_PORT
      desc "The shared key per server."
      config_param :shared_key, :string, default: nil, secret: true
      desc "The username for authentication."
      config_param :username, :string, default: ''
      desc "The password for authentication."
      config_param :password, :string, default: '', secret: true
      desc "Marks a node as the standby node for an Active-Standby model between Fluentd nodes."
      config_param :standby, :bool, default: false
      desc "The load balancing weight."
      config_param :weight, :integer, default: 60
    end
    attr_reader :nodes

    # backward compatibility
    config_param :port, :integer, default: LISTEN_PORT
    config_param :host, :string, default: nil

    attr_accessor :extend_internal_protocol
    attr_reader :read_interval

    def configure(conf)
      super

      # backward compatibility
      if host = conf['host']
        log.warn "'host' option in forward output is obsoleted. Use '<server> host xxx </server>' instead."
        port = conf['port']
        port = port ? port.to_i : LISTEN_PORT
        s = Fluent::Config::Section.new(
          host: host, port: port, name: "#{host}:#{port}", shared_key: nil,
          weight: 60, username: "", password: "", standby: false)
        @servers << s
      end

      @read_interval = @read_interval_msec / 1000.0
      recover_sample_size = @recover_wait / @heartbeat_interval

      # add options here if any options addes which uses extended protocol
      @extend_internal_protocol = if @require_ack_response
                                    true
                                  else
                                    false
                                  end

      if @dns_round_robin
        if @heartbeat_type == :udp
          raise ConfigError, "forward output heartbeat type must be 'tcp' or 'none' to use dns_round_robin option"
        end
      end

      @servers.each do |server|
        failure = FailureDetector.new(@heartbeat_interval, @hard_timeout, Time.now.to_i.to_f)

        name = server.name || "#{server.host}:#{server.port}"

        if @heartbeat_type == :none
          @nodes << NoneHeartbeatNode.new(self, server, failure: failure, recover_sample_size: recover_sample_size)
        else
          @nodes << Node.new(self, server, failure: failure, recover_sample_size: recover_sample_size)
        end
        log.info("adding forwarding server '#{name}'",
                 host: server.host, port: server.port, weight: server.weight, plugin_id: plugin_id)
      end

      if @nodes.empty?
        raise ConfigError, "forward output plugin requires at least one <server> is required"
      end
    end

    def start
      super

      @rand_seed = Random.new.seed
      rebuild_weight_array
      @rr = 0

      unless @heartbeat_type == :none
        @loop = Coolio::Loop.new

        if @heartbeat_type == :udp
          # assuming all hosts use udp
          @usock = SocketUtil.create_udp_socket(@nodes.first.host)
          @usock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
          @hb = HeartbeatHandler.new(@usock, method(:on_heartbeat))
          @loop.attach(@hb)
        end

        @timer = HeartbeatRequestTimer.new(@heartbeat_interval, method(:on_timer))
        @loop.attach(@timer)

        @thread = Thread.new(&method(:run))
      end
    end

    def shutdown
      @finished = true
      if @loop
        @loop.watchers.each {|w| w.detach }
        @loop.stop
      end
      @thread.join if @thread
      @usock.close if @usock

      super
    end

    def run
      @loop.run if @loop
    rescue
      log.error "unexpected error", error: $!.to_s
      log.error_backtrace
    end

    def write_objects(tag, chunk)
      return if chunk.empty?

      error = nil

      wlen = @weight_array.length
      wlen.times do
        @rr = (@rr + 1) % wlen
        node = @weight_array[@rr]

        if node.available?
          begin
            node.send_data(tag, chunk)
            return
          rescue
            # for load balancing during detecting crashed servers
            error = $!  # use the latest error
          end
        end
      end

      if error
        raise error
      else
        raise "no nodes are available"  # TODO message
      end
    end

    # MessagePack FixArray length = 3 (if @extend_internal_protocol)
    #                             = 2 (else)
    FORWARD_HEADER = [0x92].pack('C').freeze
    FORWARD_HEADER_EXT = [0x93].pack('C').freeze
    def forward_header
      if @extend_internal_protocol
        FORWARD_HEADER_EXT
      else
        FORWARD_HEADER
      end
    end

    private

    def rebuild_weight_array
      standby_nodes, regular_nodes = @nodes.partition {|n|
        n.standby?
      }

      lost_weight = 0
      regular_nodes.each {|n|
        unless n.available?
          lost_weight += n.weight
        end
      }
      log.debug "rebuilding weight array", lost_weight: lost_weight

      if lost_weight > 0
        standby_nodes.each {|n|
          if n.available?
            regular_nodes << n
            log.warn "using standby node #{n.host}:#{n.port}", weight: n.weight
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

      # for load balancing during detecting crashed servers
      coe = (regular_nodes.size * 6) / weight_array.size
      weight_array *= coe if coe > 1

      r = Random.new(@rand_seed)
      weight_array.sort_by! { r.rand }

      @weight_array = weight_array
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
      @nodes.each {|n|
        if n.tick
          rebuild_weight_array
        end
        begin
          #log.trace "sending heartbeat #{n.host}:#{n.port} on #{@heartbeat_type}"
          n.usock = @usock
          n.send_heartbeat
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR, Errno::ECONNREFUSED
          # TODO log
          log.debug "failed to send heartbeat packet to #{n.host}:#{n.port}", error: $!.to_s
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
        begin
          msg, addr = @io.recvfrom(1024)
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          return
        end
        host = addr[3]
        port = addr[1]
        sockaddr = Socket.pack_sockaddr_in(port, host)
        @callback.call(sockaddr, msg)
      rescue
        # TODO log?
      end
    end

    def on_heartbeat(sockaddr, msg)
      if node = @nodes.find {|n| n.sockaddr == sockaddr }
        #log.trace "heartbeat from '#{node.name}'", :host=>node.host, :port=>node.port
        if node.heartbeat
          rebuild_weight_array
        end
      end
    end

    class Node
      def initialize(sender, conf, failure:, recover_sample_size:)
        @sender = sender
        @log = sender.log

        @conf = conf
        @name = @conf.name || "#{@conf.host}:#{@conf.port}"
        @host = @conf.host
        @port = @conf.port
        @weight = @conf.weight
        @failure = failure
        @recover_sample_size = recover_sample_size
        @available = true
        @first_session = true
        @state = nil

        @usock = nil

        @username = @conf.username
        @password = @conf.password
        @shared_key = @conf.shared_key || (sender.security && sender.security.shared_key) || ""
        @shared_key_salt = generate_salt
        @shared_key_nonce = ""
        @unpacker = MessagePack::Unpacker.new

        @resolved_host = nil
        @resolved_time = 0
        resolved_host  # check dns
      end

      attr_reader :conf
      attr_reader :name, :host, :port, :weight
      attr_reader :sockaddr  # used by on_heartbeat
      attr_reader :failure, :available # for test
      attr_accessor :usock

      def available?
        @available
      end

      def disable!
        @available = false
      end

      def standby?
        @conf.standby
      end

      def connect
        # TODO unix socket?
        TCPSocket.new(resolved_host, port)
      end

      def send_data(tag, chunk)
        sock = connect
        @state = @sender.security ? :helo : :established
        begin
          opt = [1, @sender.send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

          opt = [@sender.send_timeout.to_i, 0].pack('L!L!')  # struct timeval
          sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

          loop do
            break if @sender.connection_hard_timeout && Time.now > @mtime + @sender.connection_hard_timeout
            begin
              while buf = sock.read_nonblock(@sender.read_length)
                if buf.empty?
                  sleep @sender.read_interval
                  next
                end
                @unpacker.feed_each(buf) do |data|
                  on_read(sock, data)
                end
                @mtime = Time.now
              end
            rescue Errno::EAGAIN # TODO replace IO::WaitReadable if we drop Ruby2.0 support
              break if @state == :established
            rescue SystemCallError => e
              @log.warn "disconnected by error", error_class: e.class, error: e, host: @host, port: @port
              disable!
              break
            rescue EOFError
              @log.warn "disconnected", host: @host, port: @port
              disable!
              break
            end
          end

          # beginArray(2)
          sock.write @sender.forward_header

          # writeRaw(tag)
          sock.write tag.to_msgpack  # tag

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

          if @sender.extend_internal_protocol
            option = {}
            option['chunk'] = Base64.encode64(chunk.unique_id) if @sender.require_ack_response
            sock.write option.to_msgpack

            if @sender.require_ack_response && @sender.ack_response_timeout > 0
              # Waiting for a response here results in a decrease of throughput because a chunk queue is locked.
              # To avoid a decrease of troughput, it is necessary to prepare a list of chunks that wait for responses
              # and process them asynchronously.
              if IO.select([sock], nil, nil, @sender.ack_response_timeout)
                begin
                  raw_data = sock.recv(1024)
                rescue Errno::ECONNRESET
                  raw_data = ""
                end

                # When connection is closed by remote host, socket is ready to read and #recv returns an empty string that means EOF.
                # If this happens we assume the data wasn't delivered and retry it.
                if raw_data.empty?
                  @log.warn "node #{host}:#{port} closed the connection. regard it as unavailable."
                  disable!
                  raise ForwardOutputConnectionClosedError, "node #{host}:#{port} closed connection"
                else
                  # Serialization type of the response is same as sent data.
                  res = MessagePack.unpack(raw_data)

                  if res['ack'] != option['chunk']
                    # Some errors may have occured when ack and chunk id is different, so send the chunk again.
                    raise ForwardOutputResponseError, "ack in response and chunk id in sent data are different"
                  end
                end

              else
                # IO.select returns nil on timeout.
                # There are 2 types of cases when no response has been received:
                # (1) the node does not support sending responses
                # (2) the node does support sending response but responses have not arrived for some reasons.
                @log.warn "no response from #{host}:#{port}. regard it as unavailable."
                disable!
                raise ForwardOutputACKTimeoutError, "node #{host}:#{port} does not return ACK"
              end
            end
          end

          heartbeat(false)
          return res  # for test
        ensure
          sock.close_write
          sock.close
        end
      end

      #FORWARD_TCP_HEARTBEAT_DATA = FORWARD_HEADER + ''.to_msgpack + [].to_msgpack
      def send_heartbeat
        if @sender.heartbeat_type == :tcp
          sock = connect
          begin
            opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
            sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
            # don't send any data to not cause a compatibility problem
            #opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
            #sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)
            #sock.write FORWARD_TCP_HEARTBEAT_DATA
            heartbeat(true)
          ensure
            sock.close_write
            sock.close
          end
        else
          @usock.send "\0", 0, Socket.pack_sockaddr_in(n.port, n.resolved_host)
        end
      end

      def resolved_host
        case @sender.expire_dns_cache
        when 0
          # cache is disabled
          return resolve_dns!

        when nil
          # persistent cache
          return @resolved_host ||= resolve_dns!

        else
          now = Engine.now
          rh = @resolved_host
          if !rh || now - @resolved_time >= @sender.expire_dns_cache
            rh = @resolved_host = resolve_dns!
            @resolved_time = now
          end
          return rh
        end
      end

      def resolve_dns!
        addrinfo_list = Socket.getaddrinfo(@host, @port, nil, Socket::SOCK_STREAM)
        addrinfo = @sender.dns_round_robin ? addrinfo_list.sample : addrinfo_list.first
        @sockaddr = Socket.pack_sockaddr_in(addrinfo[1], addrinfo[3]) # used by on_heartbeat
        addrinfo[3]
      end
      private :resolve_dns!

      def tick
        now = Time.now.to_f
        if !@available
          if @failure.hard_timeout?(now)
            @failure.clear
          end
          return nil
        end

        if @failure.hard_timeout?(now)
          @log.warn "detached forwarding server '#{@name}'", host: @host, port: @port, hard_timeout: true
          @available = false
          @resolved_host = nil  # expire cached host
          @failure.clear
          return true
        end

        if @sender.phi_failure_detector
          phi = @failure.phi(now)
          #$log.trace "phi '#{@name}'", :host=>@host, :port=>@port, :phi=>phi
          if phi > @sender.phi_threshold
            @log.warn "detached forwarding server '#{@name}'", host: @host, port: @port, phi: phi
            @available = false
            @resolved_host = nil  # expire cached host
            @failure.clear
            return true
          end
        end
        return false
      end

      def heartbeat(detect=true)
        now = Time.now.to_f
        @failure.add(now)
        #@log.trace "heartbeat from '#{@name}'", :host=>@host, :port=>@port, :available=>@available, :sample_size=>@failure.sample_size
        if detect && !@available && @failure.sample_size > @recover_sample_size
          @available = true
          @log.warn "recovered forwarding server '#{@name}'", host: @host, port: @port
          return true
        else
          return nil
        end
      end

      def to_msgpack(out = '')
        [@host, @port, @weight, @available].to_msgpack(out)
      end

      def generate_salt
        SecureRandom.hex(16)
      end

      def check_helo(message)
        @log.debug "checking helo"
        # ['HELO', options(hash)]
        unless message.size == 2 && message[0] == 'HELO'
          return false
        end
        opts = message[1]
        @shared_key_nonce = opts['nonce'] || '' # make shared_key_check failed (instead of error) if protocol version mismatch exist
        @authentication = opts['auth'] || ''
        @mtime = Time.now
        true
      end

      def generate_ping
        @log.debug "generating ping"
        # ['PING', self_hostname, sharedkey\_salt, sha512\_hex(sharedkey\_salt + self_hostname + nonce + shared_key),
        #  username || '', sha512\_hex(auth\_salt + username + password) || '']
        shared_key_hexdigest = Digest::SHA512.new.update(@shared_key_salt).update(@sender.security.self_hostname).update(@shared_key_nonce).update(@shared_key).hexdigest
        ping = ['PING', @sender.security.self_hostname, @shared_key_salt, shared_key_hexdigest]
        if !@authentication.empty?
          password_hexdigest = Digest::SHA512.new.update(@authentication).update(@username).update(@password).hexdigest
          ping.push(@username, password_hexdigest)
        else
          ping.push('','')
        end
        @mtime = Time.now
        ping
      end

      def check_pong(message)
        @log.debug "checking pong"
        # ['PONG', bool(authentication result), 'reason if authentication failed',
        #  self_hostname, sha512\_hex(salt + self_hostname + nonce + sharedkey)]
        unless message.size == 5 && message[0] == 'PONG'
          return false, 'invalid format for PONG message'
        end
        _pong, auth_result, reason, hostname, shared_key_hexdigest = message

        unless auth_result
          return false, 'authentication failed: ' + reason
        end

        if hostname == @sender.security.self_hostname
          return false, 'same hostname between input and output: invalid configuration'
        end

        clientside = Digest::SHA512.new.update(@shared_key_salt).update(hostname).update(@shared_key_nonce).update(@shared_key).hexdigest
        unless shared_key_hexdigest == clientside
          return false, 'shared key mismatch'
        end

        @mtime = Time.now
        return true, nil
      end

      def on_read(sock, data)
        @log.debug __callee__

        case @state
        when :helo
          unless check_helo(data)
            @log.warn "received invalid helo message from #{@name}"
            disable! # shutdown
            return
          end
          sock.write(generate_ping.to_msgpack)
          @mtime = Time.now
          @state = :pingpong
        when :pingpong
          succeeded, reason = check_pong(data)
          unless succeeded
            @log.warn "connection refused to #{@name}: #{reason}"
            disable! # shutdown
            return
          end
          @log.info "connection established to #{@name}" if @first_session
          @first_session = false
          @state = :established
          @mtime = Time.now
          @log.debug "connection established", host: @host, port: @port
        else
          raise "BUG: unknown session state: #{@state}"
        end
      end
    end

    # Override Node to disable heartbeat
    class NoneHeartbeatNode < Node
      def available?
        true
      end

      def tick
        false
      end

      def heartbeat(detect=true)
        true
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
        t = now - @last - @heartbeat_interval + 1
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
