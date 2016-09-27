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

    desc 'The timeout time when sending event logs.'
    config_param :send_timeout, :time, default: 60
    desc 'The transport protocol to use for heartbeats.(udp,tcp,none)'
    config_param :heartbeat_type, :enum, list: [:tcp, :udp, :none], default: :tcp
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

    desc 'Change the protocol to at-least-once.'
    config_param :require_ack_response, :bool, default: false  # require in_forward to respond with ack
    desc 'This option is used when require_ack_response is true.'
    config_param :ack_response_timeout, :time, default: 190
    desc 'Reading data size from server'
    config_param :read_length, :size, default: 512 # 512bytes
    desc 'The interval while reading data from server'
    config_param :read_interval_msec, :integer, default: 50 # 50ms
    # Linux default tcp_syn_retries is 5 (in many environment)
    # 3 + 6 + 12 + 24 + 48 + 96 -> 189 (sec)
    desc 'Enable client-side DNS round robin.'
    config_param :dns_round_robin, :bool, default: false # heartbeat_type 'udp' is not available for this

    desc 'Compress buffered data.'
    config_param :compress, :enum, list: [:text, :gzip], default: :text

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

    config_param :port, :integer, default: LISTEN_PORT, obsoleted: "User <server> section instead."
    config_param :host, :string, default: nil, obsoleted: "Use <server> section instead."

    attr_reader :read_interval, :recover_sample_size

    def configure(conf)
      super

      @read_interval = @read_interval_msec / 1000.0
      @recover_sample_size = @recover_wait / @heartbeat_interval

      if @dns_round_robin
        if @heartbeat_type == :udp
          raise ConfigError, "forward output heartbeat type must be 'tcp' or 'none' to use dns_round_robin option"
        end
      end

      @servers.each do |server|
        failure = FailureDetector.new(@heartbeat_interval, @hard_timeout, Time.now.to_i.to_f)
        name = server.name || "#{server.host}:#{server.port}"

        log.info "adding forwarding server '#{name}'", host: server.host, port: server.port, weight: server.weight, plugin_id: plugin_id
        if @heartbeat_type == :none
          @nodes << NoneHeartbeatNode.new(self, server, failure: failure)
        else
          @nodes << Node.new(self, server, failure: failure)
        end
      end

      if @compress == :gzip && @buffer.compress == :text
        @buffer.compress = :gzip
      elsif @compress == :text && @buffer.compress == :gzip
        log.info "buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>"
      end

      if @nodes.empty?
        raise ConfigError, "forward output plugin requires at least one <server> is required"
      end

      raise Fluent::ConfigError, "ack_response_timeout must be a positive integer" if @ack_response_timeout < 1
    end

    def start
      super

      @rand_seed = Random.new.seed
      rebuild_weight_array
      @rr = 0
      @usock = nil

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

    # MessagePack FixArray length is 3
    FORWARD_HEADER = [0x93].pack('C').freeze
    def forward_header
      FORWARD_HEADER
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
          log.trace "sending heartbeat", host: n.host, port: n.port, heartbeat_type: @heartbeat_type
          n.usock = @usock if @usock
          n.send_heartbeat
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR, Errno::ECONNREFUSED
          log.debug "failed to send heartbeat packet", host: n.host, port: n.port, heartbeat_type: @heartbeat_type, error: $!
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
      def initialize(sender, server, failure:)
        @sender = sender
        @log = sender.log
        @compress = sender.compress

        @name = server.name
        @host = server.host
        @port = server.port
        @weight = server.weight
        @standby = server.standby
        @failure = failure
        @available = true
        @state = nil

        @usock = nil

        @username = server.username
        @password = server.password
        @shared_key = server.shared_key || (sender.security && sender.security.shared_key) || ""
        @shared_key_salt = generate_salt
        @shared_key_nonce = ""

        @unpacker = Fluent::Engine.msgpack_unpacker

        @resolved_host = nil
        @resolved_time = 0
        resolved_host  # check dns
      end

      attr_accessor :usock

      attr_reader :name, :host, :port, :weight, :standby, :state
      attr_reader :sockaddr  # used by on_heartbeat
      attr_reader :failure, :available # for test

      def available?
        @available
      end

      def disable!
        @available = false
      end

      def standby?
        @standby
      end

      def connect
        TCPSocket.new(resolved_host, port)
      end

      def set_socket_options(sock)
        opt = [1, @sender.send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
        sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

        opt = [@sender.send_timeout.to_i, 0].pack('L!L!')  # struct timeval
        sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

        sock
      end

      def establish_connection(sock)
        while available? && @state != :established
          begin
            # TODO: On Ruby 2.2 or earlier, read_nonblock doesn't work expectedly.
            # We need rewrite around here using new socket/server plugin helper.
            buf = sock.read_nonblock(@sender.read_length)
            if buf.empty?
              sleep @sender.read_interval
              next
            end
            @unpacker.feed_each(buf) do |data|
              on_read(sock, data)
            end
          rescue IO::WaitReadable
            # If the exception is Errno::EWOULDBLOCK or Errno::EAGAIN, it is extended by IO::WaitReadable.
            # So IO::WaitReadable can be used to rescue the exceptions for retrying read_nonblock.
            # http://docs.ruby-lang.org/en/2.3.0/IO.html#method-i-read_nonblock
            sleep @sender.read_interval unless @state == :established
          rescue SystemCallError => e
            @log.warn "disconnected by error", host: @host, port: @port, error: e
            disable!
            break
          rescue EOFError
            @log.warn "disconnected", host: @host, port: @port
            disable!
            break
          end
        end
      end

      def send_data(tag, chunk)
        sock = connect
        @state = @sender.security ? :helo : :established
        begin
          set_socket_options(sock)

          if @state != :established
            establish_connection(sock)
          end

          unless available?
            raise ForwardOutputConnectionClosedError, "failed to establish connection with node #{@name}"
          end

          option = { 'size' => chunk.size_of_events, 'compressed' => @compress }
          option['chunk'] = Base64.encode64(chunk.unique_id) if @sender.require_ack_response

          # out_forward always uses Raw32 type for content.
          # Raw16 can store only 64kbytes, and it should be much smaller than buffer chunk size.

          sock.write @sender.forward_header        # beginArray(3)
          sock.write tag.to_msgpack                # 1. writeRaw(tag)
          chunk.open(compressed: @compress) do |chunk_io|
            sock.write [0xdb, chunk_io.size].pack('CN') # 2. beginRaw(size) raw32
            IO.copy_stream(chunk_io, sock)              # writeRawBody(packed_es)
          end
          sock.write option.to_msgpack             # 3. writeOption(option)

          if @sender.require_ack_response
            # Waiting for a response here results in a decrease of throughput because a chunk queue is locked.
            # To avoid a decrease of throughput, it is necessary to prepare a list of chunks that wait for responses
            # and process them asynchronously.
            if IO.select([sock], nil, nil, @sender.ack_response_timeout)
              raw_data = begin
                           sock.recv(1024)
                         rescue Errno::ECONNRESET
                           ""
                         end

              # When connection is closed by remote host, socket is ready to read and #recv returns an empty string that means EOF.
              # If this happens we assume the data wasn't delivered and retry it.
              if raw_data.empty?
                @log.warn "node closed the connection. regard it as unavailable.", host: @host, port: @port
                disable!
                raise ForwardOutputConnectionClosedError, "node #{@host}:#{@port} closed connection"
              else
                @unpacker.feed(raw_data)
                res = @unpacker.read
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
              @log.warn "no response from node. regard it as unavailable.", host: @host, port: @port
              disable!
              raise ForwardOutputACKTimeoutError, "node #{host}:#{port} does not return ACK"
            end
          end

          heartbeat(false)
          res  # for test
        ensure
          sock.close_write
          sock.close
        end
      end

      # FORWARD_TCP_HEARTBEAT_DATA = FORWARD_HEADER + ''.to_msgpack + [].to_msgpack
      def send_heartbeat
        case @sender.heartbeat_type
        when :tcp
          sock = connect
          begin
            opt = [1, @sender.send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
            sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
            # opt = [@sender.send_timeout.to_i, 0].pack('L!L!')  # struct timeval
            # sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

            ## don't send any data to not cause a compatibility problem
            # sock.write FORWARD_TCP_HEARTBEAT_DATA

            # successful tcp connection establishment is considered as valid heartbeat
            heartbeat(true)
          ensure
            sock.close_write
            sock.close
          end
        when :udp
          @usock.send "\0", 0, Socket.pack_sockaddr_in(@port, resolved_host)
        when :none # :none doesn't use this class
          raise "BUG: heartbeat_type none must not use Node"
        else
          raise "BUG: unknown heartbeat_type '#{@sender.heartbeat_type}'"
        end
      end

      def resolved_host
        case @sender.expire_dns_cache
        when 0
          # cache is disabled
          resolve_dns!

        when nil
          # persistent cache
          @resolved_host ||= resolve_dns!

        else
          now = Engine.now
          rh = @resolved_host
          if !rh || now - @resolved_time >= @sender.expire_dns_cache
            rh = @resolved_host = resolve_dns!
            @resolved_time = now
          end
          rh
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
          if phi > @sender.phi_threshold
            @log.warn "detached forwarding server '#{@name}'", host: @host, port: @port, phi: phi, phi_threshold: @sender.phi_threshold
            @available = false
            @resolved_host = nil  # expire cached host
            @failure.clear
            return true
          end
        end
        false
      end

      def heartbeat(detect=true)
        now = Time.now.to_f
        @failure.add(now)
        if detect && !@available && @failure.sample_size > @sender.recover_sample_size
          @available = true
          @log.warn "recovered forwarding server '#{@name}'", host: @host, port: @port
          true
        else
          nil
        end
      end

      # TODO: #to_msgpack(string) is deprecated
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
        opts = message[1] || {}
        # make shared_key_check failed (instead of error) if protocol version mismatch exist
        @shared_key_nonce = opts['nonce'] || ''
        @authentication = opts['auth'] || ''
        true
      end

      def generate_ping
        @log.debug "generating ping"
        # ['PING', self_hostname, sharedkey\_salt, sha512\_hex(sharedkey\_salt + self_hostname + nonce + shared_key),
        #  username || '', sha512\_hex(auth\_salt + username + password) || '']
        shared_key_hexdigest = Digest::SHA512.new.update(@shared_key_salt)
          .update(@sender.security.self_hostname)
          .update(@shared_key_nonce)
          .update(@shared_key)
          .hexdigest
        ping = ['PING', @sender.security.self_hostname, @shared_key_salt, shared_key_hexdigest]
        if !@authentication.empty?
          password_hexdigest = Digest::SHA512.new.update(@authentication).update(@username).update(@password).hexdigest
          ping.push(@username, password_hexdigest)
        else
          ping.push('','')
        end
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

        return true, nil
      end

      def on_read(sock, data)
        @log.trace __callee__

        case @state
        when :helo
          unless check_helo(data)
            @log.warn "received invalid helo message from #{@name}"
            disable! # shutdown
            return
          end
          sock.write(generate_ping.to_msgpack)
          @state = :pingpong
        when :pingpong
          succeeded, reason = check_pong(data)
          unless succeeded
            @log.warn "connection refused to #{@name}: #{reason}"
            disable! # shutdown
            return
          end
          @state = :established
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
  end
end
