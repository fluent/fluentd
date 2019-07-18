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

require 'fluent/output'
require 'fluent/config/error'
require 'fluent/clock'
require 'base64'

require 'fluent/compat/socket_util'
require 'fluent/plugin/out_forward/handshake_protocol'
require 'fluent/plugin/out_forward/load_balancer'
require 'fluent/plugin/out_forward/socket_cache'
require 'fluent/plugin/out_forward/failure_detector'
require 'fluent/plugin/out_forward/error'

module Fluent::Plugin
  class ForwardOutput < Output
    Fluent::Plugin.register_output('forward', self)

    helpers :socket, :server, :timer, :thread, :compat_parameters

    LISTEN_PORT = 24224

    desc 'The transport protocol.'
    config_param :transport, :enum, list: [:tcp, :tls], default: :tcp
    # TODO: TLS session cache/tickets

    desc 'The timeout time when sending event logs.'
    config_param :send_timeout, :time, default: 60
    desc 'The timeout time for socket connect'
    config_param :connect_timeout, :time, default: nil
    # TODO: add linger_timeout, recv_timeout

    desc 'The protocol to use for heartbeats (default is the same with "transport").'
    config_param :heartbeat_type, :enum, list: [:transport, :tcp, :udp, :none], default: :transport
    desc 'The interval of the heartbeat packer.'
    config_param :heartbeat_interval, :time, default: 1
    desc 'The wait time before accepting a server fault recovery.'
    config_param :recover_wait, :time, default: 10
    desc 'The hard timeout used to detect server failure.'
    config_param :hard_timeout, :time, default: 60
    desc 'The threshold parameter used to detect server faults.'
    config_param :phi_threshold, :integer, default: 16
    desc 'Use the "Phi accrual failure detector" to detect server failure.'
    config_param :phi_failure_detector, :bool, default: true

    desc 'Change the protocol to at-least-once.'
    config_param :require_ack_response, :bool, default: false  # require in_forward to respond with ack

    ## The reason of default value of :ack_response_timeout:
    # Linux default tcp_syn_retries is 5 (in many environment)
    # 3 + 6 + 12 + 24 + 48 + 96 -> 189 (sec)
    desc 'This option is used when require_ack_response is true.'
    config_param :ack_response_timeout, :time, default: 190

    desc 'The interval while reading data from server'
    config_param :read_interval_msec, :integer, default: 50 # 50ms
    desc 'Reading data size from server'
    config_param :read_length, :size, default: 512 # 512bytes

    desc 'Set TTL to expire DNS cache in seconds.'
    config_param :expire_dns_cache, :time, default: nil  # 0 means disable cache
    desc 'Enable client-side DNS round robin.'
    config_param :dns_round_robin, :bool, default: false # heartbeat_type 'udp' is not available for this

    desc 'Ignore DNS resolution and errors at startup time.'
    config_param :ignore_network_errors_at_startup, :bool, default: false

    desc 'Verify that a connection can be made with one of out_forward nodes at the time of startup.'
    config_param :verify_connection_at_startup, :bool, default: false

    desc 'Compress buffered data.'
    config_param :compress, :enum, list: [:text, :gzip], default: :text

    desc 'The default version of TLS transport.'
    config_param :tls_version, :enum, list: Fluent::PluginHelper::Socket::TLS_SUPPORTED_VERSIONS, default: Fluent::PluginHelper::Socket::TLS_DEFAULT_VERSION
    desc 'The cipher configuration of TLS transport.'
    config_param :tls_ciphers, :string, default: Fluent::PluginHelper::Socket::CIPHERS_DEFAULT
    desc 'Skip all verification of certificates or not.'
    config_param :tls_insecure_mode, :bool, default: false
    desc 'Allow self signed certificates or not.'
    config_param :tls_allow_self_signed_cert, :bool, default: false
    desc 'Verify hostname of servers and certificates or not in TLS transport.'
    config_param :tls_verify_hostname, :bool, default: true
    desc 'The additional CA certificate path for TLS.'
    config_param :tls_ca_cert_path, :array, value_type: :string, default: nil
    desc 'The additional certificate path for TLS.'
    config_param :tls_cert_path, :array, value_type: :string, default: nil
    desc 'The client certificate path for TLS.'
    config_param :tls_client_cert_path, :string, default: nil
    desc 'The client private key path for TLS.'
    config_param :tls_client_private_key_path, :string, default: nil
    desc 'The client private key passphrase for TLS.'
    config_param :tls_client_private_key_passphrase, :string, default: nil, secret: true
    desc "Enable keepalive connection."
    config_param :keepalive, :bool, default: false
    desc "Expired time of keepalive. Default value is nil, which means to keep connection as long as possible"
    config_param :keepalive_timeout, :time, default: nil

    config_section :security, required: false, multi: false do
      desc 'The hostname'
      config_param :self_hostname, :string
      desc 'Shared key for authentication'
      config_param :shared_key, :string, secret: true
    end

    config_section :server, param_name: :servers do
      desc "The IP address or host name of the server."
      config_param :host, :string
      desc "The name of the server. Used for logging and certificate verification in TLS transport (when host is address)."
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

    config_section :buffer do
      config_set_default :chunk_keys, ["tag"]
    end

    attr_reader :read_interval, :recover_sample_size

    def initialize
      super

      @nodes = [] #=> [Node]
      @loop = nil
      @thread = nil

      @usock = nil
      @sock_ack_waiting = nil
      @sock_ack_waiting_mutex = nil
      @keep_alive_watcher_interval = 5 # TODO
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, default_chunk_key: 'tag')

      super

      unless @chunk_key_tag
        raise Fluent::ConfigError, "buffer chunk key must include 'tag' for forward output"
      end

      @read_interval = @read_interval_msec / 1000.0
      @recover_sample_size = @recover_wait / @heartbeat_interval

      if @heartbeat_type == :tcp
        log.warn "'heartbeat_type tcp' is deprecated. use 'transport' instead."
        @heartbeat_type = :transport
      end

      if @dns_round_robin && @heartbeat_type == :udp
        raise Fluent::ConfigError, "forward output heartbeat type must be 'transport' or 'none' to use dns_round_robin option"
      end

      if @transport == :tls
        # socket helper adds CA cert or signed certificate to same cert store internally so unify it in this place.
        if @tls_cert_path && !@tls_cert_path.empty?
          @tls_ca_cert_path = @tls_cert_path
          @tls_ca_cert_path.each do |path|
            raise Fluent::ConfigError, "specified cert path does not exist:#{path}" unless File.exist?(path)
            raise Fluent::ConfigError, "specified cert path is not readable:#{path}" unless File.readable?(path)
          end
        end

        if @tls_insecure_mode
          log.warn "TLS transport is configured in insecure way"
          @tls_verify_hostname = false
          @tls_allow_self_signed_cert = true
        end
      end

      @servers.each do |server|
        failure = FailureDetector.new(@heartbeat_interval, @hard_timeout, Time.now.to_i.to_f)
        name = server.name || "#{server.host}:#{server.port}"

        log.info "adding forwarding server '#{name}'", host: server.host, port: server.port, weight: server.weight, plugin_id: plugin_id
        if @heartbeat_type == :none
          @nodes << NoneHeartbeatNode.new(self, server, failure: failure, keepalive: @keepalive, keepalive_timeout: @keepalive_timeout)
        else
          node = Node.new(self, server, failure: failure, keepalive: @keepalive, keepalive_timeout: @keepalive_timeout)
          begin
            node.validate_host_resolution!
          rescue => e
            raise unless @ignore_network_errors_at_startup
            log.warn "failed to resolve node name when configured", server: (server.name || server.host), error: e
            node.disable!
          end
          @nodes << node
        end
      end

      unless @as_secondary
        if @compress == :gzip && @buffer.compress == :text
          @buffer.compress = :gzip
        elsif @compress == :text && @buffer.compress == :gzip
          log.info "buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>"
        end
      end

      if @nodes.empty?
        raise Fluent::ConfigError, "forward output plugin requires at least one <server> is required"
      end

      if !@keepalive && @keepalive_timeout
        log.warn('The value of keepalive_timeout is ignored. if you want to use keepalive, please add `keepalive true` to your conf.')
      end

      raise Fluent::ConfigError, "ack_response_timeout must be a positive integer" if @ack_response_timeout < 1
    end

    def multi_workers_ready?
      true
    end

    def prefer_delayed_commit
      @require_ack_response
    end

    def start
      super

      # Output#start sets @delayed_commit_timeout by @buffer_config.delayed_commit_timeout
      # But it should be overwritten by ack_response_timeout to rollback chunks after timeout
      if @ack_response_timeout && @delayed_commit_timeout != @ack_response_timeout
        log.info "delayed_commit_timeout is overwritten by ack_response_timeout"
        @delayed_commit_timeout = @ack_response_timeout + 2 # minimum ack_reader IO.select interval is 1s
      end

      @load_balancer = LoadBalancer.new(log)
      @load_balancer.rebuild_weight_array(@nodes)

      unless @heartbeat_type == :none
        if @heartbeat_type == :udp
          @usock = socket_create_udp(@nodes.first.host, @nodes.first.port, nonblock: true)
          server_create_udp(:out_forward_heartbeat_receiver, 0, socket: @usock, max_bytes: @read_length, &method(:on_udp_heatbeat_response_recv))
        end
        timer_execute(:out_forward_heartbeat_request, @heartbeat_interval, &method(:on_heartbeat_timer))
      end

      if @require_ack_response
        @sock_ack_waiting_mutex = Mutex.new
        @sock_ack_waiting = []
        thread_create(:out_forward_receiving_ack, &method(:ack_reader))
      end

      if @verify_connection_at_startup
        @nodes.each do |node|
          begin
            node.verify_connection
          rescue StandardError => e
            log.fatal "forward's connection setting error: #{e.message}"
            raise Fluent::UnrecoverableError, e.message
          end
        end
      end

      if @keepalive && @keepalive_timeout
        timer_execute(:out_forward_keep_alived_socket_watcher, @keep_alive_watcher_interval, &method(:on_purge_obsolete_socks))
      end
    end

    def close
      if @usock
        # close socket and ignore errors: this socket will not be used anyway.
        @usock.close rescue nil
      end

      if @keepalive && @keepalive_timeout
        @nodes.each(&:clear)
      end
      super
    end

    def write(chunk)
      return if chunk.empty?
      tag = chunk.metadata.tag

      @load_balancer.select_healthy_node { |node| node.send_data(tag, chunk) }
    end

    ACKWaitingSockInfo = Struct.new(:sock, :chunk_id, :chunk_id_base64, :node, :time, :timeout) do
      def expired?(now)
        time + timeout < now
      end
    end

    def try_write(chunk)
      log.trace "writing a chunk to destination", chunk_id: dump_unique_id_hex(chunk.unique_id)
      if chunk.empty?
        commit_write(chunk.unique_id)
        return
      end
      tag = chunk.metadata.tag
      sock, node = @load_balancer.select_healthy_node { |n| n.send_data(tag, chunk) }
      chunk_id_base64 = Base64.encode64(chunk.unique_id)
      current_time = Fluent::Clock.now
      info = ACKWaitingSockInfo.new(sock, chunk.unique_id, chunk_id_base64, node, current_time, @ack_response_timeout)
      @sock_ack_waiting_mutex.synchronize do
        @sock_ack_waiting << info
      end
    end

    def create_transfer_socket(host, port, hostname, &block)
      case @transport
      when :tls
        socket_create_tls(
          host, port,
          version: @tls_version,
          ciphers: @tls_ciphers,
          insecure: @tls_insecure_mode,
          verify_fqdn: @tls_verify_hostname,
          fqdn: hostname,
          allow_self_signed_cert: @tls_allow_self_signed_cert,
          cert_paths: @tls_ca_cert_path,
          cert_path: @tls_client_cert_path,
          private_key_path: @tls_client_private_key_path,
          private_key_passphrase: @tls_client_private_key_passphrase,

          # Enabling SO_LINGER causes data loss on Windows
          # https://github.com/fluent/fluentd/issues/1968
          linger_timeout: Fluent.windows? ? nil : @send_timeout,
          send_timeout: @send_timeout,
          recv_timeout: @ack_response_timeout,
          connect_timeout: @connect_timeout,
          &block
        )
      when :tcp
        socket_create_tcp(
          host, port,
          linger_timeout: @send_timeout,
          send_timeout: @send_timeout,
          recv_timeout: @ack_response_timeout,
          connect_timeout: @connect_timeout,
          &block
        )
      else
        raise "BUG: unknown transport protocol #{@transport}"
      end
    end

    # MessagePack FixArray length is 3
    FORWARD_HEADER = [0x93].pack('C').freeze
    def forward_header
      FORWARD_HEADER
    end

    private

    def on_heartbeat_timer
      @nodes.each {|n|
        begin
          log.trace "sending heartbeat", host: n.host, port: n.port, heartbeat_type: @heartbeat_type
          n.usock = @usock if @usock
          if n.send_heartbeat
            @load_balancer.rebuild_weight_array(@nodes)
          end
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR, Errno::ECONNREFUSED, Errno::ETIMEDOUT => e
          log.debug "failed to send heartbeat packet", host: n.host, port: n.port, heartbeat_type: @heartbeat_type, error: e
        rescue => e
          log.debug "unexpected error happen during heartbeat", host: n.host, port: n.port, heartbeat_type: @heartbeat_type, error: e
        end
        if n.tick
          @load_balancer.rebuild_weight_array(@nodes)
        end
      }
    end

    def on_udp_heatbeat_response_recv(data, sock)
      sockaddr = Socket.pack_sockaddr_in(sock.remote_port, sock.remote_host)
      if node = @nodes.find { |n| n.sockaddr == sockaddr }
        # log.trace "heartbeat arrived", name: node.name, host: node.host, port: node.port
        if node.heartbeat
          @load_balancer.rebuild_weight_array(@nodes)
        end
      else
        log.warn("Unknown heartbeat response received host: #{soc.remote_host}, port: #{sock.remote_port}")
      end
    end

    def on_purge_obsolete_socks
      @nodes.each(&:purge_obsolete_socks)
    end

    # return chunk id to be committed
    def read_ack_from_sock(sock, unpacker)
      begin
        raw_data = sock.instance_of?(Fluent::PluginHelper::Socket::WrappedSocket::TLS) ? sock.readpartial(@read_length) : sock.recv(@read_length)
      rescue Errno::ECONNRESET, EOFError # ECONNRESET for #recv, #EOFError for #readpartial
        raw_data = ""
      end
      info = @sock_ack_waiting_mutex.synchronize{ @sock_ack_waiting.find{|i| i.sock == sock } }

      # When connection is closed by remote host, socket is ready to read and #recv returns an empty string that means EOF.
      # If this happens we assume the data wasn't delivered and retry it.
      if raw_data.empty?
        log.warn "destination node closed the connection. regard it as unavailable.", host: info.node.host, port: info.node.port
        info.node.disable!
        rollback_write(info.chunk_id, update_retry: false)
        return nil
      else
        unpacker.feed(raw_data)
        res = unpacker.read
        log.trace "getting response from destination", host: info.node.host, port: info.node.port, chunk_id: dump_unique_id_hex(info.chunk_id), response: res
        if res['ack'] != info.chunk_id_base64
          # Some errors may have occurred when ack and chunk id is different, so send the chunk again.
          log.warn "ack in response and chunk id in sent data are different", chunk_id: dump_unique_id_hex(info.chunk_id), ack: res['ack']
          rollback_write(info.chunk_id, update_retry: false)
          return nil
        else
          log.trace "got a correct ack response", chunk_id: dump_unique_id_hex(info.chunk_id)
        end
        return info.chunk_id
      end
    rescue => e
      log.error "unexpected error while receiving ack message", error: e
      log.error_backtrace
    ensure
      if @keepalive
        info.node.socket_cache.dec_ref_by_value(info.sock)
      else
        info.sock.close_write rescue nil
        info.sock.close rescue nil
      end

      @sock_ack_waiting_mutex.synchronize do
        @sock_ack_waiting.delete(info)
      end
    end

    def ack_reader
      select_interval = if @delayed_commit_timeout > 3
                          1
                        else
                          @delayed_commit_timeout / 3.0
                        end

      unpacker = Fluent::Engine.msgpack_unpacker

      while thread_current_running?
        now = Fluent::Clock.now
        sockets = []
        begin
          @sock_ack_waiting_mutex.synchronize do
            new_list = []
            @sock_ack_waiting.each do |info|
              if info.expired?(now)
                # There are 2 types of cases when no response has been received from socket:
                # (1) the node does not support sending responses
                # (2) the node does support sending response but responses have not arrived for some reasons.
                log.warn "no response from node. regard it as unavailable.", host: info.node.host, port: info.node.port
                info.node.disable!
                if @keepalive
                  info.node.socket_cache.revoke_by_value(info.sock)
                end
                info.sock.close rescue nil
                rollback_write(info.chunk_id, update_retry: false)
              else
                sockets << info.sock
                new_list << info
              end
            end
            @sock_ack_waiting = new_list
          end

          readable_sockets, _, _ = IO.select(sockets, nil, nil, select_interval)
          next unless readable_sockets

          readable_sockets.each do |sock|
            chunk_id = read_ack_from_sock(sock, unpacker)
            commit_write(chunk_id) if chunk_id
          end
        rescue => e
          log.error "unexpected error while receiving ack", error: e
          log.error_backtrace
        end
      end
    end

    class Node
      # @param keepalive [Bool]
      # @param keepalive_timeout [Integer | nil]
      def initialize(sender, server, failure:, keepalive: false, keepalive_timeout: nil)
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

        # @hostname is used for certificate verification & TLS SNI
        host_is_hostname = !(IPAddr.new(@host) rescue false)
        @hostname = case
                    when host_is_hostname then @host
                    when @name then @name
                    else nil
                    end

        @usock = nil

        @handshake = HandshakeProtocol.new(
          log: @log,
          hostname: sender.security && sender.security.self_hostname,
          shared_key: server.shared_key || (sender.security && sender.security.shared_key) || '',
          password: server.password,
          username: server.username,
        )

        @unpacker = Fluent::Engine.msgpack_unpacker

        @resolved_host = nil
        @resolved_time = 0
        @resolved_once = false

        @keepalive = keepalive
        if @keepalive
          @socket_cache = ForwardOutput::SocketCache.new(keepalive_timeout, @log)
        end
      end

      attr_accessor :usock

      attr_reader :name, :host, :port, :weight, :standby, :state
      attr_reader :sockaddr  # used by on_udp_heatbeat_response_recv
      attr_reader :failure, :available # for test
      attr_reader :socket_cache        # for ack

      RequestInfo = Struct.new(:state, :shared_key_nonce, :auth)

      def validate_host_resolution!
        resolved_host
      end

      def available?
        @available
      end

      def disable!
        @available = false
      end

      def standby?
        @standby
      end

      def verify_connection
        connect do |sock, ri|
          if ri.state != :established
            establish_connection(sock, ri)
            raise if ri.state != :established
          end
        end
      end

      def establish_connection(sock, ri)
        while available? && ri.state != :established
          begin
            # TODO: On Ruby 2.2 or earlier, read_nonblock doesn't work expectedly.
            # We need rewrite around here using new socket/server plugin helper.
            buf = sock.read_nonblock(@sender.read_length)
            if buf.empty?
              sleep @sender.read_interval
              next
            end
            @unpacker.feed_each(buf) do |data|
              if @handshake.invoke(sock, ri, data) == :established
                @log.debug "connection established", host: @host, port: @port
              end
            end
          rescue IO::WaitReadable
            # If the exception is Errno::EWOULDBLOCK or Errno::EAGAIN, it is extended by IO::WaitReadable.
            # So IO::WaitReadable can be used to rescue the exceptions for retrying read_nonblock.
            # https//docs.ruby-lang.org/en/2.3.0/IO.html#method-i-read_nonblock
            sleep @sender.read_interval unless ri.state == :established
          rescue SystemCallError => e
            @log.warn "disconnected by error", host: @host, port: @port, error: e
            disable!
            break
          rescue EOFError
            @log.warn "disconnected", host: @host, port: @port
            disable!
            break
          rescue HeloError => e
            @log.warn "received invalid helo message from #{@name}"
            disable!
            break
          rescue PingpongError => e
            @log.warn "connection refused to #{@name || @host}: #{e.message}"
            disable!
            break
          end
        end
      end

      def send_data_actual(sock, tag, chunk)
        unless available?
          raise ConnectionClosedError, "failed to establish connection with node #{@name}"
        end

        option = { 'size' => chunk.size, 'compressed' => @compress }
        option['chunk'] = Base64.encode64(chunk.unique_id) if @sender.require_ack_response

        # https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#packedforward-mode
        # out_forward always uses str32 type for entries.
        # str16 can store only 64kbytes, and it should be much smaller than buffer chunk size.

        tag = tag.dup.force_encoding(Encoding::UTF_8)

        sock.write @sender.forward_header                    # array, size=3
        sock.write tag.to_msgpack                            # 1. tag: String (str)
        chunk.open(compressed: @compress) do |chunk_io|
          entries = [0xdb, chunk_io.size].pack('CN')
          sock.write entries.force_encoding(Encoding::UTF_8) # 2. entries: String (str32)
          IO.copy_stream(chunk_io, sock)                     #    writeRawBody(packed_es)
        end
        sock.write option.to_msgpack                         # 3. option: Hash(map)

        # TODO: use bin32 for non-utf8 content(entries) when old msgpack-ruby (0.5.x or earlier) not supported
      end

      def send_data(tag, chunk)
        connect(nil, require_ack: @sender.require_ack_response) do |sock, ri|
          if ri.state != :established
            establish_connection(sock, ri)
          end

          send_data_actual(sock, tag, chunk)

          if @sender.require_ack_response
            return sock # to read ACK from socket
          end
        end

        heartbeat(false)
        nil
      end

      def clear
        @keepalive && @socket_cache.clear
      end

      def purge_obsolete_socks
        unless @keepalive
          raise "Don not call this method without keepalive option"
        end
        @socket_cache.purge_obsolete_socks
      end

      # FORWARD_TCP_HEARTBEAT_DATA = FORWARD_HEADER + ''.to_msgpack + [].to_msgpack
      def send_heartbeat
        begin
          dest_addr = resolved_host
          @resolved_once = true
        rescue ::SocketError => e
          if !@resolved_once && @sender.ignore_network_errors_at_startup
            @log.warn "failed to resolve node name in heartbeating", server: @name || @host, error: e
            return
          end
          raise
        end

        case @sender.heartbeat_type
        when :transport
          connect(dest_addr) do |_ri, _sock|
            ## don't send any data to not cause a compatibility problem
            # sock.write FORWARD_TCP_HEARTBEAT_DATA

            # successful tcp connection establishment is considered as valid heartbeat.
            # When heartbeat is succeeded after detached, return true. It rebuilds weight array.
            heartbeat(true)
          end
        when :udp
          @usock.send "\0", 0, Socket.pack_sockaddr_in(@port, resolved_host)
          # response is going to receive at on_udp_heatbeat_response_recv
          nil
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
          now = Fluent::Engine.now
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
        @sockaddr = Socket.pack_sockaddr_in(addrinfo[1], addrinfo[3]) # used by on_udp_heatbeat_response_recv
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

      private

      def connect(host = nil, require_ack: false, &block)
        if @keepalive
          return connect_keepalive(host, require_ack: require_ack, &block)
        end

        @log.debug('connect new socket')
        socket = @sender.create_transfer_socket(host || resolved_host, port, @hostname)
        request_info = RequestInfo.new(@sender.security ? :helo : :established)

        unless block_given?
          return [socket, request_info]
        end

        begin
          yield(socket, request_info)
        ensure
          unless require_ack
            socket.close_write rescue nil
            socket.close rescue nil
          end
        end
      end

      def connect_keepalive(host = nil, require_ack: false)
        request_info = RequestInfo.new(:established)
        socket = @socket_cache.fetch_or do
          s = @sender.create_transfer_socket(host || resolved_host, port, @hostname)
          request_info = RequestInfo.new(@sender.security ? :helo : :established) # overwrite if new connection
          s
        end

        unless block_given?
          return [socket, request_info]
        end

        ret = nil
        begin
          ret = yield(socket, request_info)
        rescue
          @socket_cache.revoke
          raise
        else
          unless require_ack
            @socket_cache.dec_ref
          end
        end

        ret
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
  end
end
