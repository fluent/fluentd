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
require 'fluent/tls'
require 'base64'
require 'forwardable'

require 'fluent/compat/socket_util'
require 'fluent/plugin/out_forward/handshake_protocol'
require 'fluent/plugin/out_forward/load_balancer'
require 'fluent/plugin/out_forward/socket_cache'
require 'fluent/plugin/out_forward/failure_detector'
require 'fluent/plugin/out_forward/error'
require 'fluent/plugin/out_forward/connection_manager'
require 'fluent/plugin/out_forward/ack_handler'

module Fluent::Plugin
  class ForwardOutput < Output
    Fluent::Plugin.register_output('forward', self)

    helpers :socket, :server, :timer, :thread, :compat_parameters, :service_discovery

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
    config_param :tls_version, :enum, list: Fluent::TLS::SUPPORTED_VERSIONS, default: Fluent::TLS::DEFAULT_VERSION
    desc 'The cipher configuration of TLS transport.'
    config_param :tls_ciphers, :string, default: Fluent::TLS::CIPHERS_DEFAULT
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
    desc 'The certificate thumbprint for searching from Windows system certstore.'
    config_param :tls_cert_thumbprint, :string, default: nil, secret: true
    desc 'The certificate logical store name on Windows system certstore.'
    config_param :tls_cert_logical_store_name, :string, default: nil
    desc 'Enable to use certificate enterprise store on Windows system certstore.'
    config_param :tls_cert_use_enterprise_store, :bool, default: true
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
      @keep_alive_watcher_interval = 5 # TODO
      @suspend_flush = false
      @healthy_nodes_count_metrics = nil
      @registered_nodes_count_metrics = nil
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
        end
        if @tls_ca_cert_path && !@tls_ca_cert_path.empty?
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

        if Fluent.windows?
          if (@tls_cert_path || @tls_ca_cert_path) && @tls_cert_logical_store_name
            raise Fluent::ConfigError, "specified both cert path and tls_cert_logical_store_name is not permitted"
          end
        else
          raise Fluent::ConfigError, "This parameter is for only Windows" if @tls_cert_logical_store_name
          raise Fluent::ConfigError, "This parameter is for only Windows" if @tls_cert_thumbprint
        end
      end

      @ack_handler = @require_ack_response ? AckHandler.new(timeout: @ack_response_timeout, log: @log, read_length: @read_length) : nil
      socket_cache = @keepalive ? SocketCache.new(@keepalive_timeout, @log) : nil
      @connection_manager = ConnectionManager.new(
        log: @log,
        secure: !!@security,
        connection_factory: method(:create_transfer_socket),
        socket_cache: socket_cache,
      )

      service_discovery_configure(
        :out_forward_service_discovery_watcher,
        static_default_service_directive: 'server',
        load_balancer: LoadBalancer.new(log),
        custom_build_method: method(:build_node),
      )

      service_discovery_services.each do |server|
        # it's only for test
        @nodes << server
        unless @heartbeat_type == :none
          begin
            server.validate_host_resolution!
          rescue => e
            raise unless @ignore_network_errors_at_startup
            log.warn "failed to resolve node name when configured", server: (server.name || server.host), error: e
            server.disable!
          end
        end
      end

      unless @as_secondary
        if @compress == :gzip && @buffer.compress == :text
          @buffer.compress = :gzip
        elsif @compress == :text && @buffer.compress == :gzip
          log.info "buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>"
        end
      end

      if service_discovery_services.empty?
        raise Fluent::ConfigError, "forward output plugin requires at least one node is required. Add <server> or <service_discovery>"
      end

      if !@keepalive && @keepalive_timeout
        log.warn('The value of keepalive_timeout is ignored. if you want to use keepalive, please add `keepalive true` to your conf.')
      end

      raise Fluent::ConfigError, "ack_response_timeout must be a positive integer" if @ack_response_timeout < 1
      @healthy_nodes_count_metrics = metrics_create(namespace: "fluentd", subsystem: "output", name: "healthy_nodes_count", help_text: "Number of count healthy nodes", prefer_gauge: true)
      @registered_nodes_count_metrics = metrics_create(namespace: "fluentd", subsystem: "output", name: "registered_nodes_count", help_text: "Number of count registered nodes", prefer_gauge: true)

    end

    def multi_workers_ready?
      true
    end

    def prefer_delayed_commit
      @require_ack_response
    end

    def overwrite_delayed_commit_timeout
      # Output#start sets @delayed_commit_timeout by @buffer_config.delayed_commit_timeout
      # But it should be overwritten by ack_response_timeout to rollback chunks after timeout
      if @delayed_commit_timeout != @ack_response_timeout
        log.info "delayed_commit_timeout is overwritten by ack_response_timeout"
        @delayed_commit_timeout = @ack_response_timeout + 2 # minimum ack_reader IO.select interval is 1s
      end
    end

    def start
      super

      unless @heartbeat_type == :none
        if @heartbeat_type == :udp
          @usock = socket_create_udp(service_discovery_services.first.host, service_discovery_services.first.port, nonblock: true)
          server_create_udp(:out_forward_heartbeat_receiver, 0, socket: @usock, max_bytes: @read_length, &method(:on_udp_heatbeat_response_recv))
        end
        timer_execute(:out_forward_heartbeat_request, @heartbeat_interval, &method(:on_heartbeat_timer))
      end

      if @require_ack_response
        overwrite_delayed_commit_timeout
        thread_create(:out_forward_receiving_ack, &method(:ack_reader))
      end

      if @verify_connection_at_startup
        service_discovery_services.each do |node|
          begin
            node.verify_connection
          rescue StandardError => e
            log.fatal "forward's connection setting error: #{e.message}"
            raise Fluent::UnrecoverableError, e.message
          end
        end
      end

      if @keepalive
        timer_execute(:out_forward_keep_alived_socket_watcher, @keep_alive_watcher_interval, &method(:on_purge_obsolete_socks))
      end
    end

    def close
      if @usock
        # close socket and ignore errors: this socket will not be used anyway.
        @usock.close rescue nil
      end

      super
    end

    def stop
      super

      if @keepalive
        @connection_manager.stop
      end
    end

    def before_shutdown
      super
      @suspend_flush = true
    end

    def after_shutdown
      last_ack if @require_ack_response
      super
    end

    def try_flush
      return if @require_ack_response && @suspend_flush
      super
    end

    def last_ack
      overwrite_delayed_commit_timeout
      ack_check(ack_select_interval)
    end

    def write(chunk)
      return if chunk.empty?
      tag = chunk.metadata.tag

      service_discovery_select_service { |node| node.send_data(tag, chunk) }
    end

    def try_write(chunk)
      log.trace "writing a chunk to destination", chunk_id: dump_unique_id_hex(chunk.unique_id)
      if chunk.empty?
        commit_write(chunk.unique_id)
        return
      end
      tag = chunk.metadata.tag
      service_discovery_select_service { |node| node.send_data(tag, chunk) }
      last_ack if @require_ack_response && @suspend_flush
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
          cert_thumbprint: @tls_cert_thumbprint,
          cert_logical_store_name: @tls_cert_logical_store_name,
          cert_use_enterprise_store: @tls_cert_use_enterprise_store,

          # Enabling SO_LINGER causes tcp port exhaustion on Windows.
          # This is because dynamic ports are only 16384 (from 49152 to 65535) and
          # expiring SO_LINGER enabled ports should wait 4 minutes
          # where set by TcpTimeDelay. Its default value is 4 minutes.
          # So, we should disable SO_LINGER on Windows to prevent flood of waiting ports.
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

    def statistics
      stats = super
      services = service_discovery_services
      @healthy_nodes_count_metrics.set(0)
      @registered_nodes_count_metrics.set(services.size)
      services.each do |s|
        if s.available?
          @healthy_nodes_count_metrics.inc
        end
      end

      stats = {
        'output' => stats["output"].merge({
          'healthy_nodes_count' => @healthy_nodes_count_metrics.get,
          'registered_nodes_count' => @registered_nodes_count_metrics.get,
        })
      }
      stats
    end

    # MessagePack FixArray length is 3
    FORWARD_HEADER = [0x93].pack('C').freeze
    def forward_header
      FORWARD_HEADER
    end

    private

    def build_node(server)
      name = server.name || "#{server.host}:#{server.port}"
      log.info "adding forwarding server '#{name}'", host: server.host, port: server.port, weight: server.weight, plugin_id: plugin_id

      failure = FailureDetector.new(@heartbeat_interval, @hard_timeout, Time.now.to_i.to_f)
      if @heartbeat_type == :none
        NoneHeartbeatNode.new(self, server, failure: failure, connection_manager: @connection_manager, ack_handler: @ack_handler)
      else
        Node.new(self, server, failure: failure, connection_manager: @connection_manager, ack_handler: @ack_handler)
      end
    end

    def on_heartbeat_timer
      need_rebuild = false
      service_discovery_services.each do |n|
        begin
          log.trace "sending heartbeat", host: n.host, port: n.port, heartbeat_type: @heartbeat_type
          n.usock = @usock if @usock
          need_rebuild = n.send_heartbeat || need_rebuild
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR, Errno::ECONNREFUSED, Errno::ETIMEDOUT => e
          log.debug "failed to send heartbeat packet", host: n.host, port: n.port, heartbeat_type: @heartbeat_type, error: e
        rescue => e
          log.debug "unexpected error happen during heartbeat", host: n.host, port: n.port, heartbeat_type: @heartbeat_type, error: e
        end

        need_rebuild = n.tick || need_rebuild
      end

      if need_rebuild
        service_discovery_rebalance
      end
    end

    def on_udp_heatbeat_response_recv(data, sock)
      sockaddr = Socket.pack_sockaddr_in(sock.remote_port, sock.remote_host)
      if node = service_discovery_services.find { |n| n.sockaddr == sockaddr }
        # log.trace "heartbeat arrived", name: node.name, host: node.host, port: node.port
        if node.heartbeat
          service_discovery_rebalance
        end
      else
        log.warn("Unknown heartbeat response received from #{sock.remote_host}:#{sock.remote_port}. It may service out")
      end
    end

    def on_purge_obsolete_socks
      @connection_manager.purge_obsolete_socks
    end

    def ack_select_interval
      if @delayed_commit_timeout > 3
        1
      else
        @delayed_commit_timeout / 3.0
      end
    end

    def ack_reader
      select_interval = ack_select_interval

      while thread_current_running?
        ack_check(select_interval)
      end
    end

    def ack_check(select_interval)
      @ack_handler.collect_response(select_interval) do |chunk_id, node, sock, result|
        @connection_manager.close(sock)

        case result
        when AckHandler::Result::SUCCESS
          commit_write(chunk_id)
        when AckHandler::Result::FAILED
          node&.disable!
          rollback_write(chunk_id, update_retry: false) if chunk_id
        when AckHandler::Result::CHUNKID_UNMATCHED
          rollback_write(chunk_id, update_retry: false)
        else
          log.warn("BUG: invalid status #{result} #{chunk_id}")

          if chunk_id
            rollback_write(chunk_id, update_retry: false)
          end
        end
      end
    end

    class Node
      extend Forwardable
      def_delegators :@server, :discovery_id, :host, :port, :name, :weight, :standby

      # @param connection_manager [Fluent::Plugin::ForwardOutput::ConnectionManager]
      # @param ack_handler [Fluent::Plugin::ForwardOutput::AckHandler]
      def initialize(sender, server, failure:, connection_manager:, ack_handler:)
        @sender = sender
        @log = sender.log
        @compress = sender.compress
        @server = server

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
          password: server.password || '',
          username: server.username || '',
        )

        @resolved_host = nil
        @resolved_time = 0
        @resolved_once = false

        @connection_manager = connection_manager
        @ack_handler = ack_handler
      end

      attr_accessor :usock

      attr_reader :state
      attr_reader :sockaddr  # used by on_udp_heatbeat_response_recv
      attr_reader :failure # for test

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
          ensure_established_connection(sock, ri)
        end
      end

      def establish_connection(sock, ri)
        start_time = Fluent::Clock.now
        timeout = @sender.hard_timeout

        while ri.state != :established
          # Check for timeout to prevent infinite loop
          if Fluent::Clock.now - start_time > timeout
            @log.warn "handshake timeout after #{timeout}s", host: @host, port: @port
            disable!
            break
          end

          begin
            # TODO: On Ruby 2.2 or earlier, read_nonblock doesn't work expectedly.
            # We need rewrite around here using new socket/server plugin helper.
            buf = sock.read_nonblock(@sender.read_length)
            if buf.empty?
              sleep @sender.read_interval
              next
            end
            Fluent::MessagePackFactory.msgpack_unpacker.feed_each(buf) do |data|
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
        option = { 'size' => chunk.size, 'compressed' => @compress }
        option['chunk'] = Base64.encode64(chunk.unique_id) if @ack_handler

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
        ack = @ack_handler && @ack_handler.create_ack(chunk.unique_id, self)
        connect(nil, ack: ack) do |sock, ri|
          ensure_established_connection(sock, ri)
          send_data_actual(sock, tag, chunk)
        end

        heartbeat(false)
        nil
      end

      # FORWARD_TCP_HEARTBEAT_DATA = FORWARD_HEADER + ''.to_msgpack + [].to_msgpack
      #
      # @return [Boolean] return true if it needs to rebuild nodes
      def send_heartbeat
        begin
          dest_addr = resolved_host
          @resolved_once = true
        rescue ::SocketError => e
          if !@resolved_once && @sender.ignore_network_errors_at_startup
            @log.warn "failed to resolve node name in heartbeating", server: @name || @host, error: e
            return false
          end
          raise
        end

        case @sender.heartbeat_type
        when :transport
          connect(dest_addr) do |sock, ri|
            ensure_established_connection(sock, ri)

            ## don't send any data to not cause a compatibility problem
            # sock.write FORWARD_TCP_HEARTBEAT_DATA

            # successful tcp connection establishment is considered as valid heartbeat.
            # When heartbeat is succeeded after detached, return true. It rebuilds weight array.
            heartbeat(true)
          end
        when :udp
          @usock.send "\0", 0, Socket.pack_sockaddr_in(@port, dest_addr)
          # response is going to receive at on_udp_heatbeat_response_recv
          false
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
          now = Fluent::EventTime.now
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
        unless available?
          if @failure.hard_timeout?(now)
            @failure.clear
          end
          return nil
        end

        if @failure.hard_timeout?(now)
          @log.warn "detached forwarding server '#{@name}'", host: @host, port: @port, hard_timeout: true
          disable!
          @resolved_host = nil  # expire cached host
          @failure.clear
          return true
        end

        if @sender.phi_failure_detector
          phi = @failure.phi(now)
          if phi > @sender.phi_threshold
            @log.warn "detached forwarding server '#{@name}'", host: @host, port: @port, phi: phi, phi_threshold: @sender.phi_threshold
            disable!
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
        if detect && !available? && @failure.sample_size > @sender.recover_sample_size
          @available = true
          @log.warn "recovered forwarding server '#{@name}'", host: @host, port: @port
          true
        else
          nil
        end
      end

      private

      def ensure_established_connection(sock, request_info)
        if request_info.state != :established
          establish_connection(sock, request_info)

          if request_info.state != :established
            raise ConnectionClosedError, "failed to establish connection with node #{@name}"
          end
        end
      end

      def connect(host = nil, ack: false, &block)
        @connection_manager.connect(host: host || resolved_host, port: port, hostname: @hostname, ack: ack, &block)
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
