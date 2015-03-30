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

require 'fluent/plugin_support/socket'
require 'fluent/plugin_support/timer'

require 'ipaddr'
require 'socket'
require 'openssl'
require 'digest'
require 'securerandom'

module Fluent
  module PluginSupport
    module SSLServer
      include Fluent::PluginSupport::Socket
      include Fluent::PluginSupport::Thread
      include Fluent::PluginSupport::Timer

      SSL_SERVER_KEEPALIVE_CHECK_INTERVAL = 1
      SSL_SERVER_DEFAULT_READ_LENGTH = 8 * 1024 * 1024
      SSL_SERVER_DEFAULT_READ_INTERVAL = 0.05 # [s], 50ms
      SSL_SERVER_DEFAULT_SOCKET_RESTART_INTERVAL = 0.2 # [s], 200ms

      def ssl_server_generate_cert_key(digest: OpenSSL::Digest::SHA256, algorithm: OpenSSL::PKey::RSA, key_length: 2048, cert_country: 'US', cert_state: 'CA', cert_locality: 'Mountain View', cert_common_name: "Fluentd #{self.class} SSLServer")
        key = algorithm.generate(key_length)

        issuer = subject = OpenSSL::X509::Name.new
        subject.add_entry('C', cert_country)
        subject.add_entry('ST', cert_state)
        subject.add_entry('L', cert_locality)
        subject.add_entry('CN', cert_common_name)

        cert = OpenSSL::X509::Certificate.new
        cert.not_before = Time.at(0)
        cert.not_after = Time.now + 86400 * 365 * 5
        cert.public_key = key
        cert.serial = 1
        cert.issuer = issuer
        cert.subject  = subject

        digest_obj = digest.new
        cert.sign(key, digest_obj)

        return cert, key
      end

      def ssl_server_load_cert_key(cert_file_path:, algorithm: OpenSSL::PKey::RSA, key_file_path:, key_passphrase:)
        cert = OpenSSL::X509::Certificate.new(File.read(cert_file_path))
        key = algorithm.new(File.read(key_file_path), key_passphrase)
        return cert, key
      end

      # keepalive: seconds, (default: nil [inf])
      def ssl_server_listen(ssl_version: :TLSv1_2, ciphers: nil, cert:, key:, port:, bind: '0.0.0.0', keepalive: nil, linger_timeout: nil, backlog: nil, read_length: SSL_SERVER_DEFAULT_READ_LENGTH, read_interval: SSL_SERVER_KEEPALIVE_CHECK_INTERVAL, socket_restart_interval: SSL_SERVER_DEFAULT_SOCKET_RESTART_INTERVAL, &block)
        raise "BUG: callback block is not specified for ssl_server_listen" unless block_given?
        raise "BUG: specified SSL/TLS version '#{ssl_method}' is not supported in this environment" unless OpenSSL::SSL::SSLContext::METHODS.include?(ssl_version)
        port = port.to_i

        socket_listener_add('tcp', bind, port)

        ctx = OpenSSL::SSL::SSLContext.new(ssl_version)

        # inject OpenSSL::SSL::SSLContext::DEFAULT_PARAMS
        # https://bugs.ruby-lang.org/issues/9424
        ctx.set_params({})

        ctx.cert = cert
        ctx.key = key
        if ciphers
          ctx.ciphers = ciphers
        else
          ### follow httpclient configuration by nahi
          # OpenSSL 0.9.8 default: "ALL:!ADH:!LOW:!EXP:!MD5:+SSLv2:@STRENGTH"
          ctx.ciphers = "ALL:!aNULL:!eNULL:!SSLv2" # OpenSSL >1.0.0 default
        end
        ctx.timeout = keepalive || 86400 * 365 * 5 # 5 years is like infinity

        if self.respond_to?(:detach_multi_process)
          detach_multi_process do
            ssl_server_listen_impl(bind, port, ctx, keepalive, linger_timeout, backlog, read_length, read_interval, socket_restart_interval, &block)
          end
        elsif self.respond_to?(:detach_process)
          detach_process do
            ssl_server_listen_impl(bind, port, ctx, keepalive, linger_timeout, backlog, read_length, read_interval, socket_restart_interval, &block)
          end
        else
          ssl_server_listen_impl(bind, port, ctx, keepalive, linger_timeout, backlog, read_length, read_interval, socket_restart_interval, &block)
        end
      end

      def initialize
        super
        @_ssl_server_listen_servers = []
        @_ssl_server_listen_socks = []
        @_ssl_server_listen_threads = []
        @_ssl_server_connections = {}
      end

      def configure(conf)
        super
      end

      def start
        super
        OpenSSL::Random.seed(SecureRandom.random_bytes(16))
      end

      def ssl_server_listen_impl(bind, port, ctx, keepalive, linger_timeout, backlog, read_length, read_interval, socket_restart_interval, &block)
        register_new_connection = ->(conn){ @_ssl_server_connections[conn] = conn }

        timer_execute(interval: SSL_SERVER_KEEPALIVE_CHECK_INTERVAL, repeat: true) do
          # copy keys at first to delete it in loop
          @_ssl_server_connections.keys.each do |conn|
            if !conn.writing && keepalive && conn.idle_seconds > keepalive
              @_ssl_server_connections.delete(conn)
              conn.close
            elsif conn.closed?
              @_ssl_server_connections.delete(conn)
            else
              conn.idle_seconds += SSL_SERVER_KEEPALIVE_CHECK_INTERVAL
            end
          end
        end

        listener_thread = thread_create do
          server = ::TCPServer.new(bind, port)
          if backlog
            server.listen(backlog)
          end
          sock = OpenSSL::SSL::SSLServer.new(server, ctx)
          # delay SSL session establishment not to do it when sock.accept
          # SSL session establishment is heavy, and it should be executed on child thread
          sock.start_immediately = false

          @_ssl_server_listen_servers << server
          @_ssl_server_listen_socks << sock

          socket_listener_listen('tcp', bind, port)
          while socket = sock.accept
            if linger_timeout
              opt = [1, linger_timeout].pack('I!I!')  # { int l_onoff; int l_linger; }
              socket.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_LINGER, opt)
            end
            socket.sync = true
            socket.sync_close = true
            thread_create(socket) do |socket|
              running_check = ->(){ thread_current_running? }
              conn = Handler.new(socket, register_new_connection, running_check, read_length, read_interval, socket_restart_interval, block)
              @_ssl_server_connections[conn] = conn
              conn.run
            end
          end
        end

        @_ssl_server_listen_threads << listener_thread
      end

      def stop
        super
      end

      def shutdown
        super

        # listen threads are blocking on accept. So killing them is an only way to stop it
        @_ssl_server_listen_threads.each do |thread|
          thread.kill
          thread.join
        end
        @_ssl_server_listen_socks.each do |sock|
          sock.close
        end
        @_ssl_server_listen_servers.each do |server|
          server.close unless server.closed?
        end
      end

      def close
        @_ssl_server_connections.keys.each do |conn|
          conn.close
        end

        super
      end

      def terminate
        @_ssl_server_listen_servers = []
        @_ssl_server_listen_socks = []
        @_ssl_server_listen_threads = []
        @_ssl_server_connections = {}

        super
      end

      class Handler
        attr_accessor :idle_seconds, :closing
        attr_reader :protocol, :remote_port, :remote_addr, :remote_host

        PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]

        def initialize(io, register, running_check, read_length, read_interval, socket_restart_interval, on_connect_callback)
          @io = io

          register.call(self)

          @running_check = running_check
          @read_length = read_length
          @read_interval = read_interval
          @socket_restart_interval = socket_restart_interval

          @on_connect_callback = on_connect_callback
          @on_read_callback = nil

          @buffer = nil # for on_data with delimiter

          @idle_seconds = 0
          @closing = false
          @writing = false
        end

        def run
          socket = @io

          begin
            socket.accept
          rescue OpenSSL::SSL::SSLError => e
            # TODO: log
            self.close rescue nil
            return
          end

          ### TODO: disabling name rev resolv
          proto, port, host, addr = ( socket.peeraddr rescue PEERADDR_FAILED )
          if addr == '?'
            port, addr = *Socket.unpack_sockaddr_in(socket.getpeername) rescue nil
          end
          @protocol = proto
          @remote_port = port
          @remote_addr = addr
          @remote_host = host

          @on_connect_callback.call(self)
          raise "BUG: register on_data callback" unless @on_read_callback

          @idle_seconds = 0
          buf = ''
          loop do
            begin
              while socket.read_nonblock(@read_length, buf)
                if buf.empty?
                  unless @running_check.call()
                    break
                  end
                  @idle_seconds += @read_interval
                  sleep @read_interval
                  next
                end
                p({sslserver: buf}) if $json_ssl
                @on_read_callback.call(buf)
                buf = ''
              end
            rescue OpenSSL::SSL::SSLError => e
              if $json_ssl
                p e
                p({error: e, buf: buf})
              end
              sleep @socket_restart_interval
            rescue EOFError => e
              # TODO: log
              break
            rescue IOError => e
              break if e.message == 'closed stream'
            end
          end
        rescue Errno::ECONNRESET => e
          p({econnreset: buf, error: e}) if $json_ssl
          # disconnected from client
          # TODO: log
        ensure
          self.close rescue nil
        end

        # API to register callback for data arrival
        def on_data(delimiter: nil, &callback)
          if delimiter.nil?
            @on_read_callback = callback
          else # buffering and splitting
            @buffer = "".force_encoding("ASCII-8BIT")
            @on_read_callback = ->(data) {
              @buffer << data
              pos = 0
              while i = @buffer.index(delimiter, pos)
                msg = @buffer[pos...i]
                callback.call(msg)
                pos = i + delimiter.length
              end
              @buffer.slice!(0, pos) if pos > 0
            }
          end
        end

        def write(data)
          @writing = true

          @io.write data

          @writing = false
          if @closing
            close
          end
        end

        def close
          @closing = true
          unless @writing
            @io.close
          end
        end
      end
    end
  end
end
