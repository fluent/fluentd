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

require 'fluent/plugin_support/timer'

require 'ipaddr'
require 'socket'
require 'openssl'
require 'digest'
require 'securerandom'

module Fluent
  module PluginSupport
    module SSLServer
      include Fluent::PluginSupport::Thread
      include Fluent::PluginSupport::Timer

      SSL_SERVER_KEEPALIVE_CHECK_INTERVAL = 1
      SSL_SERVER_DEFAULT_READ_LENGTH = 8 * 1024 * 1024
      SSL_SERVER_DEFAULT_READ_INTERVAL = 0.05 # [s], 50ms
      SSL_SERVER_DEFAULT_SOCKET_RESTART_INTERVAL = 0.2 # [s], 200ms

      def ssl_server_generate_cert_key(digest: OpenSSL::Digest::SHA256, algorithm: OpenSSL::PKey::RSA, key_length: 2048, cert_country: 'US', cert_state: 'CA', cert_locality: 'Mountain View', cert_common_name: "Fluentd #{self.class} SSLServer")
        key = algorithm.generate(key_length)

        digest = digest_method.new
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
        cert.sign(key, digest)

        return cert, key
      end

      def ssl_server_load_cert_key(cert_file_path: nil, algorithm: OpenSSL::PKey::RSA, key_file_path: nil, key_passphrase: nil)
        # TODO: remove argument check if issue #563 resolved https://github.com/fluent/fluentd/issues/563
        raise "BUG: cert_file_path is not specified for ssl_server_load_cert_key" unless cert_file_path
        raise "BUG: key_file_path is not specified for ssl_server_load_cert_key" unless key_file_path
        raise "BUG: key_passphrase is not specified for ssl_server_load_cert_key" unless key_passphrase

        cert = OpenSSL::X509::Certificate.new(File.read(cert_file_path))
        key = algorithm.new(File.read(key_file_path), key_passphrase)
        return cert, key
      end

      # keepalive: seconds, (default: nil [inf])
      def ssl_server_listen(ssl_version: :TLSv1_2, ciphers: nil, cert: nil, key: nil, port: nil, bind: '0.0.0.0', keepalive: nil, read_length: SSL_SERVER_DEFAULT_READ_LENGTH, read_interval: SSL_SERVER_KEEPALIVE_CHECK_INTERVAL, socket_restart_interval: SSL_SERVER_DEFAULT_SOCKET_RESTART_INTERVAL, &block)
        raise "BUG: callback block is not specified for ssl_server_listen" unless block_given?

        # TODO: remove argument check if issue #563 resolved https://github.com/fluent/fluentd/issues/563
        raise "BUG: specify port for ssl_server_listen" unless port
        raise "BUG: specify cert & key for ssl_server_listen" unless cert && key

        raise "BUG: specified SSL/TLS version '#{ssl_method}' is not supported in this environment" unless OpenSSL::SSL::SSLContext::METHODS.include?(ssl_version)

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

        server = TCPServer.new(bind, port)
        sock = OpenSSL::SSL::SSLServer.new(server, ctx)
        # delay SSL session establishment not to do it when sock.accept
        # SSL session establishment is heavy, and it should be executed on child thread
        sock.start_immediately = false

        if self.respond_to?(:detach_multi_process)
          detach_multi_process do
            ssl_server_listen_impl(sock, keepalive, read_length, read_interval, socket_restart_interval, &block)
          end
        elsif self.respond_to?(:detach_process)
          detach_process do
            ssl_server_listen_impl(sock, keepalive, read_length, read_interval, socket_restart_interval, &block)
          end
        else
          ssl_server_listen_impl(sock, keepalive, read_length, read_interval, socket_restart_interval, &block)
        end
      end

      def initialize
        super
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

      def ssl_server_listen_impl(sock, keepalive, read_length, read_interval, socket_restart_interval, &block)
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
          while socket = sock.accept
            thread_create do
              running_check = ->(){ thread_current_running? }
              conn = Handler.new(socket, register, running_check, read_length, read_interval, socket_restart_interval, block)
              @_ssl_server_connections[conn] = conn
              conn.run
            end
          end
        end

        @_ssl_server_listen_socks << sock
        @_ssl_server_listen_threads << listener_thread
      end

      def stop
        super
      end

      def shutdown
        @_ssl_server_listen_threads.each do |thread|
          thread.kill
          thread.join
        end
        @_ssl_server_connections.keys.each do |conn|
          conn.shutdown
        end

        super
      end

      def close
        @_ssl_server_listen_socks.each do |sock|
          sock.close
        end
        @_ssl_server_connections.keys.each do |conn|
          conn.close
        end

        super
      end

      def terminate
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
            socket.sync = true
            puts "[#{i}:#{Thread.current.object_id}] accept in thread"
            socket.accept
            buf = ''

            ### TODO: disabling name rev resolv
            proto, port, host, addr = ( io.peeraddr rescue PEERADDR_FAILED )
            @protocol = proto
            @remote_port = port
            @remote_addr = addr
            @remote_host = host
            ## which is better?
            # @remote_port, @remote_addr = *Socket.unpack_sockaddr_in(io.getpeername) rescue nil

            ### TODO: socket option
            #
            # PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]
            # opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
            # io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

            @on_connect_callback.call(self)
            raise "BUG: register on_data callback" unless @on_read_callback

            @idle_seconds = 0
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
                  @on_read_callback.call(buf)
                  buf = ''
                end
              rescue OpenSSL::SSL::SSLError => e
                sleep @socket_restart_interval
              end
            end
            @io.close
          end
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
