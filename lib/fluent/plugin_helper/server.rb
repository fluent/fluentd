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

require 'fluent/plugin_helper/event_loop'

require 'serverengine/socket_manager'
require 'cool.io'
require 'socket'
require 'ipaddr'
require 'fcntl'

module Fluent
  module PluginHelper
    module Server
      include Fluent::PluginHelper::EventLoop

      # This plugin helper doesn't support these things for now:
      # * SSL/TLS (TBD)
      # * IPv6
      # * TCP/TLS keepalive

      # stop     : [-]
      # shutdown : detach server event handler from event loop (event_loop)
      # close    : close listening sockets
      # terminate: remote all server instances

      attr_reader :_servers # for tests

      def server_wait_until_start
        # event_loop_wait_until_start works well for this
      end

      def server_wait_until_stop
        sleep 0.1 while @_servers.any?{|si| si.server.attached? }
        @_servers.each{|si| si.server.close rescue nil }
      end

      PROTOCOLS = [:tcp, :udp, :tls, :unix]
      CONNECTION_PROTOCOLS = [:tcp, :tls, :unix]

      # server_create_connection(:title, @port) do |conn|
      #   # on connection
      #   source_addr = conn.remote_host
      #   source_port = conn.remote_port
      #   conn.data do |data|
      #     # on data
      #     conn.write resp # ...
      #     conn.close
      #   end
      # end
      def server_create_connection(title, port, proto: :tcp, bind: '0.0.0.0', shared: true, certopts: nil, resolve_name: false, linger_timeout: 0, backlog: nil, &block)
        raise ArgumentError, "BUG: title must be a symbol" unless title && title.is_a?(Symbol)
        raise ArgumentError, "BUG: port must be an integer" unless port && port.is_a?(Integer)
        raise ArgumentError, "BUG: invalid protocol name" unless PROTOCOLS.include?(proto)
        raise ArgumentError, "BUG: cannot create connection for UDP" unless CONNECTION_PROTOCOLS.include?(proto)

        raise ArgumentError, "BUG: block not specified which handles connection" unless block_given?
        raise ArgumentError, "BUG: block must have just one argument" unless block.arity == 1

        if proto != :tls # TLS options
          raise ArgumentError, "BUG: certopts is available only for tls" if certopts
        end
        if proto != :tcp && proto != :tls # TCP/TLS options
          raise ArgumentError, "BUG: linger_timeout is available for tcp/tls" if linger_timeout != 0
        end

        case proto
        when :tcp
          server = server_create_for_tcp_connection(shared, bind, port, resolve_name, linger_timeout, backlog, &block)
        when :tls
          raise ArgumentError, "BUG: certopts (certificate options) not specified for TLS" unless certopts
          # server_certopts_validate!(certopts)
          # sock = server_create_tls_socket(shared, bind, port)
          # server = nil # ...
          raise "not implemented yet"
        when :unix
          raise "not implemented yet"
        else
          raise "unknown protocol #{proto}"
        end

        server_attach(title, proto, port, bind, shared, server)
      end

      # server_create(:title, @port) do |data|
      #   # ...
      # end
      # server_create(:title, @port) do |data, conn|
      #   # ...
      # end
      # server_create(:title, @port, proto: :udp, max_bytes: 2048) do |data, sock|
      #   sock.remote_host
      #   sock.remote_port
      #   # ...
      # end
      def server_create(title, port, proto: :tcp, bind: '0.0.0.0', shared: true, certopts: nil, resolve_name: false, linger_timeout: 0, backlog: nil, max_bytes: nil, flags: 0, &callback)
        raise ArgumentError, "BUG: title must be a symbol" unless title && title.is_a?(Symbol)
        raise ArgumentError, "BUG: port must be an integer" unless port && port.is_a?(Integer)
        raise ArgumentError, "BUG: invalid protocol name" unless PROTOCOLS.include?(proto)

        raise ArgumentError, "BUG: block not specified which handles received data" unless block_given?
        raise ArgumentError, "BUG: block must have 1 or 2 arguments" unless callback.arity == 1 || callback.arity == 2

        if proto != :tls # TLS options
          raise ArgumentError, "BUG: certopts is available only for tls" if certopts
        end
        if proto != :tcp && proto != :tls # TCP/TLS options
          raise ArgumentError, "BUG: linger_timeout is available for tcp/tls" if linger_timeout != 0
        end
        if proto != :tcp && proto != :tls && proto != :unix # options to listen/accept connections
          raise ArgumentError, "BUG: backlog is available for tcp/tls" if backlog
        end
        if proto != :udp # UDP options
          raise ArgumentError, "BUG: max_bytes is available only for udp" if max_bytes
          raise ArgumentError, "BUG: flags is available only for udp" if flags != 0
        end

        case proto
        when :tcp
          STDERR.puts "#{__LINE__}: running..."
          server = server_create_for_tcp_connection(shared, bind, port, resolve_name, linger_timeout, backlog) do |conn|
            STDERR.puts "#{__LINE__}: #{conn} running..."
            conn.data(&callback)
          end
        when :tls
          raise ArgumentError, "BUG: certopts (certificate options) not specified for TLS" unless certopts
          server_certopts_validate!(certopts)
          raise "not implemented yet"
        when :udp
          raise ArgumentError, "BUG: max_bytes must be specified for UDP" unless max_bytes
          sock = server_create_udp_socket(shared, bind, port)
          sock.do_not_reverse_lookup = !resolve_name
          server = EventHandler::UDPServer.new(sock, resolve_name, max_bytes, flags, @log, @under_plugin_development, &callback)
        when :unix
          raise "not implemented yet"
        else
          raise "BUG: unknown protocol #{proto}"
        end

        server_attach(title, proto, port, bind, shared, server)
      end

      def server_create_tcp(title, port, **kwargs, &callback)
        server_create(title, port, proto: :tcp, **kwargs, &callback)
      end

      def server_create_udp(title, port, **kwargs, &callback)
        server_create(title, port, proto: :udp, **kwargs, &callback)
      end

      def server_create_tls(title, port, **kwargs, &callback)
        server_create(title, port, proto: :tls, **kwargs, &callback)
      end

      def server_create_unix(title, port, **kwargs, &callback)
        server_create(title, port, proto: :unix, **kwargs, &callback)
      end

      ServerInfo = Struct.new(:title, :proto, :port, :bind, :shared, :server)

      def server_attach(title, proto, port, bind, shared, server)
        @_servers << ServerInfo.new(title, proto, port, bind, shared, server)
        event_loop_attach(server)
      end

      def server_create_for_tcp_connection(shared, bind, port, resolve_name, linger_timeout, backlog, &block)
        STDERR.puts "#{__LINE__}: running..."
        sock = server_create_tcp_socket(shared, bind, port)
        STDERR.puts "#{__LINE__}: running..."
        sock.do_not_reverse_lookup = !resolve_name
        STDERR.puts "#{__LINE__}: running..."
        close_callback = ->(conn){ @_server_mutex.synchronize{ @_server_connections.delete(conn) } }
        STDERR.puts "#{__LINE__}: running..."
        server = Coolio::TCPServer.new(sock, nil, EventHandler::TCPServer, close_callback, resolve_name, linger_timeout, @log, @under_plugin_development, block) do |conn|
          STDERR.puts "#{__LINE__}: running..."
          @_server_mutex.synchronize do
            @_server_connections << conn
          end
        end
        STDERR.puts "#{__LINE__}: running..."
        server.listen(backlog) if backlog
        server
      end

      def initialize
        super
        @_servers = []
        @_server_connections = []
        @_server_mutex = Mutex.new
      end

      def shutdown
        @_server_connections.each do |conn|
          conn.close rescue nil
        end
        @_server_mutex.synchronize do
          @_servers.each do |si|
            si.server.detach if si.server.attached?
          end
        end

        super
      end

      def close
        @_server_connections.each do |conn|
          conn.close rescue nil
        end
        @_server_mutex.synchronize do
          @_servers.each do |si|
            si.server.close rescue nil
          end
        end
        super
      end

      def terminate
        @_servers = []
        super
      end

      def server_certopts_validate!(certopts)
        raise "not implemented yet"
      end

      def server_socket_manager_client
        socket_manager_path = ENV['SERVERENGINE_SOCKETMANAGER_PATH']
        if Fluent.windows?
          socket_manager_path = socket_manager_path.to_i
        end
        ServerEngine::SocketManager::Client.new(socket_manager_path)
      end

      def server_create_tcp_socket(shared, bind, port)
        STDERR.puts "#{__LINE__}: running..."
        sock = if shared
        STDERR.puts "#{__LINE__}: running..."
                 server_socket_manager_client.listen_tcp(bind, port)
               else
        STDERR.puts "#{__LINE__}: running..."
                 TCPServer.new(bind, port) # this method call can create sockets for AF_INET6
               end
        STDERR.puts "#{__LINE__}: running..."
        # close-on-exec is set by default in Ruby 2.0 or later (, and it's unavailable on Windows)
        sock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK) # nonblock
        STDERR.puts "#{__LINE__}: running..."
        sock
      end

      def server_create_udp_socket(shared, bind, port)
        sock = if shared
                 server_socket_manager_client.listen_udp(bind, port)
               else
                 family = IPAddr.new(IPSocket.getaddress(bind)).ipv4? ? ::Socket::AF_INET : ::Socket::AF_INET6
                 usock = UDPSocket.new(family)
                 usock.bind(bind, port)
                 usock
               end
        # close-on-exec is set by default in Ruby 2.0 or later (, and it's unavailable on Windows)
        sock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK) # nonblock
        sock
      end

      def server_create_tls_socket(shared, bind, port)
        raise "not implemented yet"
      end

      class CallbackSocket
        def initialize(server_type, sock, enabled_events = [])
          @server_type = server_type
          @sock = sock
          @enabled_events = enabled_events
        end

        def remote_addr
          @sock.peeraddr[3]
        end

        def remote_host
          @sock.peeraddr[2]
        end

        def remote_port
          @sock.peeraddr[1]
        end

        def send(data, flags = 0)
          @sock.send(data, flags)
        end

        def write(data)
          raise "not implemented here"
        end

        def close
          @sock.close
          # close cool.io socket in another thread, not to make deadlock
          # for flushing @_write_buffer when conn.close is called in callback
          # ::Thread.new{
          #   @sock.close
          # }
        end

        def data(&callback)
          on(:data, &callback)
        end

        def on(event, &callback)
          raise "BUG: this event is disabled for #{@server_type}: #{event}" unless @enabled_events.include?(event)
          case event
          when :data
            @sock.data(&callback)
          when :write_complete
            cb = ->(){
              callback.call(self)
            }
            @sock.on_write_complete(&cb)
          when :close
            cb = ->(){ callback.call(self) }
            @sock.on_close(&cb)
          else
            raise "BUG: unknown event: #{event}"
          end
        end
      end

      class TCPCallbackSocket < CallbackSocket
        def initialize(sock)
          super("tcp", sock, [:data, :write_complete, :close])
        end

        def write(data)
          @sock.write(data)
        end
      end

      class UDPCallbackSocket < CallbackSocket
        def initialize(sock, peeraddr)
          super("udp", sock, [])
          @peeraddr = peeraddr
        end

        def remote_addr
          @peeraddr[3]
        end

        def remote_host
          @peeraddr[2]
        end

        def remote_port
          @peeraddr[1]
        end

        def write(data)
          @sock.send(data, 0, @peeraddr[3], @peeraddr[1])
        end
      end

      module EventHandler
        class UDPServer < Coolio::IO
          def initialize(sock, resolve_name, max_bytes, flags, log, under_plugin_development, &callback)
            raise ArgumentError, "socket must be a UDPSocket: sock = #{sock}" unless sock.is_a?(UDPSocket)

            super(sock)

            @sock = sock
            @resolve_name = resolve_name
            @max_bytes = max_bytes
            @flags = flags
            @log = log
            @under_plugin_development = under_plugin_development
            @callback = callback

            @sock.do_not_reverse_lookup = !resolve_name
            on_readable_impl = case @callback.arity
                               when 1 then :on_readable_without_sock
                               when 2 then :on_readable_with_sock
                               else
                                 raise "BUG: callback block must have 1 or 2 arguments"
                               end
            self.define_singleton_method(:on_readable, method(on_readable_impl))
          end

          def on_readable_without_sock
            begin
              data = @sock.recv(@max_bytes, @flags)
            rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
              return
            end
            @callback.call(data)
          rescue => e
            @log.error "unexpected error in processing UDP data", error: e
            @log.error_backtrace
            raise if @under_plugin_development
          end

          def on_readable_with_sock
            begin
              data, addr = @sock.recvfrom(@max_bytes)
            rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
              return
            end
            @callback.call(data, UDPCallbackSocket.new(@sock, addr))
          rescue => e
            @log.error "unexpected error in processing UDP data", error: e
            @log.error_backtrace
            raise if @under_plugin_development
          end
        end

        class TCPServer < Coolio::TCPSocket
          SOCK_OPT_FORMAT = 'I!I!' # { int l_onoff; int l_linger; }

          def initialize(sock, close_callback, resolve_name, linger_timeout, log, under_plugin_development, connect_callback)
            raise ArgumentError, "socket must be a TCPSocket" unless sock.is_a?(TCPSocket)

            sock_opt = [1, linger_timeout].pack(SOCK_OPT_FORMAT)
            sock.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_LINGER, sock_opt)
            sock.do_not_reverse_lookup = !resolve_name

            @_handler_socket = sock
            super(sock)

            @log = log
            @under_plugin_development = under_plugin_development

            @connect_callback = connect_callback
            @data_callback = nil
            @close_callback = close_callback

            @callback_connection = nil
            @closing = false

            @mutex = Mutex.new # to serialize #write and #close
          end

          def data(&callback)
            raise "data callback can be registered just once, but registered twice" if self.singleton_methods.include?(:on_read)
            @data_callback = callback
            on_read_impl = case callback.arity
                           when 1 then :on_read_without_connection
                           when 2 then :on_read_with_connection
                           else
                             raise "BUG: callback block must have 1 or 2 arguments"
                           end
            self.define_singleton_method(:on_read, method(on_read_impl))
          end

          def write(data)
            @mutex.synchronize do
              super
            end
          end

          def on_connect
            @callback_connection = TCPCallbackSocket.new(self)
            @connect_callback.call(@callback_connection)
            unless @data_callback
              raise "connection callback must call #data to set data callback"
            end
          end

          def on_read_without_connection(data)
            @data_callback.call(data)
          rescue => e
            @log.error "unexpected error on reading data", host: remote_host, port: remote_port, error: e
            @log.error_backtrace
            close(true) rescue nil
            raise if @under_plugin_development
          end

          def on_read_with_connection(data)
            @data_callback.call(data, @callback_connection)
          rescue => e
            @log.error "unexpected error on reading data", host: remote_host, port: remote_port, error: e
            @log.error_backtrace
            close(true) rescue nil
            raise if @under_plugin_development
          end

          def close
            @mutex.synchronize do
              return if @closing
              @closing = true
              @close_callback.call(self)
              super
            end
          end
        end
      end
    end
  end
end
