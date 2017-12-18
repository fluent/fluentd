require_relative '../helper'
require 'fluent/plugin_helper/server'
require 'fluent/plugin_helper/cert_option' # to create certs for tests
require 'fluent/plugin/base'
require 'timeout'

require 'serverengine'
require 'fileutils'

class ServerPluginHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :server
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/plugin_helper_server")

  PORT = unused_port

  setup do
    @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
    if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
    @socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
    ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s

    @d = Dummy.new
    @d.start
    @d.after_start
  end

  teardown do
    (@d.stopped? || @d.stop) rescue nil
    (@d.before_shutdown? || @d.before_shutdown) rescue nil
    (@d.shutdown? || @d.shutdown) rescue nil
    (@d.after_shutdown? || @d.after_shutdown) rescue nil
    (@d.closed? || @d.close) rescue nil
    (@d.terminated? || @d.terminate) rescue nil

    @socket_manager_server.close
    if @socket_manager_server.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
  end

  sub_test_case 'plugin instance' do
    test 'can be instantiated to be able to create threads' do
      d = Dummy.new
      assert d.respond_to?(:_servers)
      assert d._servers.empty?

      assert d.respond_to?(:server_wait_until_start)
      assert d.respond_to?(:server_wait_until_stop)
      assert d.respond_to?(:server_create_connection)
      assert d.respond_to?(:server_create)
      assert d.respond_to?(:server_create_tcp)
      assert d.respond_to?(:server_create_udp)
      assert d.respond_to?(:server_create_tls)
    end

    test 'can be configured' do
      d = Dummy.new
      assert_nothing_raised do
        d.configure(config_element())
      end
      assert d.plugin_id
      assert d.log
    end
  end

  # run tests for tcp, udp, tls and unix
  sub_test_case '#server_create and #server_create_connection' do
    methods = {server_create: :server_create, server_create_connection: :server_create_connection}

    data(methods)
    test 'raise error if title is not specified or not a symbol' do |m|
      assert_raise(ArgumentError.new("BUG: title must be a symbol")) do
        @d.__send__(m, nil, PORT){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: title must be a symbol")) do
        @d.__send__(m, "", PORT){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: title must be a symbol")) do
        @d.__send__(m, "title", PORT){|x| x }
      end
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT){|x| x }
      end
    end

    data(methods)
    test 'raise error if port is not specified or not an integer' do |m|
      assert_raise(ArgumentError.new("BUG: port must be an integer")) do
        @d.__send__(m, :myserver, nil){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: port must be an integer")) do
        @d.__send__(m, :myserver, "1"){|x| x }
      end
      assert_raise(ArgumentError.new("BUG: port must be an integer")) do
        @d.__send__(m, :myserver, 1.5){|x| x }
      end
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT){|x| x }
      end
    end

    data(methods)
    test 'raise error if block is not specified' do |m|
      assert_raise(ArgumentError) do
        @d.__send__(m, :myserver, PORT)
      end
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT){|x| x }
      end
    end

    data(methods)
    test 'creates tcp server, binds 0.0.0.0 in default' do |m|
      @d.__send__(m, :myserver, PORT){|x| x }

      assert_equal 1, @d._servers.size

      created_server_info = @d._servers.first

      assert_equal :myserver, created_server_info.title
      assert_equal PORT, created_server_info.port

      assert_equal :tcp, created_server_info.proto
      assert_equal "0.0.0.0", created_server_info.bind

      created_server = created_server_info.server

      assert created_server.is_a?(Coolio::TCPServer)
      assert_equal "0.0.0.0", created_server.instance_eval{ @listen_socket }.addr[3]
    end

    data(methods)
    test 'creates tcp server if specified in proto' do |m|
      @d.__send__(m, :myserver, PORT, proto: :tcp){|x| x }

      created_server_info = @d._servers.first
      assert_equal :tcp, created_server_info.proto
      created_server = created_server_info.server
      assert created_server.is_a?(Coolio::TCPServer)
    end

    data(methods)
    test 'creates tls server in default if transport section and tcp protocol specified' do |m|
      @d = d = Dummy.new
      transport_conf = config_element('transport', 'tcp', {}, [])
      d.configure(config_element('ROOT', '', {}, [transport_conf]))
      d.start
      d.after_start

      d.__send__(m, :myserver, PORT){|x| x }

      created_server_info = @d._servers.first
      assert_equal :tcp, created_server_info.proto
      created_server = created_server_info.server
      assert created_server.is_a?(Coolio::TCPServer)
    end

    data(methods)
    test 'creates tls server if specified in proto' do |m|
      assert_raise(ArgumentError.new("BUG: TLS transport specified, but certification options are not specified")) do
        @d.__send__(m, :myserver, PORT, proto: :tls){|x| x }
      end
      @d.__send__(m, :myserver, PORT, proto: :tls, tls_options: {insecure: true}){|x| x }

      created_server_info = @d._servers.first
      assert_equal :tls, created_server_info.proto
      created_server = created_server_info.server
      assert created_server.is_a?(Coolio::TCPServer) # yes, TCP here
    end

    data(methods)
    test 'creates tls server in default if transport section and tls protocol specified' do |m|
      @d = d = Dummy.new
      transport_conf = config_element('transport', 'tls', {'insecure' => 'true'}, [])
      d.configure(config_element('ROOT', '', {}, [transport_conf]))
      d.start
      d.after_start

      d.__send__(m, :myserver, PORT){|x| x }

      created_server_info = @d._servers.first
      assert_equal :tls, created_server_info.proto
      created_server = created_server_info.server
      assert created_server.is_a?(Coolio::TCPServer) # OK, it's Coolio::TCPServer
    end

    data(methods)
    test 'creates unix server if specified in proto' do |m|
      # pend "not implemented yet"
    end

    data(methods)
    test 'raise error if unknown protocol specified' do |m|
      assert_raise(ArgumentError.new("BUG: invalid protocol name")) do
        @d.__send__(m, :myserver, PORT, proto: :quic){|x| x }
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp],
      'server_create tls' => [:server_create, :tls],
      # 'server_create unix' => [:server_create, :unix],
      'server_create_connection tcp' => [:server_create_connection, :tcp],
      'server_create_connection tls' => [:server_create_connection, :tls],
      # 'server_create_connection tcp' => [:server_create_connection, :unix],
    )
    test 'raise error if udp options specified for tcp/tls/unix' do |(m, proto)|
      assert_raise ArgumentError do
        @d.__send__(m, :myserver, PORT, proto: proto, max_bytes: 128){|x| x }
      end
      assert_raise ArgumentError do
        @d.__send__(m, :myserver, PORT, proto: proto, flags: 1){|x| x }
      end
    end

    data(
      'server_create udp' => [:server_create, :udp],
    )
    test 'raise error if tcp/tls options specified for udp' do |(m, proto)|
      assert_raise(ArgumentError.new("BUG: linger_timeout is available for tcp/tls")) do
        @d.__send__(m, :myserver, PORT, proto: proto, linger_timeout: 1, max_bytes: 128){|x| x }
      end
    end

    data(
      'server_create udp' => [:server_create, :udp],
    )
    test 'raise error if tcp/tls/unix options specified for udp' do |(m, proto)|
      assert_raise(ArgumentError.new("BUG: backlog is available for tcp/tls")) do
        @d.__send__(m, :myserver, PORT, proto: proto, backlog: 500){|x| x }
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create_connection, :tcp, {}],
      # 'server_create_connection unix' => [:server_create_connection, :unix, {}],
    )
    test 'raise error if tls options specified for tcp/udp/unix' do |(m, proto, kwargs)|
      assert_raise(ArgumentError.new("BUG: tls_options is available only for tls")) do
        @d.__send__(m, :myserver, PORT, proto: proto, tls_options: {}, **kwargs){|x| x }
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      'server_create tls' => [:server_create, :tls, {tls_options: {insecure: true}}],
      'server_create_connection tcp' => [:server_create_connection, :tcp, {}],
      'server_create_connection tls' => [:server_create_connection, :tls, {tls_options: {insecure: true}}],
    )
    test 'can bind specified IPv4 address' do |(m, proto, kwargs)|
      @d.__send__(m, :myserver, PORT, proto: proto, bind: "127.0.0.1", **kwargs){|x| x }
      assert_equal "127.0.0.1", @d._servers.first.bind
      assert_equal "127.0.0.1", @d._servers.first.server.instance_eval{ instance_variable_defined?(:@listen_socket) ? @listen_socket : @_io }.addr[3]
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      'server_create tls' => [:server_create, :tls, {tls_options: {insecure: true}}],
      'server_create_connection tcp' => [:server_create_connection, :tcp, {}],
      'server_create_connection tls' => [:server_create_connection, :tls, {tls_options: {insecure: true}}],
    )
    test 'can bind specified IPv6 address' do |(m, proto, kwargs)| # if available
      omit "IPv6 unavailable here" unless ipv6_enabled?
      @d.__send__(m, :myserver, PORT, proto: proto, bind: "::1", **kwargs){|x| x }
      assert_equal "::1", @d._servers.first.bind
      assert_equal "::1", @d._servers.first.server.instance_eval{ instance_variable_defined?(:@listen_socket) ? @listen_socket : @_io }.addr[3]
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      'server_create tls' => [:server_create, :tls, {tls_options: {insecure: true}}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create, :tcp, {}],
      'server_create_connection tls' => [:server_create, :tls, {tls_options: {insecure: true}}],
      # 'server_create_connection unix' => [:server_create, :unix, {}],
    )
    test 'can create 2 or more servers which share same bind address and port if shared option is true' do |(m, proto, kwargs)|
      begin
        d2 = Dummy.new; d2.start; d2.after_start

        assert_nothing_raised do
          @d.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
          d2.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
        end
      ensure
        d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
      end
    end

    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      'server_create tls' => [:server_create, :tls, {tls_options: {insecure: true}}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create, :tcp, {}],
      'server_create_connection tls' => [:server_create, :tls, {tls_options: {insecure: true}}],
      # 'server_create_connection unix' => [:server_create, :unix, {}],
    )
    test 'cannot create 2 or more servers using same bind address and port if shared option is false' do |(m, proto, kwargs)|
      begin
        d2 = Dummy.new; d2.start; d2.after_start

        assert_nothing_raised do
          @d.__send__(m, :myserver, PORT, proto: proto, shared: false, **kwargs){|x| x }
        end
        assert_raise(Errno::EADDRINUSE, Errno::EACCES) do
          d2.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
        end
      ensure
        d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
      end
    end
  end

  sub_test_case '#server_create' do
    data(
      'tcp' => [:tcp, {}],
      'udp' => [:udp, {max_bytes: 128}],
      'tls' => [:tls, {tls_options: {insecure: true}}],
      # 'unix' => [:unix, {}],
    )
    test 'raise error if block argument is not specified or too many' do |(proto, kwargs)|
      assert_raise(ArgumentError.new("BUG: block must have 1 or 2 arguments")) do
        @d.server_create(:myserver, PORT, proto: proto, **kwargs){ 1 }
      end
      assert_raise(ArgumentError.new("BUG: block must have 1 or 2 arguments")) do
        @d.server_create(:myserver, PORT, proto: proto, **kwargs){|sock, conn, what_is_this| 1 }
      end
    end

    test 'creates udp server if specified in proto' do
      @d.server_create(:myserver, PORT, proto: :udp, max_bytes: 512){|x| x }

      created_server_info = @d._servers.first
      assert_equal :udp, created_server_info.proto
      created_server = created_server_info.server
      assert created_server.is_a?(Fluent::PluginHelper::Server::EventHandler::UDPServer)
    end
  end

  sub_test_case '#server_create_tcp' do
    test 'can accept all keyword arguments valid for tcp server' do
      assert_nothing_raised do
        @d.server_create_tcp(:s, PORT, bind: '127.0.0.1', shared: false, resolve_name: true, linger_timeout: 10, backlog: 500) do |data, conn|
          # ...
        end
      end
    end

    test 'creates a tcp server just to read data' do
      received = ""
      @d.server_create_tcp(:s, PORT) do |data|
        received << data
      end
      3.times do
        sock = TCPSocket.new("127.0.0.1", PORT)
        sock.puts "yay"
        sock.puts "foo"
        sock.close
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
    end

    test 'creates a tcp server to read and write data' do
      received = ""
      responses = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        received << data
        conn.write "ack\n"
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.puts "yay"
          sock.puts "foo"
          responses << sock.readline
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'creates a tcp server to read and write data using IPv6' do
      omit "IPv6 unavailable here" unless ipv6_enabled?

      received = ""
      responses = []
      @d.server_create_tcp(:s, PORT, bind: "::1") do |data, conn|
        received << data
        conn.write "ack\n"
      end
      3.times do
        TCPSocket.open("::1", PORT) do |sock|
          sock.puts "yay"
          sock.puts "foo"
          responses << sock.readline
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'does not resolve name of client address in default' do
      received = ""
      sources = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        received << data
        sources << conn.remote_host
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == "127.0.0.1" } }
    end

    test 'does resolve name of client address if resolve_name is true' do
      hostname = Socket.getnameinfo([nil, nil, nil, "127.0.0.1"])[0]

      received = ""
      sources = []
      @d.server_create_tcp(:s, PORT, resolve_name: true) do |data, conn|
        received << data
        sources << conn.remote_host
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == hostname } }
    end

    test 'can keep connections alive for tcp if keepalive specified' do
      # pend "not implemented yet"
    end

    test 'raises error if plugin registers data callback for connection object from #server_create' do
      received = ""
      errors = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        received << data
        begin
          conn.data{|d| received << d.upcase }
        rescue => e
          errors << e
        end
      end
      TCPSocket.open("127.0.0.1", PORT) do |sock|
        sock.puts "foo"
      end
      waiting(10){ sleep 0.1 until received.bytesize == 4 || errors.size == 1 }
      assert_equal "foo\n", received
      assert{ errors.size > 0 } # it might be called twice (or more) when connection was accepted, and then data arrived (or more)
      assert_equal "data callback can be registered just once, but registered twice", errors.first.message
    end

    test 'can call write_complete callback if registered' do
      buffer = ""
      lines = []
      responses = []
      response_completes = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        conn.on(:write_complete){|c| response_completes << true }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
        end
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            responses << sock.readline
          rescue EOFError, IOError, Errno::ECONNRESET
            # ignore
          end
          sock.close
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && response_completes.size == 3 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["ack\n","ack\n","ack\n"], responses
      assert_equal [true, true, true], response_completes
    end

    test 'can call close callback if registered' do
      buffer = ""
      lines = []
      callback_results = []
      @d.server_create_tcp(:s, PORT) do |data, conn|
        conn.on(:close){|c| callback_results << "closed" }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
        end
      end
      3.times do
        TCPSocket.open("127.0.0.1", PORT) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            while line = sock.readline
              if line == "ack\n"
                sock.close
              end
            end
          rescue EOFError, IOError, Errno::ECONNRESET
            # ignore
          end
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && callback_results.size == 3 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["closed", "closed", "closed"], callback_results
    end
  end

  sub_test_case '#server_create_udp' do
    test 'can accept all keyword arguments valid for udp server' do
      assert_nothing_raised do
        @d.server_create_udp(:s, PORT, bind: '127.0.0.1', shared: false, resolve_name: true, max_bytes: 100, flags: 1) do |data, conn|
          # ...
        end
      end
    end

    test 'creates a udp server just to read data' do
      received = ""
      @d.server_create_udp(:s, PORT, max_bytes: 128) do |data|
        received << data
      end
      bind_port = unused_port(protocol: :udp, bind: "127.0.0.1")
      3.times do
        sock = UDPSocket.new(Socket::AF_INET)
        sock.bind("127.0.0.1", bind_port)
        sock.connect("127.0.0.1", PORT)
        sock.puts "yay"
        sock.puts "foo"
        sock.close
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
    end

    test 'creates a udp server to read and write data' do
      received = ""
      responses = []
      @d.server_create_udp(:s, PORT, max_bytes: 128) do |data, sock|
        received << data
        sock.write "ack\n"
      end
      bind_port = unused_port
      3.times do
        begin
          sock = UDPSocket.new(Socket::AF_INET)
          sock.bind("127.0.0.1", bind_port)
          sock.connect("127.0.0.1", PORT)
          th = Thread.new do
            while true
              begin
                in_data, _addr = sock.recvfrom_nonblock(16)
                if in_data
                  responses << in_data
                  break
                end
              rescue IO::WaitReadable
                IO.select([sock])
              end
            end
            true
          end
          sock.write "yay\nfoo\n"
          th.join(5)
        ensure
          sock.close
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'creates a udp server to read and write data using IPv6' do
      omit "IPv6 unavailable here" unless ipv6_enabled?

      received = ""
      responses = []
      @d.server_create_udp(:s, PORT, bind: "::1", max_bytes: 128) do |data, sock|
        received << data
        sock.write "ack\n"
      end
      bind_port = unused_port
      3.times do
        begin
          sock = UDPSocket.new(Socket::AF_INET6)
          sock.bind("::1", bind_port)
          th = Thread.new do
            responses << sock.recv(16)
            true
          end
          sock.connect("::1", PORT)
          sock.write "yay\nfoo\n"
          th.join(5)
        ensure
          sock.close
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal "yay\nfoo\nyay\nfoo\nyay\nfoo\n", received
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'does not resolve name of client address in default' do
      received = ""
      sources = []
      @d.server_create_udp(:s, PORT, max_bytes: 128) do |data, sock|
        received << data
        sources << sock.remote_host
      end
      3.times do
        sock = UDPSocket.new(Socket::AF_INET)
        sock.connect("127.0.0.1", PORT)
        sock.puts "yay"
        sock.close
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == "127.0.0.1" } }
    end

    test 'does resolve name of client address if resolve_name is true' do
      hostname = Socket.getnameinfo([nil, nil, nil, "127.0.0.1"])[0]

      received = ""
      sources = []
      @d.server_create_udp(:s, PORT, resolve_name: true, max_bytes: 128) do |data, sock|
        received << data
        sources << sock.remote_host
      end
      3.times do
        sock = UDPSocket.new(Socket::AF_INET)
        sock.connect("127.0.0.1", PORT)
        sock.puts "yay"
        sock.close
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == hostname } }
    end

    test 'raises error if plugin registers data callback for connection object from #server_create' do
      received = ""
      errors = []
      @d.server_create_udp(:s, PORT, max_bytes: 128) do |data, sock|
        received << data
        begin
          sock.data{|d| received << d.upcase }
        rescue => e
          errors << e
        end
      end
      sock = UDPSocket.new(Socket::AF_INET)
      sock.connect("127.0.0.1", PORT)
      sock.write "foo\n"
      sock.close

      waiting(10){ sleep 0.1 until received.bytesize == 4 && errors.size == 1 }
      assert_equal "foo\n", received
      assert_equal 1, errors.size
      assert_equal "BUG: this event is disabled for udp: data", errors.first.message
    end

    test 'raise error if plugin registers write_complete callback for udp' do
      received = ""
      errors = []
      @d.server_create_udp(:s, PORT, max_bytes: 128) do |data, sock|
        received << data
        begin
          sock.on(:write_complete){|conn| "" }
        rescue => e
          errors << e
        end
      end
      sock = UDPSocket.new(Socket::AF_INET)
      sock.connect("127.0.0.1", PORT)
      sock.write "foo\n"
      sock.close

      waiting(10){ sleep 0.1 until received.bytesize == 4 && errors.size == 1 }
      assert_equal "foo\n", received
      assert_equal 1, errors.size
      assert_equal "BUG: this event is disabled for udp: write_complete", errors.first.message
    end

    test 'raises error if plugin registers close callback for udp' do
      received = ""
      errors = []
      @d.server_create_udp(:s, PORT, max_bytes: 128) do |data, sock|
        received << data
        begin
          sock.on(:close){|d| "" }
        rescue => e
          errors << e
        end
      end
      sock = UDPSocket.new(Socket::AF_INET)
      sock.connect("127.0.0.1", PORT)
      sock.write "foo\n"
      sock.close

      waiting(10){ sleep 0.1 until received.bytesize == 4 && errors.size == 1 }
      assert_equal "foo\n", received
      assert_equal 1, errors.size
      assert_equal "BUG: this event is disabled for udp: close", errors.first.message
    end
  end

  module CertUtil
    extend Fluent::PluginHelper::CertOption
  end

  def create_ca_options
    {
      private_key_length: 2048,
      country: 'US',
      state: 'CA',
      locality: 'Mountain View',
      common_name: 'ca.testing.fluentd.org',
      expiration: 30 * 86400,
      digest: :sha256,
    }
  end

  def create_server_options
    {
      private_key_length: 2048,
      country: 'US',
      state: 'CA',
      locality: 'Mountain View',
      common_name: 'server.testing.fluentd.org',
      expiration: 30 * 86400,
      digest: :sha256,
    }
  end

  def write_cert_and_key(cert_path, cert, key_path, key, passphrase)
    File.open(cert_path, "w"){|f| f.write(cert.to_pem) }
    # Write the secret key (raw or ecnrypted by AES256) in PEM format
    key_str = passphrase ? key.export(OpenSSL::Cipher.new("AES-256-CBC"), passphrase) : key.export
    File.open(key_path, "w"){|f| f.write(key_str) }
    File.chmod(0600, cert_path, key_path)
  end

  def create_server_pair_signed_by_self(cert_path, private_key_path, passphrase)
    cert, key, _ = CertUtil.cert_option_generate_server_pair_self_signed(create_server_options)
    write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
    return cert
  end

  def create_ca_pair_signed_by_self(cert_path, private_key_path, passphrase)
    cert, key, _ = CertUtil.cert_option_generate_ca_pair_self_signed(create_ca_options)
    write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
  end

  def create_server_pair_signed_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, passphrase)
    cert, key, _ = CertUtil.cert_option_generate_server_pair_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, create_server_options)
    write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
    return cert
  end

  def create_server_pair_chained_with_root_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, passphrase)
    root_cert, root_key, _ = CertUtil.cert_option_generate_ca_pair_self_signed(create_ca_options)
    write_cert_and_key(ca_cert_path, root_cert, ca_key_path, root_key, ca_key_passphrase)

    intermediate_ca_options = create_ca_options
    intermediate_ca_options[:common_name] = 'ca2.testing.fluentd.org'
    chain_cert, chain_key = CertUtil.cert_option_generate_pair(intermediate_ca_options, root_cert.subject)
    chain_cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(true)]))
    chain_cert.sign(root_key, "sha256")

    server_cert, server_key, _ = CertUtil.cert_option_generate_pair(create_server_options, chain_cert.subject)
    server_cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(false)]))
    server_cert.add_extension OpenSSL::X509::Extension.new('nsCertType', 'server')
    server_cert.sign(chain_key, "sha256")

    # write chained cert
    File.open(cert_path, "w") do |f|
      f.write server_cert.to_pem
      f.write chain_cert.to_pem
    end
    key_str = passphrase ? server_key.export(OpenSSL::Cipher.new("AES-256-CBC"), passphrase) : server_key.export
    File.open(private_key_path, "w"){|f| f.write(key_str) }
    File.chmod(0600, cert_path, private_key_path)
  end

  def open_tls_session(addr, port, verify: true, cert_path: nil, selfsigned: true, hostname: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.set_params({})
    if verify
      cert_store = OpenSSL::X509::Store.new
      cert_store.set_default_paths
      if selfsigned && OpenSSL::X509.const_defined?('V_FLAG_CHECK_SS_SIGNATURE')
        cert_store.flags = OpenSSL::X509::V_FLAG_CHECK_SS_SIGNATURE
      end
      if cert_path
        cert_store.add_file(cert_path)
      end
      context.verify_mode = OpenSSL::SSL::VERIFY_PEER
      context.cert_store = cert_store
      if !hostname && context.respond_to?(:verify_hostname=)
        context.verify_hostname = false # In test code, using hostname to be connected is very difficult
      end
    else
      context.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end

    sock = OpenSSL::SSL::SSLSocket.new(TCPSocket.new(addr, port), context)
    sock.hostname = hostname if hostname && sock.respond_to?(:hostname)
    sock.connect
    yield sock
  ensure
    sock.close rescue nil
  end

  def assert_certificate(cert, expected_extensions)
    get_extension = lambda do |oid|
      cert.extensions.detect { |e| e.oid == oid }
    end

    assert_true cert.serial > 1
    assert_equal 2, cert.version

    expected_extensions.each do |ext|
      expected_oid, expected_value = ext
      assert_equal expected_value, get_extension.call(expected_oid).value
    end
  end

  sub_test_case '#server_create_tls with various certificate options' do
    setup do
      @d = Dummy.new # to get plugin not configured/started yet

      @certs_dir = File.join(TMP_DIR, "tls_certs")
      @server_cert_dir = File.join(@certs_dir, "server")
      FileUtils.rm_rf @certs_dir
      FileUtils.mkdir_p @server_cert_dir
    end

    sub_test_case 'using tls_options arguments to specify cert options' do
      setup do
        @d.configure(config_element()); @d.start; @d.after_start
      end

      test 'create dynamic self-signed cert/key pair (without any verification from clients)' do
        # insecure
        tls_options = {
          protocol: :tls,
          version: 'TLSv1_2',
          ciphers: 'ALL:!aNULL:!eNULL:!SSLv2',
          insecure: true,
          generate_private_key_length: 2048,
          generate_cert_country: 'US',
          generate_cert_state: 'CA',
          generate_cert_locality: 'Mountain View',
          generate_cert_common_name: 'myserver.testing.fluentd.org',
          generate_cert_expiration: 10 * 365 * 86400,
          generate_cert_digest: :sha256,
        }

        received = ""
        @d.server_create_tls(:s, PORT, tls_options: tls_options) do |data, conn|
          received << data
        end
        assert_raise "" do
          open_tls_session('127.0.0.1', PORT) do |sock|
            sock.post_connection_check('myserver.testing.fluentd.org')
            # cannot connect ....
          end
        end
        open_tls_session('127.0.0.1', PORT, verify: false) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => 'yaaaaaaaaaaaaaaaaaaay',
           'without passphrase'  => nil)
      test 'load self-signed cert/key pair (files), verified from clients using cert files' do |private_key_passphrase|
        cert_path = File.join(@server_cert_dir, "cert.pem")
        private_key_path = File.join(@certs_dir, "server.key.pem")
        cert = create_server_pair_signed_by_self(cert_path, private_key_path, private_key_passphrase)

        assert_certificate(cert,[
          ['basicConstraints', 'CA:FALSE'],
          ['nsCertType', 'SSL Server']
        ])

        tls_options = {
          protocol: :tls,
          version: 'TLSv1_2',
          ciphers: 'ALL:!aNULL:!eNULL:!SSLv2',
          insecure: false,
          cert_path: cert_path,
          private_key_path: private_key_path,
        }
        tls_options[:private_key_passphrase] = private_key_passphrase if private_key_passphrase
        received = ""
        @d.server_create_tls(:s, PORT, tls_options: tls_options) do |data, conn|
          received << data
        end
        assert_raise "" do
          open_tls_session('127.0.0.1', PORT) do |sock|
            sock.post_connection_check('server.testing.fluentd.org')
            # cannot connect by failing verification without server cert
          end
        end
        open_tls_session('127.0.0.1', PORT, cert_path: cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => "fooooooooooooooooooooooooo",
           'without passphrase'  => nil)
      test 'create dynamic server cert by private CA cert file, verified from clients using CA cert file' do |ca_key_passphrase|
        ca_cert_path = File.join(@certs_dir, "ca_cert.pem")
        ca_key_path = File.join(@certs_dir, "ca.key.pem")
        create_ca_pair_signed_by_self(ca_cert_path, ca_key_path, ca_key_passphrase)

        tls_options = {
          protocol: :tls,
          version: 'TLSv1_2',
          ciphers: 'ALL:!aNULL:!eNULL:!SSLv2',
          insecure: false,
          ca_cert_path: ca_cert_path,
          ca_private_key_path: ca_key_path,
          generate_private_key_length: 2048,
        }
        tls_options[:ca_private_key_passphrase] = ca_key_passphrase if ca_key_passphrase
        received = ""
        @d.server_create_tls(:s, PORT, tls_options: tls_options) do |data, conn|
          received << data
        end
        open_tls_session('127.0.0.1', PORT, cert_path: ca_cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => ["foooooooo", "yaaaaaaaaaaaaaaaaaaay"],
           'without passphrase'  => [nil, nil])
      test 'load static server cert by private CA cert file, verified from clients using CA cert file' do |(ca_key_passphrase, private_key_passphrase)|
        ca_cert_path = File.join(@certs_dir, "ca_cert.pem")
        ca_key_path = File.join(@certs_dir, "ca.key.pem")
        create_ca_pair_signed_by_self(ca_cert_path, ca_key_path, ca_key_passphrase)

        cert_path = File.join(@server_cert_dir, "cert.pem")
        private_key_path = File.join(@certs_dir, "server.key.pem")
        cert = create_server_pair_signed_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, private_key_passphrase)

        assert_certificate(cert,[
            ['basicConstraints', 'CA:FALSE'],
            ['nsCertType', 'SSL Server'],
            ['keyUsage', 'Digital Signature, Key Encipherment'],
            ['extendedKeyUsage', 'TLS Web Server Authentication']
        ])

        tls_options = {
          protocol: :tls,
          version: 'TLSv1_2',
          ciphers: 'ALL:!aNULL:!eNULL:!SSLv2',
          insecure: false,
          cert_path: cert_path,
          private_key_path: private_key_path,
        }
        tls_options[:private_key_passphrase] = private_key_passphrase if private_key_passphrase
        received = ""
        @d.server_create_tls(:s, PORT, tls_options: tls_options) do |data, conn|
          received << data
        end
        open_tls_session('127.0.0.1', PORT, cert_path: ca_cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => ["foooooooo", "yaaaaaaaaaaaaaaaaaaay"],
           'without passphrase'  => [nil, nil])
      test 'load chained server cert by private CA cert file, verified from clients using CA cert file as root' do |(ca_key_passphrase, private_key_passphrase)|
        ca_cert_path = File.join(@certs_dir, "ca_cert.pem")
        ca_key_path = File.join(@certs_dir, "ca.key.pem")
        cert_path = File.join(@server_cert_dir, "cert.pem")
        private_key_path = File.join(@certs_dir, "server.key.pem")
        create_server_pair_chained_with_root_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, private_key_passphrase)

        tls_options = {
          protocol: :tls,
          version: 'TLSv1_2',
          ciphers: 'ALL:!aNULL:!eNULL:!SSLv2',
          insecure: false,
          cert_path: cert_path,
          private_key_path: private_key_path,
        }
        tls_options[:private_key_passphrase] = private_key_passphrase if private_key_passphrase
        received = ""
        @d.server_create_tls(:s, PORT, tls_options: tls_options) do |data, conn|
          received << data
        end
        open_tls_session('127.0.0.1', PORT, cert_path: ca_cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end
    end

    sub_test_case 'using configurations to specify cert options' do
      test 'create dynamic self-signed cert/key pair (without any verification from clients)' do
        # insecure
        transport_opts = {
          'insecure' => 'true',
        }
        transport_conf = config_element('transport', 'tls', transport_opts)
        conf = config_element('match', 'tag.*', {}, [transport_conf])

        @d.configure(conf); @d.start; @d.after_start

        received = ""
        @d.server_create_tls(:s, PORT) do |data, conn|
          received << data
        end
        assert_raise "" do
          open_tls_session('127.0.0.1', PORT) do |sock|
            sock.post_connection_check('myserver.testing.fluentd.org')
            # cannot connect ....
          end
        end
        open_tls_session('127.0.0.1', PORT, verify: false) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => "yaaaaaaaaaaaaaaaaaaay",
           'without passphrase'  => nil)
      test 'load self-signed cert/key pair (files), verified from clients using cert files' do |private_key_passphrase|
        cert_path = File.join(@server_cert_dir, "cert.pem")
        private_key_path = File.join(@certs_dir, "server.key.pem")
        create_server_pair_signed_by_self(cert_path, private_key_path, private_key_passphrase)

        transport_opts = {
          'cert_path' => cert_path,
          'private_key_path' => private_key_path,
        }
        transport_opts['private_key_passphrase'] = private_key_passphrase if private_key_passphrase
        transport_conf = config_element('transport', 'tls', transport_opts)
        conf = config_element('match', 'tag.*', {}, [transport_conf])

        @d.configure(conf); @d.start; @d.after_start

        received = ""
        @d.server_create_tls(:s, PORT) do |data, conn|
          received << data
        end
        assert_raise "" do
          open_tls_session('127.0.0.1', PORT) do |sock|
            sock.post_connection_check('server.testing.fluentd.org')
            # cannot connect by failing verification without server cert
          end
        end
        open_tls_session('127.0.0.1', PORT, cert_path: cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => "fooooooooooooooooooooooooo",
           'without passphrase'  => nil)
      test 'create dynamic server cert by private CA cert file, verified from clients using CA cert file' do |ca_key_passphrase|
        ca_cert_path = File.join(@certs_dir, "ca_cert.pem")
        ca_key_path = File.join(@certs_dir, "ca.key.pem")
        create_ca_pair_signed_by_self(ca_cert_path, ca_key_path, ca_key_passphrase)

        transport_opts = {
          'ca_cert_path' => ca_cert_path,
          'ca_private_key_path' => ca_key_path,
        }
        transport_opts['ca_private_key_passphrase'] = ca_key_passphrase if ca_key_passphrase
        transport_conf = config_element('transport', 'tls', transport_opts)
        conf = config_element('match', 'tag.*', {}, [transport_conf])

        @d.configure(conf); @d.start; @d.after_start

        received = ""
        @d.server_create_tls(:s, PORT) do |data, conn|
          received << data
        end
        open_tls_session('127.0.0.1', PORT, cert_path: ca_cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => ["foooooooo", "yaaaaaaaaaaaaaaaaaaay"],
           'without passphrase'  => [nil, nil])
      test 'load static server cert by private CA cert file, verified from clients using CA cert file' do |(ca_key_passphrase, private_key_passphrase)|
        ca_cert_path = File.join(@certs_dir, "ca_cert.pem")
        ca_key_path = File.join(@certs_dir, "ca.key.pem")
        create_ca_pair_signed_by_self(ca_cert_path, ca_key_path, ca_key_passphrase)

        cert_path = File.join(@server_cert_dir, "cert.pem")
        private_key_path = File.join(@certs_dir, "server.key.pem")
        create_server_pair_signed_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, private_key_passphrase)

        transport_opts = {
          'cert_path' => cert_path,
          'private_key_path' => private_key_path,
        }
        transport_opts['private_key_passphrase'] = private_key_passphrase if private_key_passphrase
        transport_conf = config_element('transport', 'tls', transport_opts)
        conf = config_element('match', 'tag.*', {}, [transport_conf])

        @d.configure(conf); @d.start; @d.after_start

        received = ""
        @d.server_create_tls(:s, PORT) do |data, conn|
          received << data
        end
        open_tls_session('127.0.0.1', PORT, cert_path: ca_cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end

      data('with passphrase' => ["foooooooo", "yaaaaaaaaaaaaaaaaaaay"],
           'without passphrase'  => [nil, nil])
      test 'load chained server cert by private CA cert file, verified from clients using CA cert file as root' do |(ca_key_passphrase, private_key_passphrase)|
        ca_cert_path = File.join(@certs_dir, "ca_cert.pem")
        ca_key_path = File.join(@certs_dir, "ca.key.pem")
        cert_path = File.join(@server_cert_dir, "cert.pem")
        private_key_path = File.join(@certs_dir, "server.key.pem")
        create_server_pair_chained_with_root_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, private_key_passphrase)

        transport_opts = {
          'cert_path' => cert_path,
          'private_key_path' => private_key_path,
        }
        transport_opts['private_key_passphrase'] = private_key_passphrase if private_key_passphrase
        transport_conf = config_element('transport', 'tls', transport_opts)
        conf = config_element('match', 'tag.*', {}, [transport_conf])

        @d.configure(conf); @d.start; @d.after_start

        received = ""
        @d.server_create_tls(:s, PORT) do |data, conn|
          received << data
        end
        open_tls_session('127.0.0.1', PORT, cert_path: ca_cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
        waiting(10){ sleep 0.1 until received.bytesize == 8 }
        assert_equal "yay\nfoo\n", received
      end
    end
  end

  sub_test_case '#server_create_tls' do
    setup do
      @certs_dir = File.join(TMP_DIR, "tls_certs")
      FileUtils.rm_rf @certs_dir
      FileUtils.mkdir_p @certs_dir

      @server_cert_dir = File.join(@certs_dir, "server")
      FileUtils.mkdir_p @server_cert_dir

      @cert_path = File.join(@server_cert_dir, "cert.pem")
      private_key_path = File.join(@certs_dir, "server.key.pem")
      private_key_passphrase = "yaaaaaaaaaaaaaaaaaaay"
      create_server_pair_signed_by_self(@cert_path, private_key_path, private_key_passphrase)

      @default_hostname = ::Socket.gethostname

      @tls_options = {
        protocol: :tls,
        version: 'TLSv1_2',
        ciphers: 'ALL:!aNULL:!eNULL:!SSLv2',
        insecure: false,
        cert_path: @cert_path,
        private_key_path: private_key_path,
        private_key_passphrase: private_key_passphrase,
      }
    end

    test 'can accept all keyword arguments valid for tcp/tls server' do
      assert_nothing_raised do
        @d.server_create_tls(:s, PORT, bind: '127.0.0.1', shared: false, resolve_name: true, linger_timeout: 10, backlog: 500, tls_options: @tls_options) do |data, conn|
          # ...
        end
      end
    end

    test 'creates a tls server just to read data' do
      received = ""
      @d.server_create_tls(:s, PORT, tls_options: @tls_options) do |data, conn|
        received << data
      end
      3.times do
        open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal 3, received.scan("yay\n").size
      assert_equal 3, received.scan("foo\n").size
    end

    test 'creates a tls server to read and write data' do
      received = ""
      responses = []
      @d.server_create_tls(:s, PORT, tls_options: @tls_options) do |data, conn|
        received << data
        conn.write "ack\n"
      end
      3.times do
        # open_tls_session('127.0.0.1', PORT, cert_path: @cert_path, hostname: @default_hostname) do |sock|
        open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
          responses << sock.readline
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal 3, received.scan("yay\n").size
      assert_equal 3, received.scan("foo\n").size
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'creates a tls server to read and write data using IPv6' do
      omit "IPv6 unavailable here" unless ipv6_enabled?

      received = ""
      responses = []
      @d.server_create_tls(:s, PORT, bind: "::1",  tls_options: @tls_options) do |data, conn|
        received << data
        conn.write "ack\n"
      end
      3.times do
        # open_tls_session('::1', PORT, cert_path: @cert_path, hostname: @default_hostname) do |sock|
        open_tls_session('::1', PORT, cert_path: @cert_path) do |sock|
          sock.puts "yay"
          sock.puts "foo"
          responses << sock.readline
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 24 }
      assert_equal 3, received.scan("yay\n").size
      assert_equal 3, received.scan("foo\n").size
      assert_equal ["ack\n","ack\n","ack\n"], responses
    end

    test 'does not resolve name of client address in default' do
      received = ""
      sources = []
      @d.server_create_tls(:s, PORT, tls_options: @tls_options) do |data, conn|
        received << data
        sources << conn.remote_host
      end
      3.times do
        # open_tls_session('127.0.0.1', PORT, cert_path: @cert_path, hostname: @default_hostname) do |sock|
        open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal 3, received.scan("yay\n").size
      assert{ sources.all?{|s| s == "127.0.0.1" } }
    end

    test 'does resolve name of client address if resolve_name is true' do
      hostname = Socket.getnameinfo([nil, nil, nil, "127.0.0.1"])[0]

      received = ""
      sources = []
      @d.server_create_tls(:s, PORT, resolve_name: true, tls_options: @tls_options) do |data, conn|
        received << data
        sources << conn.remote_host
      end
      3.times do
        # open_tls_session('127.0.0.1', PORT, cert_path: @cert_path, hostname: @default_hostname) do |sock|
        open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal 3, received.scan("yay\n").size
      assert{ sources.all?{|s| s == hostname } }
    end

    test 'can keep connections alive for tls if keepalive specified' do
      # pend "not implemented yet"
    end

    test 'raises error if plugin registers data callback for connection object from #server_create' do
      received = ""
      errors = []
      @d.server_create_tls(:s, PORT, tls_options: @tls_options) do |data, conn|
        received << data
        begin
          conn.data{|d| received << d.upcase }
        rescue => e
          errors << e
        end
      end
      open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
        sock.puts "foo"
      end
      waiting(10){ sleep 0.1 until received.bytesize == 4 || errors.size == 1 }
      assert_equal "foo\n", received
      assert_equal 1, errors.size
      assert_equal "data callback can be registered just once, but registered twice", errors.first.message
    end

    test 'can call write_complete callback if registered' do
      buffer = ""
      lines = []
      responses = []
      response_completes = []
      @d.server_create_tls(:s, PORT, tls_options: @tls_options) do |data, conn|
        conn.on(:write_complete){|c| response_completes << true }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
        end
      end
      3.times do
        open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            responses << sock.readline
          rescue EOFError, IOError, Errno::ECONNRESET
            # ignore
          end
          sock.close
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && response_completes.size == 3 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["ack\n","ack\n","ack\n"], responses
      assert_equal [true, true, true], response_completes
    end

    test 'can call close callback if registered' do
      buffer = ""
      lines = []
      callback_results = []
      @d.server_create_tls(:s, PORT, tls_options: @tls_options) do |data, conn|
        conn.on(:close){|c| callback_results << "closed" }
        buffer << data
        if idx = buffer.index("\n")
          lines << buffer.slice!(0,idx+1)
          conn.write "ack\n"
        end
      end
      3.times do
        open_tls_session('127.0.0.1', PORT, cert_path: @cert_path) do |sock|
          sock.write "yay"
          sock.write "foo\n"
          begin
            while line = sock.readline
              if line == "ack\n"
                sock.close
              end
            end
          rescue EOFError, IOError, Errno::ECONNRESET
            # ignore
          end
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 && callback_results.size == 3 }
      assert_equal ["yayfoo\n", "yayfoo\n", "yayfoo\n"], lines
      assert_equal ["closed", "closed", "closed"], callback_results
    end
  end

  sub_test_case '#server_create_unix' do
    # not implemented yet

    # test 'can accept all keyword arguments valid for unix server'
    # test 'creates a unix server just to read data'
    # test 'creates a unix server to read and write data'

    # test 'raises error if plugin registers data callback for connection object from #server_create'
    # test 'can call write_complete callback if registered'
    # test 'can call close callback if registered'
  end

  def open_client(proto, addr, port)
    client = case proto
             when :tcp
               TCPSocket.open(addr, port)
             when :tls
               c = OpenSSL::SSL::SSLSocket.new(TCPSocket.open(addr, port))
               c.sync_close = true
               c.connect
             else
               raise ArgumentError, "unknown proto:#{proto}"
             end
    yield client
  ensure
    client.close rescue nil
  end

  # run tests for tcp, tls and unix
  sub_test_case '#server_create_connection' do
    test 'raise error if udp is specified in proto' do
      assert_raise(ArgumentError.new("BUG: cannot create connection for UDP")) do
        @d.server_create_connection(:myserver, PORT, proto: :udp){|c| c }
      end
    end

    # def server_create_connection(title, port, proto: :tcp, bind: '0.0.0.0', shared: true, tls_options: nil, resolve_name: false, linger_timeout: 0, backlog: nil, &block)
    protocols = {
      'tcp' => [:tcp, {}],
      'tls' => [:tls, {tls_options: {insecure: true}}],
      # 'unix' => [:unix, {path: ""}],
    }

    data(protocols)
    test 'raise error if block argument is not specified or too many' do |(proto, kwargs)|
      empty_block = ->(){}
      assert_raise(ArgumentError.new("BUG: block must have just one argument")) do
        @d.server_create_connection(:myserver, PORT, proto: proto, **kwargs, &empty_block)
      end
      assert_raise(ArgumentError.new("BUG: block must have just one argument")) do
        @d.server_create_connection(:myserver, PORT, proto: proto, **kwargs){|conn, what_is_this| [conn, what_is_this] }
      end
    end

    data(protocols)
    test 'does not resolve name of client address in default' do |(proto, kwargs)|
      received = ""
      sources = []
      @d.server_create_connection(:s, PORT, proto: proto, **kwargs) do |conn|
        sources << conn.remote_host
        conn.data do |d|
          received << d
        end
      end
      3.times do
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == "127.0.0.1" } }
    end

    data(protocols)
    test 'does resolve name of client address if resolve_name is true' do |(proto, kwargs)|
      hostname = Socket.getnameinfo([nil, nil, nil, "127.0.0.1"])[0]

      received = ""
      sources = []
      @d.server_create_connection(:s, PORT, proto: proto, resolve_name: true, **kwargs) do |conn|
        sources << conn.remote_host
        conn.data do |d|
          received << d
        end
      end
      3.times do
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until received.bytesize == 12 }
      assert_equal "yay\nyay\nyay\n", received
      assert{ sources.all?{|s| s == hostname } }
    end

    data(protocols)
    test 'creates a server to provide connection, which can read, write and close' do |(proto, kwargs)|
      lines = []
      buffer = ""
      @d.server_create_connection(:s, PORT, proto: proto, **kwargs) do |conn|
        conn.data do |d|
          buffer << d
          if buffer == "x"
            buffer.slice!(0, 1)
            conn.close
          end
          if idx = buffer.index("\n")
            lines << buffer.slice!(0, idx + 1)
            conn.write "foo!\n"
          end
        end
      end
      replied = []
      disconnecteds = []
      3.times do |i|
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sock.puts "yay"
          while line = sock.readline
            replied << line
            break
          end
          sock.write "x"
          connection_closed = false
          begin
            data = sock.read
            if data.empty?
              connection_closed = true
            end
          rescue => e
            if e.is_a?(Errno::ECONNRESET)
              connection_closed = true
            end
          ensure
            disconnecteds << connection_closed
          end
        end
      end
      waiting(10){ sleep 0.1 until lines.size == 3 }
      waiting(10){ sleep 0.1 until replied.size == 3 }
      waiting(10){ sleep 0.1 until disconnecteds.size == 3 }
      assert_equal ["yay\n", "yay\n", "yay\n"], lines
      assert_equal ["foo!\n", "foo!\n", "foo!\n"], replied
      assert_equal [true, true, true], disconnecteds
    end

    data(protocols)
    test 'creates a server to provide connection, which accepts callbacks for data, write_complete, and close' do |(proto, kwargs)|
      lines = []
      buffer = ""
      written = 0
      closed = 0
      @d.server_create_connection(:s, PORT, proto: proto, **kwargs) do |conn|
        conn.on(:write_complete){|_conn| written += 1 }
        conn.on(:close){|_conn| closed += 1 }
        conn.on(:data) do |d|
          buffer << d
          if idx = buffer.index("\n")
            lines << buffer.slice!(0, idx + 1)
            conn.write "foo!\n"
          end
        end
      end
      replied = []
      3.times do
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sock.puts "yay"
          while line = sock.readline
            replied << line
            break
          end
        end # TCP socket is closed here
      end
      waiting(10){ sleep 0.1 until lines.size == 3 }
      waiting(10){ sleep 0.1 until replied.size == 3 }
      waiting(10){ sleep 0.1 until closed == 3 }
      assert_equal ["yay\n", "yay\n", "yay\n"], lines
      assert_equal 3, written
      assert_equal 3, closed
      assert_equal ["foo!\n", "foo!\n", "foo!\n"], replied
    end

    data(protocols)
    test 'creates a server, and does not leak connections' do |(proto, kwargs)|
      buffer = ""
      closed = 0
      @d.server_create_connection(:s, PORT, proto: proto, **kwargs) do |conn|
        conn.on(:close){|_c| closed += 1 }
        conn.on(:data) do |d|
          buffer << d
        end
      end
      3.times do
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sock.puts "yay"
        end
      end
      waiting(10){ sleep 0.1 until buffer.bytesize == 12 }
      waiting(10){ sleep 0.1 until closed == 3 }
      assert_equal 0, @d.instance_eval{ @_server_connections.size }
    end

    data(protocols)
    test 'will refuse more connect requests after stop, but read data from sockets already connected, in non-shared server' do |(proto, kwargs)|
      connected = false
      begin
        open_client(proto, "127.0.0.1", PORT) do |sock|
          # expected behavior is connection refused...
          connected = true
        end
      rescue
      end

      assert_false connected

      received = ""
      @d.server_create_connection(:s, PORT, proto: proto, shared: false, **kwargs) do |conn|
        conn.on(:data) do |data|
          received << data
          conn.write "ack\n"
        end
      end

      th0 = Thread.new do
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sock.puts "yay"
          sock.readline
        end
      end

      value0 = waiting(5){ th0.value }
      assert_equal "ack\n", value0

      stopped = false
      sleeping = false
      ending = false

      th1 = Thread.new do
        open_client(proto, "127.0.0.1", PORT) do |sock|
          sleeping = true
          sleep 0.1 until stopped
          sock.puts "yay"
          res = sock.readline
          ending = true
          res
        end
      end

      sleep 0.1 until sleeping

      @d.stop
      assert @d.stopped?
      stopped = true

      sleep 0.1 until ending

      @d.before_shutdown
      @d.shutdown

      th2 = Thread.new do
        begin
          open_client(proto, "127.0.0.1", PORT) do |sock|
            sock.puts "foo"
          end
          false # failed
        rescue
          true # success
        end
      end

      value1 = waiting(5){ th1.value }
      value2 = waiting(5){ th2.value }

      assert_equal "yay\nyay\n", received
      assert_equal "ack\n", value1
      assert value2, "should be truthy value to show connection was correctly refused"
    end

    test 'can keep connections alive for tcp/tls if keepalive specified' do
      # pend "not implemented yet"
    end
  end

end
