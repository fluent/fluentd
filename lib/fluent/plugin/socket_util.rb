module Fluent
  module SocketUtil
    def create_udp_socket(host)
      require 'ipaddr'

      if IPAddr.new(IPSocket.getaddress(host)).ipv4?
        UDPSocket.new
      else
        UDPSocket.new(Socket::AF_INET6)
      end
    end
    module_function :create_udp_socket

    # Create TCP Socket
    #
    # Example:
    #
    #     begin
    #       SocketUtil.create_tcp_socket(host, port, connect_timeout: 0.5)
    #     rescue Timeout::Error
    #       log.debug "Timeout!"
    #     end
    #
    # @param [String] host
    # @param [Integer] port
    # @param [Hash] opts
    # @return [Socket]
    #
    # cf. https://bugs.ruby-lang.org/issues/5101
    def create_tcp_socket(host, port, opts={})
      connect_timeout = opts[:connect_timeout] || 5.0
      addr = Socket.pack_sockaddr_in(port, host)
      s = Socket.new(:AF_INET, :SOCK_STREAM, 0)
      begin
        s.connect_nonblock(addr)
      rescue Errno::EINPROGRESS
        IO.select(nil, [s], nil, connect_timeout) or raise Timeout::Error
      end
      s
    end
    module_function :create_tcp_socket

    # Open TCP Socket
    #
    # Example:
    #
    #     begin
    #       SocketUtil.open_tcp_socket(host, port, connect_timeout: 0.5) {|sock| }
    #     rescue Timeout::Error
    #       log.debug "Timeout!"
    #     end
    #
    # @param [String] host
    # @param [Integer] port
    # @param [Hash] opts
    def open_tcp_socket(host, port, opts={}, &block)
      s = create_tcp_socket(host, port, opts)
      begin
        yield s
      ensure
        s.close
      end
    end
    module_function :open_tcp_socket
  end
end
