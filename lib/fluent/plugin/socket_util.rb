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
  end
end
