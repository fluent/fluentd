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

require 'socket'
require 'ipaddr'

require_relative 'socket_option'

module Fluent
  module PluginHelper
    module Socket
      # stop     : [-]
      # shutdown : [-]
      # close    : [-]
      # terminate: [-]

      include Fluent::PluginHelper::SocketOption

      attr_reader :_sockets # for tests

      # TODO: implement connection pool for specified host

      def socket_create(proto, host, port, **kwargs, &block)
        case proto
        when :tcp
          socket_create_tcp(host, port, **kwargs, &block)
        when :udp
          socket_create_udp(host, port, **kwargs, &block)
        when :tls
          socket_create_tls(host, port, **kwargs, &block)
        when :unix
          raise "not implemented yet"
        else
          raise ArgumentError, "invalid protocol: #{proto}"
        end
      end

      def socket_create_tcp(host, port, resolve_name: false, **kwargs, &block)
        sock = TCPSocket.new(host, port)
        socket_option_set(sock, resolve_name: resolve_name, **kwargs)
        if block
          begin
            block.call(sock)
          ensure
            sock.close_write rescue nil
            sock.close rescue nil
          end
        else
          sock
        end
      end

      def socket_create_udp(host, port, resolve_name: false, connect: false, **kwargs, &block)
        family = IPAddr.new(IPSocket.getaddress(host)).ipv4? ? ::Socket::AF_INET : ::Socket::AF_INET6
        sock = UDPSocket.new(family)
        socket_option_set(sock, resolve_name: resolve_name, **kwargs)
        sock.connect(host, port) if connect
        if block
          begin
            block.call(sock)
          ensure
            sock.close rescue nil
          end
        else
          sock
        end
      end

      def socket_create_tls(host, port, resolve_name: false, certopts: {}, &block)
        raise "not implemented yet"
      end

      # socket_create_socks ?

      def initialize
        super
        # @_sockets = [] # for keepalived sockets / connection pool
      end

      # def close
      #   @_sockets.each do |sock|
      #     sock.close
      #   end
      #   super
      # end
    end
  end
end
