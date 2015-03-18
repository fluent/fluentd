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

require 'fluent/plugin_support/event_loop'

require 'socket'
require 'ipaddr'

module Fluent
  module PluginSupport
    module UDPServer
      include Fluent::PluginSupport::EventLoop

      def initialize
        super
        @_udp_server_socks = []
      end

      def configure(conf)
        super
      end

      def udp_server_listen(port: nil, bind: '0.0.0.0', &block)
        raise "BUG: callback block is not specified for udp_server_listen" unless block_given?
        raise "BUG: specify port for udp_server_listen" unless port

        bind_sock = if IPAddr.new(IPSocket.getaddress(bind)).ipv4?
                      UDPSocket.new
                    else
                      UDPSocket.new(Socket::AF_INET6)
                    end
        bind_sock.bind(bind, port)

        if self.respond_to?(:detach_multi_process)
          detach_multi_process do
            udp_server_listen_impl(bind_sock, &block)
          end
        elsif self.respond_to?(:detach_process)
          detach_process do
            udp_server_listen_impl(bind_sock, &block)
          end
        else
          udp_server_listen_impl(bind_sock, &block)
        end
      end

      def udp_server_read(port: nil, bind: '0.0.0.0', size_limit: 2048, &block)
        raise "BUG: callback block is not specified for udp_server_read" unless block_given?
        raise "BUG: specify port for udp_server_read" unless port

        udp_server_listen(port: port, bind: bind) do |sock|
          sock.read(size_limit: size_limit, &block)
        end
      end

      def shutdown
        super
        @_udp_server_socks.each do |s|
          s.detach if s.attached?
          s.close unless s.closed?
        end
      end

      def udp_server_listen_impl(sock, &block)
        sock = Handler.new(sock)
        block.call sock
        @_udp_server_socks << sock
        event_loop_attach( sock )
      end

      class Handler < Coolio::IO
        def initialize(io)
          super(io)

          @io = io

          ### TODO: address/name, socket option
          #
          ###### from tcp server
          # PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]
          # @addr = (io.peeraddr rescue PEERADDR_FAILED)
          # opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          # io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
          ####### from in_forward
          # @usock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
        end

        def read(size_limit: 2048, &callback)
          @size_limit = size_limit
          @callback = callback
        end

        def send(host: nil, port: nil, data: nil, flags: 0)
          @io.send(data, flags, port, host)
        end

        def on_readable
          data, addr = @io.recvfrom_nonblock(@size_limit)
          @callback.call(addr[2], addr[1], data) # host, port, data
          ## TODO: fix here
          # rescue => e
          ####### from in_forward
          #       rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          #          return

          # TODO: logging
          # @log.error "unexpected error", error: e, error_class: e.class
          # raise
        end
      end
    end
  end
end
