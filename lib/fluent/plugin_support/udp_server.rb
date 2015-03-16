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

      # keepalive: seconds, (default: nil [inf])
      def udp_server_listen(port: nil, bind: '0.0.0.0', size_limit: 2048, &block)
        raise "BUG: specify port for udp_server_listen" unless port

        bind_sock = if IPAddr.new(IPSocket.getaddress(bind)).ipv4?
                      UDPSocket.new
                    else
                      UDPSocket.new(Socket::AF_INET6)
                    end
        bind_sock.bind(bind, port)

        if self.respond_to?(:detach_multi_process)
          detach_multi_process do
            udp_server_read_impl(bind_sock, size_limit, &block)
          end
        elsif self.respond_to?(:detach_process)
          detach_process do
            udp_server_read_impl(bind_sock, size_limit, &block)
          end
        else
          udp_server_read_impl(bind_sock, size_limit, &block)
        end
      end

      def shutdown
        super
        @_udp_server_socks.each{|s| s.close unless s.closed? }
      end

      def udp_server_read_impl(sock, body_size_limit, &block)
        sock = Handler.new(sock, body_size_limit, &block)
        @_udp_server_socks << sock
        event_loop_attach( sock )
      end

      class Handler < Coolio::IO
        def initialize(io, body_size_limit, &callback)
          super(io)

          @io = io
          @body_size_limit = body_size_limit
          @callback = callback

          ### TODO: address/name, socket option
          #
          ###### from tcp server
          # PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]
          # @addr = (io.peeraddr rescue PEERADDR_FAILED)
          # opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          # io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
        end

        def on_readable
          data, addr = @io.recvfrom_nonblock(@body_size_limit)
          @callback.call(addr[2], addr[1], data) # host, port, data
        rescue => e
          # TODO: logging
          @log.error "unexpected error", error: e, error_class: e.class
        end
      end
    end
  end
end
