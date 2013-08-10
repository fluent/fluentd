#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
module Fluentd

  require 'socket'
  require 'ipaddr'

  module SocketManager
    module API
      def listen_tcp(bind, port)
        io = open_io("TCPServer.new(params[0], params[1])", [bind, port])
        TCPServer.for_
      end

      def listen_udp(bind, port)
        code = <<EOF
if IPAddr.new(IPSocket.getaddress(params[0])).ipv4?
  s = UDPSocket.new
else
  s = UDPSocket.new(Socket::AF_INET6)
end
s.bind(params[0], params[1])
s
EOF
        open_io(code, [bind, port])
      end

      def listen_unix(path)
        open_io("UNIXServer.new(params[0])", [path])
      end
    end

    require_relative 'socket_manager/unix'
    #require_relative 'socket_manager/windows'  # TODO

    class NonManagedAPI
      include API

      def open_io(code, params)
        eval(code)
      end
    end
  end

end
