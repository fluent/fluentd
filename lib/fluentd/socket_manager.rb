#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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

  class SocketManager < IOExchange::Server
    def initialize
      super
      @sockets = {}
      self.cloexec_mode = :server
    end

    def close
      clear_sockets
      super
    end

    def clear_sockets
      @sockets.keys.each {|key|
        @sockets[key].close
        @sockets.delete(key)
      }
    end

    def new_client
      return Client.new(new_connection, connection_mutex)
    end

    private
    def open_io(msg)
      key, code, params = *msg
      if io = @sockets[key]
        return io
      else
        @socket[key] = listen(code, params)
      end
    end

    def listen(code, params)
      io = eval(code)
      io.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
      return io
    end

    class Client < IOExchange::Client
      def listen_tcp(address, port)
        key = "tcp:#{address}:#{port}"
        listen(key, "TCPServer.listen(params[0], params[1])", [address, port])
      end

      def listen_udp(address, port)
        key = "udp:#{address}:#{port}"
        listen(key, "UDPServer.listen(params[0], params[1])", [address, port])
      end

      def listen_unix(path)
        key = "unix:#{path}"
        listen(key, "UNIXServer.listen(params[0])", [path])
      end

      def listen(key, code, params)
        open_io([key, code, params])
      end
    end
  end

end
