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

  class SocketManager
    def initialize
      @finish_flag = BlockingFlag.new
      @sockets = {}
      @clients = []
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def stop
      @finish_flag.set!
    end

    def shutdown
      stop
      if @thread
        @thread.join
        @thread = nil
      end
      close
    end

    def close
      clear
      until @clients.empty?
        @clients.last.close
        @clients.pop
      end
    end

    def clear
      @sockets.keys.each {|key|
        @sockets[key].close
        @sockets.delete(key)
      }
    end

    def run
      until @finish_flag.set?
        if @clients.empty?
          @finish_flag.wait(1)
          next
        end

        ready_clients, _, _ = IO.select(@clients, nil, nil, 0.5)
        if ready_clients
          ready_clients.each {|c|
            c.fcntl(Fcntl::F_SETFL, File::NONBLOCK)
            begin
              data = c.recv
            rescue Errno::EAGAIN, Errno::EINTR
              next
            end

            msg = Marshal.load(data)
            begin
              io = lookup(*msg)
            rescue
              error = $!
            end

            c.fcntl(Fcntl::F_SETFL, 0)
            if io
              c.send Marshal.dump(io.fileno)
              c.send_io io
            else
              begin
                data = Marshal.dump(error)
              rescue
                data = Marshal.dump(error.to_s)
              end
              c.send data
            end
          }
        end
      end

    #rescue
    #  # TODO log
    end

    def new_client
      # TODO create socket pair
      # TODO set cloexec
      rec, con = UNIXSocket.pair
      rec.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
      @clients << rec
      return Client.new(con)
    end

    def lookup(key, code, params)
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

    class Client
      def initialize(con)
        @con = con
      end

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
        @con.send Marshal.dump([key, code, params])

        data = @con.recv
        msg = Marshal.load(data)

        if msg.is_a?(Integer)
          # success
          @con.recv_io
        else
          # error
          raise msg
        end
      end

      def close
        @con.close
      end
    end
  end

end
