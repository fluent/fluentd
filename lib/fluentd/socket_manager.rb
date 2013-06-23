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

  class SocketManager
    class API
      def initialize
        @sockets = {}  # cache opened sockets
      end

      def listen_tcp(bind, port)
        key = "tcp:#{bind}:#{port}"
        open_io(key, "TCPServer.listen(params[0], params[1])", [bind, port])
      end

      def listen_udp(bind, port)
        key = "udp:#{bind}:#{port}"
        open_io(key, "UDPServer.listen(params[0], params[1])", [bind, port])
      end

      def listen_unix(path)
        key = "unix:#{path}"
        open_io(key, "UNIXServer.listen(params[0])", [path])
      end

      def open_io(key, code, params)
        if io = @sockets[key]
          return io
        else
          @sockets[key] = open_eval(key, code, params)
          return io
        end
      end

      def close_all_io
        @sockets.keys.each {|key|
          @sockets[key].close
          @sockets.delete(key)
        }
      end

      private

      def open_eval(key, code, params)
        io = eval(code)
        io.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        io
      end
    end

    # parent process
    class Server < API
      def initialize
        @finish_flag = BlockingFlag.new
        @client_connections = []
        @connection_mutex = IPCMutex.new
        super
      end

      attr_accessor :logger

      def start
        @thread = Thread.new(&method(:run))
        self
      end

      def stop
        @finish_flag.set!
        self
      end

      def shutdown
        stop
        join
        close
        self
      end

      def close_client_connections
        @client_connections.each {|cc|
          cc.close rescue nil
        }
        @client_connections.clear
        nil
      end

      def close
        close_client_connections
        close_all_io
      end

      def join
        if @thread
          @thread.join
          @thread = nil
        end
      end

      def new_client
        if @finish_flag.set?
          # TODO raise "Already finished"  # TODO
        end

        s1, s2 = UNIXSocket.pair
        s1.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        s2.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        s1.sync = true
        s2.sync = true

        #s1.fcntl(Fcntl::F_SETFL, File::NONBLOCK)

        @client_connections << s1
        return Client.new(s2, connection_mutex)
      end

      private

      def run
        until @finish_flag.set?
          if @client_connections.empty?
            @finish_flag.wait(0.5)
            next
          end

          ready_sockets, _, _ = IO.select(@client_connections, nil, nil, 0.5)
          unless ready_sockets
            next
          end

          ready_sockets.each {|cc|
            handle_connection(cc)
          }
        end

      rescue
        # TODO logger
        STDERR.write "#{$!}\n"
        $!.backtrace.each {|bt| STDERR.write "\t#{bt}\n" }
      end

      def finished?
        return @finish_flag.set?
      end

      def handle_connection(cc)
        begin
          data = cc.recv_nonblock(32*1024)
        rescue Errno::EAGAIN, Errno::EINTR
          return
        end

        if data == nil || data.empty?
          @client_connections.delete(cc)
          cc.close rescue nil
          return
        end

        msg = Marshal.load(data)

        error = nil
        begin
          io = open_io(*msg)
        rescue
          error = $!
        end

        if error
          # send error
          begin
            data = Marshal.dump(error)
          rescue
            data = Marshal.dump(error.to_s)
          end
          cc.write data

        else
          # send io
          cc.write Marshal.dump(io.fileno)
          cc.send_io io
        end
      end
    end

    # child process
    class Client < API
      def initialize(connection, connection_mutex)
        @connection = connection
        @connection_mutex = connection_mutex
        super
      end

      # override; calls parent Server#open_io
      def open_io(key, code, params)
        msg = [key, code, params]

        @connection_mutex.synchronize do
          @connection.write Marshal.dump(msg)

          data = @connection.recv(32*1024)
          res = Marshal.load(data)

          if res.is_a?(Integer)
            # success
            @connection.recv_io
          else
            # error
            raise res
          end
        end
      end

      def close
        @connection.close
      end
    end

    class IPCMutex
      def initialize
        @mutex = Mutex.new
        @file = Tempfile.new('fluentd-ipc-mutex-')
        @file.unlink
      end

      def synchronize(&block)
        @mutex.lock
        begin
          @file.flock(File::LOCK_EX)
          begin
            return block.call
          ensure
            @file.flock(File::LOCK_UN)
          end
        ensure
          @mutex.unlock
        end
      end
    end
  end

end
