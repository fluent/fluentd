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
module Fluentd

  require 'socket'
  require 'fcntl'

  module SocketManager
    class Client
      include SocketManager::API

      def initialize(client_fileno, server_id)
        @sock = UNIXSocket.for_fd(client_fileno)
        @server_id = server_id
        @used_keys = {}
      end

      def open_io(code, params)
        key = [code, params]
        if @used_keys[key]
          raise Errno::EADDRINUSE, "Address already in use in the same server '#{@server_id}'"
        end

        msg = [@server_id, code, params]
        @sock.write Marshal.dump(msg)

        data = @sock.recv(32*1024)
        res = Marshal.load(data)

        if res.is_a?(Integer)
          # success
          io = @sock.recv_io
          @used_keys[key] = true
          return io
        else
          # error
          raise res
        end
      end

      def start_heartbeat
        Thread.new do
          while true
            sleep 0.5
            heartbeat
          end
        end
      end

      def heartbeat
        @sock.write Marshal.dump(nil)
      end

      def close
        @sock.close
      end
    end

    class Server
      include SocketManager::API

      def initialize
        @stop_flag = ServerEngine::BlockingFlag.new
        @clients = {}
        @sockets = {}
        super
      end

      attr_accessor :logger

      def start
        @thread = Thread.new(&method(:run))
        self
      end

      def stop
        @stop_flag.set!
        self
      end

      def shutdown
        stop
        join
        close
        self
      end

      def join
        if @thread
          @thread.join
          @thread = nil
        end
      end

      def close
        @clients.each_pair {|sock,monitor|
          sock.close
        }
        @sockets.each_pair {|cache_key,io|
          io.close
        }
        nil
      end

      def new_client_pipe
        if @stop_flag.set?
          # TODO raise "Already finished"  # TODO
        end

        s1, s2 = UNIXSocket.pair
        s1.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        s2.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        s1.sync = true
        s2.sync = true

        monitor = HeartbeatMonitor.new
        @clients[s1] = monitor

        return s2, monitor
      end

      def run
        until @stop_flag.set?
          if @clients.empty?
            @stop_flag.wait(0.5)
            next
          end

          ready_sockets, _, _ = IO.select(@clients.keys, nil, nil, 0.5)
          unless ready_sockets
            next
          end

          ready_sockets.each {|sock|
            handle_connection(sock)
          }
        end

      rescue => e
        # TODO logger
        STDERR.write "#{e}\n"
        e.backtrace.each {|bt| STDERR.write "\t#{bt}\n" }
      end

      def finished?
        return @stop_flag.set?
      end

      def handle_connection(sock)
        begin
          data = sock.recv_nonblock(32*1024)
        rescue Errno::EAGAIN, Errno::EINTR
          return
        end

        if data == nil || data.empty?
          @clients.delete(sock)
          sock.close rescue nil
          return
        end

        @clients[sock].update
        msg = Marshal.load(data)

        return if msg == nil

        error = nil
        begin
          io = open_eval(*msg)
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
          sock.write data

        else
          # send io
          sock.write Marshal.dump(io.fileno)
          sock.send_io io
        end
      end

      def open_io(code, params)
        open_eval(nil, code, params)
      end

      def open_eval(server_id, code, params)
        reuse_key = [code, params]

        wid, io = @sockets[reuse_key]
        if io
          if wid != server_id
            raise Errno::EADDRINUSE, "Address already in use by server '#{server_id}'"
          end
          return io
        else
          io = eval(code)
          @sockets[reuse_key] = io
          return io
        end
      end

      class HeartbeatMonitor
        def initialize
          @last_time = nil
        end

        attr_reader :last_time

        def update
          @last_time = Time.now
        end

        def check
          # TODO
        end
      end
    end

  end
end
