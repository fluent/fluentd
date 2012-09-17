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

  module IOExchange

    class Server
      def initialize
        @finish_flag = BlockingFlag.new
        @client_sockets = []
        @cloexec_mode = nil
      end

      attr_accessor :cloexec_mode

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
        until @client_sockets.empty?
          @client_sockets.last.close
          @client_sockets.pop
        end
      end

      def run
        until @finish_flag.set?
          if @client_sockets.empty?
            @finish_flag.wait(1)
            next
          end

          ready_sockets, _, _ = IO.select(@client_sockets, nil, nil, 0.5)
          unless ready_sockets
            next
          end

          ready_sockets.each {|c|
            begin
              data = c.recv_nonblock(32*1024)
            rescue Errno::EAGAIN, Errno::EINTR
              next
            end

            if data == nil || data.empty?
              c.close
              @client_sockets.delete(c)
              next
            end

            msg = Marshal.load(data)
            error = nil
            begin
              io = open_io(msg)
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
              c.send data
            else
              # send io
              c.send Marshal.dump(io.fileno)
              c.send_io io
            end
          }
        end

        #rescue
        #  # TODO log
      end

      private
      def finished?
        return @finish_flag.set?
      end

      def open_io(msg)
        # override this method
      end

      def new_connection
        s1, s2 = UNIXSocket.pair
        case @cloexec_mode
        when :client
          s2.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        when :server
          s1.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        when :both
          s1.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
          s2.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        end
        #s1.fcntl(Fcntl::F_SETFL, File::NONBLOCK)
        @client_sockets << s1
        return s2
      end
    end

    class Client
      def initialize(connection)
        @connection = connection
      end

      def open_io(msg)
        @connection.send Marshal.dump(msg)

        data = @connection.recv
        msg = Marshal.load(data)

        if msg.is_a?(Integer)
          # success
          @connection.recv_io
        else
          # error
          raise msg
        end
      end

      def close
        @connection.close
      end
    end

  end

end

