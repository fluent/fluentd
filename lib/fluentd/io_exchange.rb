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
  require 'tempfile'

  module IOExchange
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

    class Server
      def initialize
        @finish_flag = BlockingFlag.new
        @client_sockets = []
        @cloexec_mode = nil
        @connection_mutex = IPCMutex.new
      end

      attr_accessor :cloexec_mode
      attr_reader :connection_mutex

      def start
        @thread = Thread.new(&method(:run))
      end

      def stop
        @finish_flag.set!
      end

      def shutdown
        stop
        join
        close
      end

      def close
        @client_sockets.each {|c|
          c.close rescue nil
        }
      end

      def join
        if @thread
          @thread.join
          @thread = nil
        end
      end

      private
      def run
        until @finish_flag.set?
          if @client_sockets.empty?
            @finish_flag.wait(0.5)
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
              @client_sockets.delete(c)
              c.close rescue nil
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
              c.write data
            else
              # send io
              c.write Marshal.dump(io.fileno)
              c.send_io io
            end
          }
        end

      rescue
        STDERR.write "#{$!}\n"
        $!.backtrace.each {|bt| STDERR.write "\t#{bt}\n" }
      end

      def finished?
        return @finish_flag.set?
      end

      def open_io(msg)
        # override this method
      end

      def new_connection
        if @finish_flag.set?
          # TODO raise "Already finished"  # TODO
        end

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
        s1.sync = true
        s2.sync = true
        #s1.fcntl(Fcntl::F_SETFL, File::NONBLOCK)
        @client_sockets << s1
        return s2
      end
    end

    class Client
      def initialize(connection, connection_mutex)
        @connection = connection
        @connection_mutex = connection_mutex
      end

      def open_io(msg)
        @connection_mutex.synchronize do
          @connection.write Marshal.dump(msg)

          data = @connection.recv(32*1024)
          msg = Marshal.load(data)

          if msg.is_a?(Integer)
            # success
            @connection.recv_io
          else
            # error
            raise msg
          end
        end
      end

      def close
        @connection.close
      end
    end

  end

end

