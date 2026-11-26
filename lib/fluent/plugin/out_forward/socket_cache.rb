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

require 'fluent/plugin/output'

module Fluent::Plugin
  class ForwardOutput < Output
    class SocketCache
      TimedSocket = Struct.new(:timeout, :key, :sock)

      DISCARD_READ_SIZE = 16 * 1024
      DISCARD_MAX_READS = 64

      def initialize(timeout, log)
        @log = log
        @timeout = timeout
        @available_sockets = Hash.new { |obj, k| obj[k] = [] }
        @inflight_sockets = {}
        @inactive_sockets = []
        @mutex = Mutex.new
      end

      def checkout_or(key)
        obsolete_sockets = []

        @mutex.synchronize do
          tsock, obsolete_sockets = pick_socket(key)

          if tsock
            return tsock.sock
          else
            sock = yield
            new_tsock = TimedSocket.new(timeout, key, sock)
            @log.debug("connect new socket #{new_tsock}")

            @inflight_sockets[sock] = new_tsock
            return new_tsock.sock
          end
        end
      ensure
        obsolete_sockets.each do |sock|
          close_socket(sock.sock)
        end
      end

      def checkin(sock)
        @mutex.synchronize do
          if (s = @inflight_sockets.delete(sock))
            s.timeout = timeout
            @available_sockets[s.key] << s
          else
            @log.debug("there is no socket #{sock}")
          end
        end
      end

      def revoke(sock)
        @mutex.synchronize do
          if (s = @inflight_sockets.delete(sock))
            @inactive_sockets << s
          else
            @log.debug("there is no socket #{sock}")
          end
        end
      end

      def purge_obsolete_socks
        sockets = []
        inactive_sockets = []

        @mutex.synchronize do
          # don't touch @inflight_sockets

          @available_sockets.each do |_, socks|
            socks.each do |sock|
              if expired_socket?(sock)
                sockets << sock
                socks.delete(sock)
              end
            end
          end

          # reuse same object (@available_sockets)
          @available_sockets.reject! { |_, v| v.empty? }

          inactive_sockets = @inactive_sockets.dup
          @inactive_sockets.clear
        end

        sockets.each do |s|
          close_socket(s.sock)
        end
        inactive_sockets.each do |s|
          close_inactive_socket(s.sock)
        end
      end

      def clear
        sockets = []
        inactive_sockets = []
        @mutex.synchronize do
          sockets += @available_sockets.values.flat_map { |v| v }
          sockets += @inflight_sockets.values
          inactive_sockets = @inactive_sockets.dup

          @available_sockets.clear
          @inflight_sockets.clear
          @inactive_sockets.clear
        end

        sockets.each do |s|
          close_socket(s.sock)
        end
        inactive_sockets.each do |s|
          close_inactive_socket(s.sock)
        end
      end

      private

      # A failed send may leave earlier chunks in the send buffer. close resets the connection and drops them
      # when the socket has unread data, such as TLS 1.3 session tickets. The limit is for a peer that keeps writing.
      def close_inactive_socket(sock)
        DISCARD_MAX_READS.times do
          data = sock.read_nonblock(DISCARD_READ_SIZE, exception: false)
          break unless data.is_a?(String) && !data.empty?
        end
      rescue StandardError
      ensure
        close_socket(sock)
      end

      # Windows cannot close a non-blocking socket with SO_LINGER and unsent data unless it is shut down first.
      def close_socket(sock)
        sock.shutdown(:WR) if sock.respond_to?(:shutdown)
      rescue StandardError
      ensure
        sock.close rescue nil
      end

      # this method is not thread safe
      def pick_socket(key)
        if @available_sockets[key].empty?
          return nil, []
        end

        t = Time.now
        selected = nil
        remaining = []
        obsolete_sockets = []

        @available_sockets[key].each do |sock|
          if expired_socket?(sock, time: t) || unavailable_socket?(sock.sock)
            obsolete_sockets << sock
          elsif selected.nil?
            selected = sock
          else
            remaining << sock
          end
        end

        @available_sockets[key] = remaining

        if selected
          @inflight_sockets[selected.sock] = selected
          selected.timeout = timeout
        end

        [selected, obsolete_sockets]
      end

      def timeout
        @timeout && Time.now + @timeout
      end

      def expired_socket?(sock, time: Time.now)
        sock.timeout ? sock.timeout < time : false
      end

      def unavailable_socket?(sock)
        return sock.closed? if sock.respond_to?(:closed?) && sock.closed?

        io = if sock.respond_to?(:to_io)
               sock.to_io
             elsif sock.is_a?(IO)
               sock
             end

        io ? !!IO.select([io], nil, nil, 0) : false
      rescue IOError, SystemCallError
        true
      end
    end
  end
end
