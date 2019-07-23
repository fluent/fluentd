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

      def initialize(timeout, log)
        @log = log
        @timeout = timeout
        @available_sockets = Hash.new { |obj, k| obj[k] = [] }
        @inflight_sockets = {}
        @inactive_sockets = []
        @mutex = Mutex.new
      end

      def checkout_or(key)
        @mutex.synchronize do
          tsock = pick_socket(key)

          if tsock
            tsock.sock
          else
            val = yield
            new_tsock = TimedSocket.new(timeout, key, val)
            @log.debug("connect new socket #{new_tsock}")

            @inflight_sockets[val] = new_tsock
            new_tsock.sock
          end
        end
      end

      def checkin(sock)
        @mutex.synchronize do
          if (s = @inflight_sockets.delete(sock))
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
          @available_sockets = @available_sockets.select { |_, v| !v.empty? }

          sockets += @inactive_sockets
          @inactive_sockets.clear
        end

        sockets.each do |s|
          s.sock.close rescue nil
        end
      end

      def clear
        sockets = []
        @mutex.synchronize do
          sockets += @available_sockets.values.flat_map { |v| v }
          sockets += @inflight_sockets.values
          sockets += @inactive_sockets

          @available_sockets.clear
          @inflight_sockets.clear
          @inactive_sockets.clear
        end

        sockets.each do |s|
          s.sock.close rescue nil
        end
      end

      private

      # this method is not thread safe
      def pick_socket(key)
        if @available_sockets[key].empty?
          return nil
        end

        t = Time.now
        if (s = @available_sockets[key].find { |sock| !expired_socket?(sock, time: t) })
          @inflight_sockets[s.sock] = @available_sockets[key].delete(s)
          s
        else
          nil
        end
      end

      def timeout
        @timeout && Time.now + @timeout
      end

      def expired_socket?(sock, time: Time.now)
        sock.timeout ? sock.timeout < time : false
      end
    end
  end
end
