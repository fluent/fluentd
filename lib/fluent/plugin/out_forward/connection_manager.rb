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

require 'fluent/output'

module Fluent::Plugin
  class ForwardOutput < Output
    class ConnectionManager
      RequestInfo = Struct.new(:state, :shared_key_nonce, :auth)

      def initialize(log:, secure:, connection_factory:, socket_cache:)
        @log = log
        @secure = secure
        @connection_factory = connection_factory
        @socket_cache = socket_cache
      end

      def stop
        @socket_cache && @socket_cache.clear
      end

      def connect(host:, port:, hostname:, require_ack:, &block)
        if @socket_cache
          return connect_keepalive(host: host, port: port, hostname: hostname, require_ack: require_ack, &block)
        end

        @log.debug('connect new socket')
        socket = @connection_factory.call(host, port, hostname)
        request_info = RequestInfo.new(@secure ? :helo : :established)

        unless block_given?
          return [socket, request_info]
        end

        begin
          yield(socket, request_info)
        ensure
          unless require_ack
            socket.close_write rescue nil
            socket.close rescue nil
          end
        end
      end

      def purge_obsolete_socks
        unless @socket_cache
          raise "Do not call this method without keepalive option"
        end
        @socket_cache.purge_obsolete_socks
      end

      def close(sock)
        if @socket_cache
          @socket_cache.dec_ref_by_value(sock)
        else
          sock.close_write rescue nil
          sock.close rescue nil
        end
      end

      private

      def connect_keepalive(host:, port:, hostname:, require_ack:)
        request_info = RequestInfo.new(:established)
        socket = @socket_cache.fetch_or do
          s = @connection_factory.call(host, port, hostname)
          request_info = RequestInfo.new(@secure ? :helo : :established) # overwrite if new connection
          s
        end

        unless block_given?
          return [socket, request_info]
        end

        ret = nil
        begin
          ret = yield(socket, request_info)
        rescue
          @socket_cache.revoke
          raise
        else
          unless require_ack
            @socket_cache.dec_ref
          end
        end

        ret
      end
    end
  end
end
