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
require 'fluent/clock'

module Fluent::Plugin
  class ForwardOutput < Output
    class AckHandler
      def initialize(timeout, log:, read_length:)
        @mutex = Mutex.new
        @ack_waitings = []
        @timeout = timeout
        @log = log
        @read_length = read_length
        @unpacker = Fluent::Engine.msgpack_unpacker
      end

      def ack_reader(select_interval)
        now = Fluent::Clock.now
        sockets = []
        begin
          invalid_sockets = []
          valid_sockets = []

          new_list = []
          @mutex.synchronize do
            @ack_waitings.each do |info|
              if info.expired?(now)
                # There are 2 types of cases when no response has been received from socket:
                # (1) the node does not support sending responses
                # (2) the node does support sending response but responses have not arrived for some reasons.
                @log.warn 'no response from node. regard it as unavailable.', host: info.node.host, port: info.node.port
                info.node.disable!
                info.node.close(info.sock)
                invalid_sockets << info.chunk_id
              else
                sockets << info.sock
                new_list << info
              end
            end
            @ack_waitings = new_list
          end

          readable_sockets, _, _ = IO.select(sockets, nil, nil, select_interval)
          if readable_sockets
            readable_sockets.each do |sock|
              chunk_id, success = read_ack_from_sock(sock)
              next if chunk_id.nil?

              if success
                valid_sockets << chunk_id
              else
                invalid_sockets << chunk_id
              end
            end
          end

          invalid_sockets.each do |chunk_id|
            yield chunk_id, false
          end

          valid_sockets.each do |chunk_id|
            yield chunk_id, true
          end
        rescue => e
          @log.error 'unexpected error while receiving ack', error: e
          @log.error_backtrace
        end
      end

      ACKWaitingSockInfo = Struct.new(:sock, :chunk_id, :chunk_id_base64, :node, :time, :timeout) do
        def expired?(now)
          time + timeout < now
        end
      end

      Ack = Struct.new(:id, :handler, :node) do
        def enqueue(sock)
          handler.enqueue(node, sock, id)
        end
      end

      def create_ack(id, node)
        Ack.new(id, self, node)
      end

      def enqueue(node, sock, cid)
        info = ACKWaitingSockInfo.new(sock, cid, Base64.encode64(cid), node, Fluent::Clock.now, @timeout)
        @mutex.synchronize do
          @ack_waitings << info
        end
      end

      private

      def read_ack_from_sock(sock)
        begin
          raw_data = sock.instance_of?(Fluent::PluginHelper::Socket::WrappedSocket::TLS) ? sock.readpartial(@read_length) : sock.recv(@read_length)
        rescue Errno::ECONNRESET, EOFError # ECONNRESET for #recv, #EOFError for #readpartial
          raw_data = ''
        end

        info = find(sock)

        # When connection is closed by remote host, socket is ready to read and #recv returns an empty string that means EOF.
        # If this happens we assume the data wasn't delivered and retry it.
        if raw_data.empty?
          @log.warn 'destination node closed the connection. regard it as unavailable.', host: info.node.host, port: info.node.port
          info.node.disable!
          return info.chunk_id, false
        else
          @unpacker.feed(raw_data)
          res = @unpacker.read
          @log.trace "getting response from destination", host: info.node.host, port: info.node.port, chunk_id: dump_unique_id_hex(info.chunk_id), response: res
          if res['ack'] != info.chunk_id_base64
            # Some errors may have occurred when ack and chunk id is different, so send the chunk again.
            @log.warn 'ack in response and chunk id in sent data are different', chunk_id: dump_unique_id_hex(info.chunk_id), ack: res['ack']
            return info.chunk_id, false
          else
            @log.trace 'got a correct ack response', chunk_id: dump_unique_id_hex(info.chunk_id)
          end

          return info.chunk_id, true
        end
      rescue => e
        @log.error 'unexpected error while receiving ack message', error: e
        @log.error_backtrace
        [nil, false]
      ensure
        info.node.close(info.sock)
        delete(info)
      end

      def dump_unique_id_hex(unique_id)
        Fluent::UniqueId.hex(unique_id)
      end

      def find(sock)
        @mutex.synchronize do
          @ack_waitings.find { |info| info.sock == sock }
        end
      end

      def delete(info)
        @mutex.synchronize do
          @ack_waitings.delete(info)
        end
      end
    end
  end
end
