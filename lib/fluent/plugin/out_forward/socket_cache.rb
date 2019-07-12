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
      TimedSocket = Struct.new(:timeout, :sock, :ref)

      def initialize(timeout, log)
        @log = log
        @timeout = timeout
        @active_socks = {}
        @inactive_socks = {}
        @mutex = Mutex.new
      end

      def revoke(key = Thread.current.object_id)
        @mutex.synchronize do
          if @active_socks[key]
            @inactive_socks[key] = @active_socks.delete(key)
            @inactive_socks[key].ref = 0
          end
        end
      end

      def clear
        @mutex.synchronize do
          @inactive_socks.values.each do |s|
            s.sock.close rescue nil
          end
          @inactive_socks.clear

          @active_socks.values.each do |s|
            s.sock.close rescue nil
          end
          @active_socks.clear
        end
      end

      def purge_obsolete_socks
        @mutex.synchronize do
          @inactive_socks.keys.each do |k|
            # 0 means sockets stored in this class received all acks
            if @inactive_socks[k].ref <= 0
              s = @inactive_socks.delete(k)
              s.sock.close  rescue nil
              @log.debug("purged obsolete socket #{s.sock}")
            end
          end

          @active_socks.keys.each do |k|
            if expired?(k) && @active_socks[k].ref <= 0
              @inactive_socks[k] = @active_socks.delete(k)
            end
          end
        end
      end

      # We expect that `yield` returns a unique object in this class
      def fetch_or(key = Thread.current.object_id)
        @mutex.synchronize do
          unless @active_socks[key]
            @active_socks[key] = TimedSocket.new(timeout, yield, 1)
            @log.debug("connect new socket #{@active_socks[key]}")
            return @active_socks[key].sock
          end

          if expired?(key)
            # Do not close this socket here in case of it will be used by other place (e.g. wait for receiving ack)
            @inactive_socks[key] = @active_socks.delete(key)
            @log.debug("connection #{@inactive_socks[key]} is expired. reconnecting...")
            @active_socks[key] = TimedSocket.new(timeout, yield, 0)
          end

          @active_socks[key].ref += 1
          @active_socks[key].sock
        end
      end

      def dec_ref(key = Thread.current.object_id)
        @mutex.synchronize do
          if @active_socks[key]
            @active_socks[key].ref -= 1
          elsif @inactive_socks[key]
            @inactive_socks[key].ref -= 1
          else
            @log.warn("Not found key for dec_ref: #{key}")
          end
        end
      end

      # This method is expected to be called in class which doesn't call #inc_ref
      def dec_ref_by_value(val)
        @mutex.synchronize do
          sock = @active_socks.detect { |_, v| v.sock == val }
          if sock
            key = sock.first
            @active_socks[key].ref -= 1
            return
          end

          sock = @inactive_socks.detect { |_, v| v.sock == val }
          if sock
            key = sock.first
            @inactive_socks[key].ref -= 1
            return
          else
            @log.warn("Not found key for dec_ref_by_value: #{key}")
          end
        end
      end

      # This method is expected to be called in class which doesn't call #fetch_or
      def revoke_by_value(val)
        @mutex.synchronize do
          sock = @active_socks.detect { |_, v| v.sock == val }
          if sock
            key = sock.first
            @inactive_socks[key] = @active_socks.delete(key)
            @inactive_socks[key].ref = 0
          else
            @log.debug("Not found for revoke_by_value :#{val}")
          end
        end
      end

      private

      def timeout
        @timeout && Time.now + @timeout
      end

      # This method is thread unsafe
      def expired?(key = Thread.current.object_id)
        @active_socks[key].timeout ? @active_socks[key].timeout < Time.now : false
      end
    end
  end
end
