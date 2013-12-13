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
  module Plugin

    class ForwardInput < Input
      Plugin.register_input('forward', self)

      config_param :port, :integer, :default => 24224
      config_param :bind, :string, :default => '0.0.0.0'

      def start
        @usock = Engine.sockets.listen_udp(@bind, @port)
        actor.watch_io(@usock, &method(:on_heartbeat_readable))

        actor.listen_tcp(@bind, @port) do |sock|
          h = Handler.new(self, sock, method(:on_message))
          actor.watch_io(sock, &(h.method(:on_readable)))
        end

        super
      end

      def on_heartbeat_readable(sock)
        begin
          sock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
          msg, addr = sock.recvfrom(1024)
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          return
        end
        host = addr[3]
        port = addr[1]
        send_heartbeat(sock, host, port)
      end

      def send_heartbeat(sock, host, port)
        begin
          @usock.send "\0", 0, host, port
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
        end
      end

      # message Entry {
      #   1: long time
      #   2: object record
      # }
      #
      # message Forward {
      #   1: string tag
      #   2: list<Entry> entries
      # }
      #
      # message PackedForward {
      #   1: string tag
      #   2: raw entries  # msgpack stream of Entry
      # }
      #
      # message Message {
      #   1: string tag
      #   2: long? time
      #   3: object record
      # }
      def on_message(msg)
        if msg.nil?
          # for future TCP heartbeat_request
          return
        end

        # TODO format error
        tag = msg[0].to_s
        entries = msg[1]

        if entries.class == String
          # PackedForward
          es = MessagePackEventCollection.new(entries)
          event_router.emits(tag, es)

        elsif entries.class == Array
          # Forward
          es = MultiEventCollection.new
          entries.each {|e|
            time = e[0].to_i
            time = (now ||= Time.now.to_i) if time == 0
            record = e[1]
            es.add(time, record)
          }
          event_router.emits(tag, es)

        else
          # Message
          time = msg[1]
          time = Time.now.to_i if time == 0
          record = msg[2]
          event_router.emit(tag, time, record)
        end
      end

      class Handler
        attr_reader :log

        def initialize(parent, io, on_message)
          @parent = parent
          @log = parent.log
          @io = io
          if @io.is_a?(TCPSocket)
            opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
            @io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
          end
          log.trace { "accepted fluent socket object_id=#{self.object_id}" }
          @on_message = on_message
          @buffer = ''
        end

        def on_readable(io)
          begin
            data = io.read_nonblock(32*1024, @buffer)
          rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR, EOFError
            return
          end
          on_read(data)
        end

        def on_read(data)
          first = data[0]
          if first == '{' || first == '['
            m = method(:on_read_json)
            @y = Yajl::Parser.new
            @y.on_parse_complete = @on_message
          else
            m = method(:on_read_msgpack)
            @u = MessagePack::Unpacker.new
          end

          (class << self; self; end).module_eval do
            define_method(:on_read, m)
          end
          m.call(data)
        end

        def on_read_json(data)
          @y << data
        rescue
          log.error "forward error: #{$!.to_s}"
          log.error_backtrace
          @io.close
        end

        def on_read_msgpack(data)
          @u.feed_each(data, &@on_message)
        rescue
          log.error "forward error: #{$!.to_s}"
          log.error_backtrace
          @io.close
        end

        def on_close
          log.trace { "closed fluent socket object_id=#{self.object_id}" }
        end
      end
    end

  end
end
