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

require 'ipaddr'

require 'cool.io'

require 'fluent/plugin'

module Fluent
  module SocketUtil
    def create_udp_socket(host)
      if IPAddr.new(IPSocket.getaddress(host)).ipv4?
        UDPSocket.new
      else
        UDPSocket.new(Socket::AF_INET6)
      end
    end
    module_function :create_udp_socket

    class UdpHandler < Coolio::IO
      def initialize(io, log, body_size_limit, callback, resolve_hostname = false, remove_newline = true, receive_buffer_size = nil)
        super(io)
        if io.is_a?(UDPSocket) && receive_buffer_size
          io.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, receive_buffer_size)
        end
        @io = io
        @io.do_not_reverse_lookup = !resolve_hostname
        @log = log
        @body_size_limit = body_size_limit
        @remove_newline = remove_newline
        @callback = callback
      end

      def on_readable
        msg, addr = @io.recvfrom_nonblock(@body_size_limit)
        msg.chomp! if @remove_newline
        @callback.call(msg, addr)
      rescue => e
        @log.error "unexpected error", error: e, error_class: e.class
      end
    end

    class TcpHandler < Coolio::Socket
      PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]

      def initialize(io, log, delimiter, callback, resolve_hostname = false)
        super(io)
        if io.is_a?(TCPSocket)
          io.do_not_reverse_lookup = resolve_hostname
          @addr = (io.peeraddr rescue PEERADDR_FAILED)

          opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
        end
        @delimiter = delimiter
        @callback = callback
        @log = log
        @log.trace { "accepted fluent socket object_id=#{self.object_id}" }
        @buffer = "".force_encoding('ASCII-8BIT')
      end

      def on_connect
      end

      def on_read(data)
        @buffer << data
        pos = 0

        while i = @buffer.index(@delimiter, pos)
          msg = @buffer[pos...i]
          @callback.call(msg, @addr)
          pos = i + @delimiter.length
        end
        @buffer.slice!(0, pos) if pos > 0
      rescue => e
        @log.error "unexpected error", error: e, error_class: e.class
        close
      end

      def on_close
        @log.trace { "closed fluent socket object_id=#{self.object_id}" }
      end
    end

    class BaseInput < Fluent::Input
      def initialize
        super
        require 'fluent/parser'
      end

      desc 'Tag of output events.'
      config_param :tag, :string
      desc 'The format of the payload.'
      config_param :format, :string
      desc 'The port to listen to.'
      config_param :port, :integer, default: 5150
      desc 'The bind address to listen to.'
      config_param :bind, :string, default: '0.0.0.0'
      desc "Deprecated parameter. Use source_hostname_key instead"
      config_param :source_host_key, :string, default: nil, deprecated: "Use source_hostname_key instead"
      desc "The field name of the client's hostname."
      config_param :source_hostname_key, :string, default: nil
      config_param :blocking_timeout, :time, default: 0.5

      def configure(conf)
        super

        @source_hostname_key = @source_host_key if @source_host_key
        @parser = Plugin.new_parser(@format)
        @parser.configure(conf)
      end

      def start
        @loop = Coolio::Loop.new
        @handler = listen(method(:on_message))
        @loop.attach(@handler)
        @thread = Thread.new(&method(:run))
      end

      def shutdown
        @loop.watchers.each { |w| w.detach }
        @loop.stop
        @handler.close
        @thread.join
      end

      def run
        @loop.run(@blocking_timeout)
      rescue => e
        log.error "unexpected error", error: e, error_class: e.class
        log.error_backtrace
      end

      private

      def on_message(msg, addr)
        @parser.parse(msg) { |time, record|
          unless time && record
            log.warn "pattern not match: #{msg.inspect}"
            return
          end

          record[@source_hostname_key] = addr[2] if @source_hostname_key
          router.emit(@tag, time, record)
        }
      rescue => e
        log.error msg.dump, error: e, error_class: e.class, host: addr[3]
        log.error_backtrace
      end
    end
  end
end
