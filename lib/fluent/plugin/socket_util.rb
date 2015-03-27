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

require 'cool.io'

module Fluent
  # Obsolete in v0.14
  #   Use TCPServer / UDPServer of PluginSupport instead
  module SocketUtil
    def create_udp_socket(host)
      require 'ipaddr'

      if IPAddr.new(IPSocket.getaddress(host)).ipv4?
        UDPSocket.new
      else
        UDPSocket.new(Socket::AF_INET6)
      end
    end
    module_function :create_udp_socket

    # obsolete in v0.14
    class UdpHandler < Coolio::IO
      def initialize(io, log, body_size_limit, callback)
        super(io)
        @io = io
        @log = log
        @body_size_limit = body_size_limit
        @callback = callback
      end

      def on_readable
        msg, addr = @io.recvfrom_nonblock(@body_size_limit)
        msg.chomp!
        @callback.call(msg, addr)
      rescue => e
        @log.error "unexpected error", :error => e, :error_class => e.class
      end
    end

    # obsolete in v0.14
    class TcpHandler < Coolio::Socket
      PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]

      def initialize(io, log, delimiter, callback)
        super(io)
        if io.is_a?(TCPSocket)
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
        @log.error "unexpected error", :error => e, :error_class => e.class
        close
      end

      def on_close
        @log.trace { "closed fluent socket object_id=#{self.object_id}" }
      end
    end

    # obsolete in v0.14
    class BaseInput < Fluent::Input
      def initialize
        super
        require 'fluent/parser'
      end

      config_param :tag, :string
      config_param :format, :string
      config_param :port, :integer, :default => 5150
      config_param :bind, :string, :default => '0.0.0.0'
      config_param :source_host_key, :string, :default => nil
      config_param :blocking_timeout, :time, :default => 0.5

      def configure(conf)
        super

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
        log.error "unexpected error", :error => e, :error_class => e.class
        log.error_backtrace
      end

      private

      def on_message(msg, addr)
        @parser.parse(msg) { |time, record|
          unless time && record
            log.warn "pattern not match: #{msg.inspect}"
            return
          end

          record[@source_host_key] = addr[3] if @source_host_key
          router.emit(@tag, time, record)
        }
      rescue => e
        log.error msg.dump, :error => e, :error_class => e.class, :host => addr[3]
        log.error_backtrace
      end
    end
  end
end
