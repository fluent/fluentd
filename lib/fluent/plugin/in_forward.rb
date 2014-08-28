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

module Fluent
  class ForwardInput < Input
    Plugin.register_input('forward', self)

    def initialize
      super
      require 'fluent/plugin/socket_util'
    end

    config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
    config_param :bind, :string, :default => '0.0.0.0'
    config_param :backlog, :integer, :default => nil
    # SO_LINGER 0 to send RST rather than FIN to avoid lots of connections sitting in TIME_WAIT at src
    config_param :linger_timeout, :integer, :default => 0
    # This option is for Cool.io's loop wait timeout to avoid loop stuck at shutdown. Almost users don't need to change this value.
    config_param :blocking_timeout, :time, :default => 0.5

    config_param :chunk_size_warn_limit, :size, :default => nil
    config_param :chunk_size_limit, :size, :default => nil

    def configure(conf)
      super
    end

    def start
      @loop = Coolio::Loop.new

      @lsock = listen
      @loop.attach(@lsock)

      @usock = SocketUtil.create_udp_socket(@bind)
      @usock.bind(@bind, @port)
      @usock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
      @hbr = HeartbeatRequestHandler.new(@usock, method(:on_heartbeat_request))
      @loop.attach(@hbr)

      @thread = Thread.new(&method(:run))
    end

    def shutdown
      # In test cases it occasionally appeared that when detaching a watcher, another watcher is also detached.
      # In the case in the iteration of watchers, a watcher that has been already detached is intended to be detached
      # and therfore RuntimeError occurs saying that it is not attached to a loop.
      # It occures only when testing for sending responses to ForwardOutput.
      # Sending responses needs to write the socket that is previously used only to read
      # and a handler has 2 watchers that is used to read and to write.
      # This problem occurs possibly because those watchers are thought to be related to each other
      # and when detaching one of them the other is also detached for some reasons.
      # As a workaround, check if watchers are attached before detaching them.
      @loop.watchers.each {|w| w.detach if w.attached? }
      @loop.stop
      @usock.close
      @thread.join
      @lsock.close
    end

    def listen
      log.info "listening fluent socket on #{@bind}:#{@port}"
      s = Coolio::TCPServer.new(@bind, @port, Handler, @linger_timeout, log, method(:on_message))
      s.listen(@backlog) unless @backlog.nil?
      s
    end

    #config_param :path, :string, :default => DEFAULT_SOCKET_PATH
    #def listen
    #  if File.exist?(@path)
    #    File.unlink(@path)
    #  end
    #  FileUtils.mkdir_p File.dirname(@path)
    #  log.debug "listening fluent socket on #{@path}"
    #  Coolio::UNIXServer.new(@path, Handler, method(:on_message))
    #end

    def run
      @loop.run(@blocking_timeout)
    rescue => e
      log.error "unexpected error", :error => e, :error_class => e.class
      log.error_backtrace
    end

    protected

    # message Entry {
    #   1: long time
    #   2: object record
    # }
    #
    # message Forward {
    #   1: string tag
    #   2: list<Entry> entries
    #   3: object option (optional)
    # }
    #
    # message PackedForward {
    #   1: string tag
    #   2: raw entries  # msgpack stream of Entry
    #   3: object option (optional)
    # }
    #
    # message Message {
    #   1: string tag
    #   2: long? time
    #   3: object record
    #   4: object option (optional)
    # }
    def on_message(msg, chunk_size, source)
      if msg.nil?
        # for future TCP heartbeat_request
        return
      end

      # TODO format error
      tag = msg[0].to_s
      entries = msg[1]

      if @chunk_size_limit && (chunk_size > @chunk_size_limit)
        log.warn "Input chunk size is larger than 'chunk_size_limit', dropped:", tag: tag, source: source, limit: @chunk_size_limit, size: chunk_size
        return
      elsif @chunk_size_warn_limit && (chunk_size > @chunk_size_warn_limit)
        log.warn "Input chunk size is larger than 'chunk_size_warn_limit':", tag: tag, source: source, limit: @chunk_size_warn_limit, size: chunk_size
      end

      if entries.class == String
        # PackedForward
        es = MessagePackEventStream.new(entries)
        Engine.emit_stream(tag, es)
        option = msg[2]

      elsif entries.class == Array
        # Forward
        es = MultiEventStream.new
        entries.each {|e|
          record = e[1]
          next if record.nil?
          time = e[0].to_i
          time = (now ||= Engine.now) if time == 0
          es.add(time, record)
        }
        Engine.emit_stream(tag, es)
        option = msg[2]

      else
        # Message
        record = msg[2]
        return if record.nil?
        time = msg[1]
        time = Engine.now if time == 0
        Engine.emit(tag, time, record)
        option = msg[3]
      end

      # return option for response
      option
    end

    class Handler < Coolio::Socket
      PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]

      def initialize(io, linger_timeout, log, on_message)
        super(io)

        if io.is_a?(TCPSocket) # for unix domain socket support in the future
          proto, port, host, addr = ( io.peeraddr rescue PEERADDR_FAILED )
          @source = "host: #{host}, addr: #{addr}, port: #{port}"

          opt = [1, linger_timeout].pack('I!I!')  # { int l_onoff; int l_linger; }
          io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
        end

        @chunk_counter = 0
        @on_message = on_message
        @log = log
        @log.trace {
          begin
            remote_port, remote_addr = *Socket.unpack_sockaddr_in(@_io.getpeername)
          rescue => e
            remote_port = nil
            remote_addr = nil
          end
          "accepted fluent socket from '#{remote_addr}:#{remote_port}': object_id=#{self.object_id}"
        }
      end

      def on_connect
      end

      def on_read(data)
        first = data[0]
        if first == '{' || first == '['
          m = method(:on_read_json)
          @serializer = :to_json.to_proc
          @y = Yajl::Parser.new
          @y.on_parse_complete = lambda { |obj|
            option = @on_message.call(obj, @chunk_counter, @source)
            respond option if option
            @chunk_counter = 0
          }
        else
          m = method(:on_read_msgpack)
          @serializer = :to_msgpack.to_proc
          @u = MessagePack::Unpacker.new
        end

        (class << self; self; end).module_eval do
          define_method(:on_read, m)
        end
        m.call(data)
      end

      def on_read_json(data)
        @chunk_counter += data.bytesize
        @y << data
      rescue => e
        @log.error "forward error", :error => e, :error_class => e.class
        @log.error_backtrace
        close
      end

      def on_read_msgpack(data)
        @chunk_counter += data.bytesize
        @u.feed_each(data) do |obj|
          option = @on_message.call(obj, @chunk_counter, @source)
          respond option if option
          @chunk_counter = 0
        end
      rescue => e
        @log.error "forward error", :error => e, :error_class => e.class
        @log.error_backtrace
        close
      end

      def respond(option)
        if option && option['seq']
          res = { 'ack' => option['seq'] }
          write @serializer.call(res)
          @log.trace { "sent response to fluent socket object_id=#{self.object_id}" }
        end
      end

      def on_close
        @log.trace { "closed fluent socket object_id=#{self.object_id}" }
      end
    end

    class HeartbeatRequestHandler < Coolio::IO
      def initialize(io, callback)
        super(io)
        @io = io
        @callback = callback
      end

      def on_readable
        begin
          msg, addr = @io.recvfrom(1024)
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          return
        end
        host = addr[3]
        port = addr[1]
        @callback.call(host, port, msg)
      rescue
        # TODO log?
      end
    end

    def on_heartbeat_request(host, port, msg)
      #log.trace "heartbeat request from #{host}:#{port}"
      begin
        @usock.send "\0", 0, host, port
      rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
      end
    end
  end
end
