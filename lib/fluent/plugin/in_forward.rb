#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
    @cached_unpacker = $use_msgpack_5 ? nil : MessagePack::Unpacker.new
  end

  def shutdown
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @usock.close
    listen_address = (@bind == '0.0.0.0' ? '127.0.0.1' : @bind)
    TCPSocket.open(listen_address, @port) {|sock| }  # FIXME @thread.join blocks without this line
    @thread.join
    @lsock.close
  end

  def listen
    $log.info "listening fluent socket on #{@bind}:#{@port}"
    Coolio::TCPServer.new(@bind, @port, Handler, method(:on_message))
  end

  #config_param :path, :string, :default => DEFAULT_SOCKET_PATH
  #def listen
  #  if File.exist?(@path)
  #    File.unlink(@path)
  #  end
  #  FileUtils.mkdir_p File.dirname(@path)
  #  $log.debug "listening fluent socket on #{@path}"
  #  Coolio::UNIXServer.new(@path, Handler, method(:on_message))
  #end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
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
      es = MessagePackEventStream.new(entries, @cached_unpacker)
      Engine.emit_stream(tag, es)

    elsif entries.class == Array
      # Forward
      es = MultiEventStream.new
      entries.each {|e|
        time = e[0].to_i
        time = (now ||= Engine.now) if time == 0
        record = e[1]
        es.add(time, record)
      }
      Engine.emit_stream(tag, es)

    else
      # Message
      time = msg[1]
      time = Engine.now if time == 0
      record = msg[2]
      Engine.emit(tag, time, record)
    end
  end

  class Handler < Coolio::Socket
    def initialize(io, on_message)
      super(io)
      if io.is_a?(TCPSocket)
        opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
        io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
      end
      $log.trace { "accepted fluent socket object_id=#{self.object_id}" }
      @on_message = on_message
    end

    def on_connect
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

      (class<<self;self;end).module_eval do
        define_method(:on_read, m)
      end
      m.call(data)
    end

    def on_read_json(data)
      @y << data
    rescue
      $log.error "forward error: #{$!.to_s}"
      $log.error_backtrace
      close
    end

    def on_read_msgpack(data)
      @u.feed_each(data, &@on_message)
    rescue
      $log.error "forward error: #{$!.to_s}"
      $log.error_backtrace
      close
    end

    def on_close
      $log.trace { "closed fluent socket object_id=#{self.object_id}" }
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
    #$log.trace "heartbeat request from #{host}:#{port}"
    begin
      @usock.send "\0", 0, host, port
    rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
    end
  end
end


end

