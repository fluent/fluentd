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


class StreamInput < Input
  def initialize
    require 'socket'
  end

  def start
    @loop = Coolio::Loop.new
    @lsock = listen
    @loop.attach(@lsock)
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @lsock.close
    @loop.stop
    #@thread.join  # TODO
  end

  #def listen
  #end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  protected
  def callback(msgs)
    msgs.each {|msg|
      on_message(msg)
    }
  end

  # message Entry {
  #   1: long? time
  #   2: object record
  # }
  #
  # message Forward {
  #   1: string tag
  #   2: list<Entry> entries
  # }
  #
  # message Message {
  #   1: string tag
  #   2: long? time
  #   3: object record
  # }
  def on_message(msg)
    # TODO format error
    tag = msg[0].to_s
    entries = msg[1]

    if entries.class == Array
      # Forward
      entries.map! {|e|
        time = e[0].to_i
        time = (now ||= Engine.now) if time == 0
        record = e[1]
        Event.new(time, record)
      }
      es = ArrayEventStream.new(entries)
    else
      # Message
      time = msg[1]
      time = Engine.now if time == 0
      record = msg[2]
      event = Event.new(time, record)
      es = ArrayEventStream.new([event])
    end

    Engine.emit_stream(tag, es)
  end

  class Handler < Coolio::Socket
    def initialize(io, callback)
      super(io)
      opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
      io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
      $log.trace { "accepted fluent socket object_id=#{self.object_id}" }
      @callback = callback
    end

    def on_connect
      @u = MessagePack::Unpacker.new
    end

    def on_read(data)
      msgs = []
      @u.feed_each(data) {|msg|
        msgs << msg
      }
      @callback.call(msgs)
    end

    def on_close
      $log.trace { "closed fluent socket object_id=#{self.object_id}" }
    end
  end
end


class TcpInput < StreamInput
  Plugin.register_input('tcp', self)

  def configure(conf)
    @port = conf['port'] || DEFAULT_LISTEN_PORT
    @bind = conf['bind'] || '0.0.0.0'
  end

  def listen
    $log.debug "listening fluent socket on #{@bind}:#{@port}"
    Coolio::TCPServer.new(@bind, @port, Handler, method(:callback))
  end
end


class UnixInput < StreamInput
  Plugin.register_input('unix', self)

  def configure(conf)
    @path = conf['path'] || DEFAULT_SOCKET_PATH
  end

  def listen
    if File.exist?(@path)
      File.unlink(@path)
    end
    FileUtils.mkdir_p File.dirname(@path)
    $log.debug "listening fluent socket on #{@path}"
    Coolio::UNIXServer.new(@path, Handler, method(:callback))
  end
end


end

