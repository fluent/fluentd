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
    require 'msgpack'
  end

  def start
    @lsock = listen
    Thread.new(&method(:run))
  end

  def shutdown
    @lsock.close
  end

  #def listen
  #end

  private
  def run
    while true
      sock = @lsock.accept
      StreamInputThread.new(sock)
    end
  end

  class StreamInputThread < Thread
    def initialize(sock)
      @sock = sock
      @mpac = MessagePack::Unpacker.new(@sock)
      super(&method(:run))
    end

    def run
      $log.trace { "accepted fluent socket object_id=#{@sock.object_id}" }
      @mpac.each {|msg|
        on_message(msg)
      }
    ensure
      $log.trace { "closed fluent socket object_id=#{@sock.object_id}" }
      @sock.close
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
        array = entries.map {|e|
          time = e[0].to_i
          time = Engine.now if time == 0
          record = e[1]
          Event.new(time, record)
        }
        es = ArrayEventStream.new(array)
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
    TCPServer.new(@bind, @port)
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
    UNIXServer.new(@path)
  end
end


end

