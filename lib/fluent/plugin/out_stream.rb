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


class StreamOutput < BufferedOutput
  def initialize
    super
    require 'socket'
    require 'fileutils'
    @send_timeout = 60
  end

  def configure(conf)
    super

    if send_timeout = conf['send_timeout']
      @send_timeout = Config.time_value(send_timeout).to_i
    end
  end

  def format_stream(tag, es)
    entries = []
    es.each {|event|
      entries << event
    }
    [tag, entries].to_msgpack
  end

  def write(chunk)
    sock = connect
    begin
      opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
      sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

      opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
      sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

      chunk.write_to(sock)
    ensure
      sock.close
    end
  end

  def flush_secondary(secondary)
    unless secondary.is_a?(StreamOutput)
      secondary = ReformatWriter.new(secondary)
    end
    while @buffer.pop(secondary)
    end
  end

  class ReformatWriter
    def initialize(secondary)
      @secondary = secondary
    end

    def write(chunk)
      chain = NullOutputChain.instance
      chunk.open {|io|
        u = MessagePack::Unpacker.new(io)
        begin
          u.each {|(tag,entries)|
            entries.map! {|o| Event.new.from_msgpack(o) }
            es = ArrayEventStream.new(entries)
            @secondary.emit(tag, es, chain)
          }
        rescue EOFError
        end
      }
    end
  end
end


class TcpOutput < StreamOutput
  Plugin.register_output('tcp', self)

  def initialize
    super
    @port = DEFAULT_LISTEN_PORT
  end

  def configure(conf)
    super

    @port = conf['port'] || @port
    @port = @port.to_i

    if host = conf['host']
      @host = host
    else
      raise ConfigError, "'host' parameter is required on tcp output"
    end
  end

  def connect
    TCPSocket.new(@host, @port)
  end
end


class UnixOutput < StreamOutput
  Plugin.register_output('unix', self)

  def initialize
    super
    @path = ''
  end

  def configure(conf)
    super

    if path = conf['path']
      @path = path
    else
      raise ConfigError, "'path' parameter is required on unix output"
    end
  end

  def connect
    UNIXSocket.new(@path)
  end
end


end

