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
    require 'socket'
    require 'fileutils'
    super
  end

  config_param :send_timeout, :time, :default => 60

  def configure(conf)
    super
  end

  def format_stream(tag, es)
    # use PackedForward
    [tag, es.to_msgpack_stream].to_msgpack
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
    @buffer.pop(secondary)
  end

  class ReformatWriter
    def initialize(secondary)
      @secondary = secondary
    end

    def write(chunk)
      chain = NullOutputChain.instance
      chunk.open {|io|
        # TODO use MessagePackIoEventStream
        u = MessagePack::Unpacker.new(io)
        begin
          u.each {|(tag,entries)|
            es = MultiEventStream.new
            entries.each {|o|
              es.add(o[0], o[1])
            }
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
  end

  config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
  config_param :host, :string

  def configure(conf)
    super
  end

  def connect
    TCPSocket.new(@host, @port)
  end
end


class UnixOutput < StreamOutput
  Plugin.register_output('unix', self)

  def initialize
    super
  end

  config_param :path, :string

  def configure(conf)
    super
  end

  def connect
    UNIXSocket.new(@path)
  end
end


end

