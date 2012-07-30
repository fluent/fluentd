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
  end

  config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
  config_param :bind, :string, :default => '0.0.0.0'
  # encrypt
  config_param :password, :string, :default => nil

  def configure(conf)
    super
  end

  def start
    @loop = Coolio::Loop.new

    @lsock = listen
    @loop.attach(@lsock)

    @usock = UDPSocket.new
    @usock.bind(@bind, @port)
    @hbr = HeartbeatRequestHandler.new(@usock, method(:on_heartbeat_request))
    @loop.attach(@hbr)

    @thread = Thread.new(&method(:run))
    @cached_unpacker = MessagePack::Unpacker.new
  end

  def shutdown
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @usock.close
    TCPSocket.open('127.0.0.1', @port) {|sock| }  # FIXME @thread.join blocks without this line
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
  def decrypt(encrypted, password, salt = '')
    cipher = OpenSSL::Cipher::BF.new
    key_iv = OpenSSL::PKCS5.pbkdf2_hmac_sha1(password, salt, 2000, cipher.key_len + cipher.iv_len)
    key = key_iv[0, cipher.key_len]
    iv = key_iv[cipher.key_len, cipher.iv_len]
    cipher.key = key
    cipher.iv = iv
    cipher.decrypt
    
    decrypted_data = ''
    decrypted_data << cipher.update(encrypted)
    decrypted_data << cipher.final
    decrypted_data
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
  #   3: hash attribute # msgpack stream of attribute
  # }
  #
  # message Message {
  #   1: string tag
  #   2: long? time
  #   3: object record
  # }
  def on_message(msg)
    # TODO format error
    attribute = (msg.length == 3 && msg[1].class == String && msg[2].class == Hash) ? msg.last : {}
    attribute_detail = (attribute['detail']) ? attribute['detail'] : {}
    tag = msg[0].to_s
    entries = msg[1]

    if attribute['encrypted']
      # attribute detail
      packed_attribute_detail = decrypt(attribute['detail'], @password, attribute['salt'])
      attribute_detail = MessagePack.unpack(packed_attribute_detail)
      # tag
      encrypted_tag = tag.dump
      tag = decrypt(tag, @password, attribute['salt'])
      decrypted_tag = tag.dump
      $log.trace("tag decrypted(dump): [#{encrypted_tag}] -> [#{decrypted_tag}]")
      # entries
      entries = decrypt(entries, @password, attribute['salt'])
    end
    attribute['detail'] = attribute_detail
    $log.trace("attribute:#{attribute}")
    if attribute_detail['compressed']
      # entries
      before_size = entries.bytesize
      entries = Zlib::Inflate.inflate(entries)
      after_size = entries.bytesize
      percent = sprintf("%.2f", after_size.to_f / before_size.to_f * 100)
      $log.trace("entries inflated: #{before_size} bytes to #{after_size} bytes (#{percent} %)")
    end

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
      msg, addr = @io.recvfrom(1024)
      host = addr[3]
      port = addr[1]
      @callback.call(host, port, msg)
    rescue
      # TODO log?
    end
  end

  def on_heartbeat_request(host, port, msg)
    #$log.trace "heartbeat request from #{host}:#{port}"
    @usock.send "", 0, host, port
  end
end


end

