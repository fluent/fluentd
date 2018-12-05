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

require 'fcntl'

require 'cool.io'
require 'yajl'

require 'fluent/input'

module Fluent
  class ForwardInput < Input
    Plugin.register_input('forward', self)

    LISTEN_PORT = 24224

    def initialize
      super
      require 'fluent/plugin/socket_util'
    end

    desc 'The port to listen to.'
    config_param :port, :integer, default: LISTEN_PORT
    desc 'The bind address to listen to.'
    config_param :bind, :string, default: '0.0.0.0'
    config_param :backlog, :integer, default: nil
    # SO_LINGER 0 to send RST rather than FIN to avoid lots of connections sitting in TIME_WAIT at src
    desc 'The timeout time used to set linger option.'
    config_param :linger_timeout, :integer, default: 0
    # This option is for Cool.io's loop wait timeout to avoid loop stuck at shutdown. Almost users don't need to change this value.
    config_param :blocking_timeout, :time, default: 0.5
    desc 'Connections will be disconnected right after receiving first message if this value is true.'
    config_param :deny_keepalive, :bool, default: false

    desc 'Log warning if received chunk size is larger than this value.'
    config_param :chunk_size_warn_limit, :size, default: nil
    desc 'Received chunk is dropped if it is larger than this value.'
    config_param :chunk_size_limit, :size, default: nil
    desc 'Skip an event if incoming event is invalid.'
    config_param :skip_invalid_event, :bool, default: false
    desc "The field name of the client's hostname."
    config_param :source_hostname_key, :string, default: nil

    config_section :security, required: false, multi: false do
      desc 'The hostname'
      config_param :self_hostname, :string
      desc 'Shared key for authentication'
      config_param :shared_key, :string, secret: true
      desc 'If true, use user based authentication'
      config_param :user_auth, :bool, default: false
      desc 'Allow anonymous source. <client> sections required if disabled.'
      config_param :allow_anonymous_source, :bool, default: true

      ### User based authentication
      config_section :user, param_name: :users, required: false, multi: true do
        desc 'The username for authentication'
        config_param :username, :string
        desc 'The password for authentication'
        config_param :password, :string, secret: true
      end

      ### Client ip/network authentication & per_host shared key
      config_section :client, param_name: :clients, required: false, multi: true do
        desc 'The IP address or host name of the client'
        config_param :host, :string, default: nil
        desc 'Network address specification'
        config_param :network, :string, default: nil
        desc 'Shared key per client'
        config_param :shared_key, :string, default: nil, secret: true
        desc 'Array of username.'
        config_param :users, :array, default: []
      end
    end

    def configure(conf)
      super

      if @security
        if @security.user_auth && @security.users.empty?
          raise Fluent::ConfigError, "<user> sections required if user_auth enabled"
        end
        if !@security.allow_anonymous_source && @security.clients.empty?
          raise Fluent::ConfigError, "<client> sections required if allow_anonymous_source disabled"
        end

        @nodes = []

        @security.clients.each do |client|
          if client.host && client.network
            raise Fluent::ConfigError, "both of 'host' and 'network' are specified for client"
          end
          if !client.host && !client.network
            raise Fluent::ConfigError, "Either of 'host' and 'network' must be specified for client"
          end
          source = nil
          if client.host
            begin
              source = IPSocket.getaddress(client.host)
            rescue SocketError => e
              raise Fluent::ConfigError, "host '#{client.host}' cannot be resolved"
            end
          end
          source_addr = begin
                          IPAddr.new(source || client.network)
                        rescue ArgumentError => e
                          raise Fluent::ConfigError, "network '#{client.network}' address format is invalid"
                        end
          @nodes.push({
              address: source_addr,
              shared_key: (client.shared_key || @security.shared_key),
              users: client.users
            })
        end
      end
    end

    def start
      super

      @loop = Coolio::Loop.new

      socket_manager_path = ENV['SERVERENGINE_SOCKETMANAGER_PATH']
      if Fluent.windows?
        socket_manager_path = socket_manager_path.to_i
      end
      client = ServerEngine::SocketManager::Client.new(socket_manager_path)

      @lsock = listen(client)
      @loop.attach(@lsock)

      @usock = client.listen_udp(@bind, @port)
      @usock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
      @hbr = HeartbeatRequestHandler.new(@usock, method(:on_heartbeat_request))
      @loop.attach(@hbr)

      @thread = Thread.new(&method(:run))
    end

    def shutdown
      # In test cases it occasionally appeared that when detaching a watcher, another watcher is also detached.
      # In the case in the iteration of watchers, a watcher that has been already detached is intended to be detached
      # and therfore RuntimeError occurs saying that it is not attached to a loop.
      # It occurs only when testing for sending responses to ForwardOutput.
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

      super
    end

    def listen(client)
      log.info "listening fluent socket on #{@bind}:#{@port}"
      sock = client.listen_tcp(@bind, @port)
      s = Coolio::TCPServer.new(sock, nil, Handler, @linger_timeout, log, method(:handle_connection))
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
      log.error "unexpected error", error: e
      log.error_backtrace
    end

    private

    def handle_connection(conn)
      send_data = ->(serializer, data){ conn.write serializer.call(data) }

      log.trace "connected fluent socket", address: conn.remote_addr, port: conn.remote_port
      state = :established
      nonce = nil
      user_auth_salt = nil

      if @security
        # security enabled session MUST use MessagePack as serialization format
        state = :helo
        nonce = generate_salt
        user_auth_salt = generate_salt
        send_data.call(:to_msgpack.to_proc, generate_helo(nonce, user_auth_salt))
        state = :pingpong
      end

      log.trace "accepted fluent socket", address: conn.remote_addr, port: conn.remote_port

      read_messages(conn) do |msg, chunk_size, serializer|
        case state
        when :pingpong
          success, reason_or_salt, shared_key = check_ping(msg, conn.remote_addr, user_auth_salt, nonce)
          unless success
            send_data.call(serializer, generate_pong(false, reason_or_salt, nonce, shared_key))
            conn.close
            next
          end
          send_data.call(serializer, generate_pong(true, reason_or_salt, nonce, shared_key))

          log.debug "connection established", address: conn.remote_addr, port: conn.remote_port
          state = :established
        when :established
          options = on_message(msg, chunk_size, conn.remote_addr)
          if options && r = response(options)
            send_data.call(serializer, r)
            log.trace "sent response to fluent socket", address: conn.remote_addr, response: r
            if @deny_keepalive
              conn.on_write_complete do
                conn.close
              end
            end
          else
            if @deny_keepalive
              conn.close
            end
          end
        else
          raise "BUG: unknown session state: #{state}"
        end
      end
    end

    def read_messages(conn, &block)
      feeder = nil
      serializer = nil
      bytes = 0
      conn.on_data do |data|
        # only for first call of callback
        unless feeder
          first = data[0]
          if first == '{' || first == '[' # json
            parser = Yajl::Parser.new
            parser.on_parse_complete = ->(obj){
              block.call(obj, bytes, serializer)
              bytes = 0
            }
            serializer = :to_json.to_proc
            feeder = ->(d){ parser << d }
          else # msgpack
            parser = Fluent::Engine.msgpack_factory.unpacker
            serializer = :to_msgpack.to_proc
            feeder = ->(d){
              parser.feed_each(d){|obj|
                block.call(obj, bytes, serializer)
                bytes = 0
              }
            }
          end
        end

        bytes += data.bytesize
        feeder.call(data)
      end
    end

    def response(option)
      if option && option['chunk']
        return { 'ack' => option['chunk'] }
      end
      nil
    end

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
    def on_message(msg, chunk_size, peeraddr)
      if msg.nil?
        # for future TCP heartbeat_request
        return
      end

      # TODO: raise an exception if broken chunk is generated by recoverable situation
      unless msg.is_a?(Array)
        log.warn "incoming chunk is broken:", source: source_message(peeraddr), msg: msg
        return
      end

      tag = msg[0]
      entries = msg[1]

      if @chunk_size_limit && (chunk_size > @chunk_size_limit)
        log.warn "Input chunk size is larger than 'chunk_size_limit', dropped:", tag: tag, source: source_message(peeraddr), limit: @chunk_size_limit, size: chunk_size
        return
      elsif @chunk_size_warn_limit && (chunk_size > @chunk_size_warn_limit)
        log.warn "Input chunk size is larger than 'chunk_size_warn_limit':", tag: tag, source: source_message(peeraddr), limit: @chunk_size_warn_limit, size: chunk_size
      end

      case entries
      when String
        # PackedForward
        option = msg[2]
        size = (option && option['size']) || 0
        es_class = (option && option['compressed'] == 'gzip') ? CompressedMessagePackEventStream : MessagePackEventStream
        es = es_class.new(entries, nil, size.to_i)
        es = check_and_skip_invalid_event(tag, es, peeraddr) if @skip_invalid_event
        es = add_source_host(es, peeraddr[2]) if @source_hostname_key
        router.emit_stream(tag, es)

      when Array
        # Forward
        es = if @skip_invalid_event
               check_and_skip_invalid_event(tag, entries, peeraddr)
             else
               es = MultiEventStream.new
               entries.each { |e|
                 record = e[1]
                 next if record.nil?
                 time = e[0]
                 time = (now ||= Engine.now) if time.to_i == 0
                 es.add(time, record)
               }
               es
             end
        es = add_source_host(es, peeraddr[2]) if @source_hostname_key
        router.emit_stream(tag, es)
        option = msg[2]

      else
        # Message
        time = msg[1]
        record = msg[2]
        if @skip_invalid_event && invalid_event?(tag, time, record)
          log.warn "got invalid event and drop it:", source: source_message(peeraddr), tag: tag, time: time, record: record
          return msg[3] # retry never succeeded so return ack and drop incoming event.
        end
        return if record.nil?
        time = Engine.now if time.to_i == 0
        record[@source_hostname_key] = peeraddr[2] if @source_hostname_key
        router.emit(tag, time, record)
        option = msg[3]
      end

      # return option for response
      option
    end

    def invalid_event?(tag, time, record)
      !((time.is_a?(Integer) || time.is_a?(::Fluent::EventTime)) && record.is_a?(Hash) && tag.is_a?(String))
    end

    def check_and_skip_invalid_event(tag, es, peeraddr)
      new_es = MultiEventStream.new
      es.each { |time, record|
        if invalid_event?(tag, time, record)
          log.warn "skip invalid event:", source: source_message(peeraddr), tag: tag, time: time, record: record
          next
        end
        new_es.add(time, record)
      }
      new_es
    end

    def add_source_host(es, host)
      new_es = MultiEventStream.new
      es.each { |time, record|
        record[@source_hostname_key] = host
        new_es.add(time, record)
      }
      new_es
    end

    def source_message(peeraddr)
      _, port, host, addr = peeraddr
      "host: #{host}, addr: #{addr}, port: #{port}"
    end

    def select_authenticate_users(node, username)
      if node.nil? || node[:users].empty?
        @security.users.select{|u| u.username == username}
      else
        @security.users.select{|u| node[:users].include?(u.username) && u.username == username}
      end
    end

    def generate_salt
      OpenSSL::Random.random_bytes(16)
    end

    def generate_helo(nonce, user_auth_salt)
      log.debug "generating helo"
      # ['HELO', options(hash)]
      ['HELO', {'nonce' => nonce, 'auth' => (@security ? user_auth_salt : ''), 'keepalive' => !@deny_keepalive}]
    end

    ##### Authentication Handshake
    #
    # 1. (client) connect to server
    #   * Socket handshake, checks certificate and its significate (in client, if using SSL)
    # 2. (server)
    #   * check network/domain acl (if enabled)
    #   * disconnect when failed
    # 3. (server) send HELO
    #   * ['HELO', options(hash)]
    #   * options:
    #     * nonce: string (required)
    #     * auth: string or blank_string (string: authentication required, and its salt is this value)
    # 4. (client) send PING
    #   * ['PING', selfhostname, sharedkey_salt, sha512_hex(sharedkey_salt + selfhostname + nonce + sharedkey), username || '', sha512_hex(auth_salt + username + password) || '']
    # 5. (server) check PING
    #   * check sharedkey
    #   * check username / password (if required)
    #   * send PONG FAILURE if failed
    #   * ['PONG', false, 'reason of authentication failure', '', '']
    # 6. (server) send PONG
    #   * ['PONG', bool(authentication result), 'reason if authentication failed', selfhostname, sha512_hex(salt + selfhostname + nonce + sharedkey)]
    # 7. (client) check PONG
    #   * check sharedkey
    #   * disconnect when failed
    # 8. connection established
    #   * send data from client

    def check_ping(message, remote_addr, user_auth_salt, nonce)
      log.debug "checking ping"
      # ['PING', self_hostname, shared_key_salt, sha512_hex(shared_key_salt + self_hostname + nonce + shared_key), username || '', sha512_hex(auth_salt + username + password) || '']
      unless message.size == 6 && message[0] == 'PING'
        return false, 'invalid ping message'
      end
      _ping, hostname, shared_key_salt, shared_key_hexdigest, username, password_digest = message

      node = @nodes.select{|n| n[:address].include?(remote_addr) rescue false }.first
      if !node && !@security.allow_anonymous_source
        log.warn "Anonymous client disallowed", address: remote_addr, hostname: hostname
        return false, "anonymous source host '#{remote_addr}' denied", nil
      end

      shared_key = node ? node[:shared_key] : @security.shared_key
      serverside = Digest::SHA512.new.update(shared_key_salt).update(hostname).update(nonce).update(shared_key).hexdigest
      if shared_key_hexdigest != serverside
        log.warn "Shared key mismatch", address: remote_addr, hostname: hostname
        return false, 'shared_key mismatch', nil
      end

      if @security.user_auth
        users = select_authenticate_users(node, username)
        success = false
        users.each do |user|
          passhash = Digest::SHA512.new.update(user_auth_salt).update(username).update(user[:password]).hexdigest
          success ||= (passhash == password_digest)
        end
        unless success
          log.warn "Authentication failed", address: remote_addr, hostname: hostname, username: username
          return false, 'username/password mismatch', nil
        end
      end

      return true, shared_key_salt, shared_key
    end

    def generate_pong(auth_result, reason_or_salt, nonce, shared_key)
      log.debug "generating pong"
      # ['PONG', bool(authentication result), 'reason if authentication failed', self_hostname, sha512_hex(salt + self_hostname + nonce + sharedkey)]
      unless auth_result
        return ['PONG', false, reason_or_salt, '', '']
      end

      shared_key_digest_hex = Digest::SHA512.new.update(reason_or_salt).update(@security.self_hostname).update(nonce).update(shared_key).hexdigest
      ['PONG', true, '', @security.self_hostname, shared_key_digest_hex]
    end

    class Handler < Coolio::Socket
      attr_reader :protocol, :remote_port, :remote_addr, :remote_host

      PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]

      def initialize(io, linger_timeout, log, on_connect_callback)
        super(io)

        @peeraddr = nil
        if io.is_a?(TCPSocket) # for unix domain socket support in the future
          @peeraddr = (io.peeraddr rescue PEERADDR_FAILED)
          opt = [1, linger_timeout].pack('I!I!')  # { int l_onoff; int l_linger; }
          io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
        end

        ### TODO: disabling name rev resolv
        proto, port, host, addr = ( io.peeraddr rescue PEERADDR_FAILED )
        if addr == '?'
          port, addr = *Socket.unpack_sockaddr_in(io.getpeername) rescue nil
        end
        @protocol = proto
        @remote_port = port
        @remote_addr = addr
        @remote_host = host
        @writing = false
        @closing = false
        @mutex = Mutex.new

        @chunk_counter = 0
        @on_connect_callback = on_connect_callback
        @log = log
        @log.trace {
          begin
            remote_port, remote_addr = *Socket.unpack_sockaddr_in(@_io.getpeername)
          rescue
            remote_port = nil
            remote_addr = nil
          end
          [ "accepted fluent socket", {address: remote_addr, port: remote_port, instance: self.object_id} ]
        }
      end

      def on_connect
        @on_connect_callback.call(self)
      end

      # API to register callback for data arrival
      def on_data(&callback)
        @on_read_callback = callback
      end

      def on_read(data)
        @on_read_callback.call(data)
      rescue => e
        @log.error "unexpected error on reading data from client", address: @remote_addr, error: e
        @log.error_backtrace
        close
      end

      def on_write_complete
        closing = @mutex.synchronize {
          @writing = false
          @closing
        }
        if closing
          close
        end
      end

      def close
        writing = @mutex.synchronize {
          @closing = true
          @writing
        }
        unless writing
          super
        end
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
