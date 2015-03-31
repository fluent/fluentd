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

require 'fluent/plugin/input'
require 'fluent/plugin_support/tcp_server'
require 'fluent/plugin_support/udp_server'
require 'fluent/plugin_support/ssl_server'

require 'openssl'
require 'socket'

module Fluent::Plugin
  class ForwardInput < Fluent::Plugin::Input
    DEFAULT_FORWARD_PORT = 24224

    include Fluent::PluginSupport::TCPServer
    include Fluent::PluginSupport::UDPServer
    include Fluent::PluginSupport::SSLServer

    Fluent::Plugin.register_input('forward', self)

    # TODO: burst transferring mode

    config_param :self_hostname, :string, default: Socket.gethostname

    config_param :port, :integer, default: DEFAULT_FORWARD_PORT
    config_param :bind, :string, default: '0.0.0.0'

    config_param :keepalive, :integer, default: nil # [1-] seconds, 0/nil: don't allowed, -1: not to timeout

    config_param :disable_udp_heartbeat, :bool, default: false

    config_param :chunk_size_warn_limit, :size, default: nil
    config_param :chunk_size_limit, :size, default: nil

    config_section :ssl, param_name: :ssl_options, required: false, multi: false do
      config_param :version, default: :TLSv1_2 do |val|
        ver = val.sub('.', '_').to_sym
        unless OpenSSL::SSL::SSLContext::METHODS.include?(ver)
          raise Fluent::ConfigError, "Invalid SSL version in this environment:'#{val}'"
        end
        ver
      end
      config_param :ciphers, :string, default: nil
      config_param :cert_auto_generate, :bool, default: false

      # cert auto generation
      config_param :digest, default: OpenSSL::Digest::SHA256 do |val|
        begin
          eval("OpenSSL::Digest::#{val}")
        rescue NameError => e
          raise Fluent::ConfigError, "Invalid digest method name in this environment:'#{val}'"
        end
      end
      config_param :algorithm, default: OpenSSL::PKey::RSA do |val|
        begin
          eval("OpenSSL::PKey::#{val}")
        rescue NameError => e
          raise Fluent::ConfigError, "Invalid name for public key encryption in this environment:'#{val}'"
        end
      end
      config_param :key_length, :integer, default: 2048
      config_param :cert_country, :string, default: 'US'
      config_param :cert_state, :string, default: 'CA'
      config_param :cert_locality, :string, default: 'Mountain View'
      config_param :cert_common_name, :string, default: 'Fluentd forward plugin'

      # cert file
      config_param :cert_file, :string, default: nil
      config_param :key_file, :string, default: nil
      config_param :key_passphrase, :string, default: nil # you can use ENV w/ in-place ruby code
    end

    config_section :security, required: false, multi: false do
      config_param :shared_key, :string
      config_param :user_auth, :bool, default: false
      config_param :allow_anonymous_source, :bool, default: true

      ### User based authentication
      config_section :user, param_name: :users, required: false, multi: true do
        config_param :username, :string
        config_param :password, :string
      end

      ### Client ip/network authentication & per_host shared key
      config_section :client, param_name: :clients, required: false, multi: true do
        config_param :host, :string, default: nil
        config_param :network, :string, default: nil
        config_param :shared_key, :string, default: nil
        config_param :users, :array, default: []
      end
    end

    # SO_LINGER 0 to send RST rather than FIN to avoid lots of connections sitting in TIME_WAIT at src
    config_param :linger_timeout, :integer, default: 0
    config_param :backlog, :integer, default: nil

    def configure(conf)
      super

      if @ssl_options
        unless @ssl_options.cert_auto_generate
          opts = @ssl_options
          unless opts.cert_file && opts.key_file && opts.key_passphrase
            raise Fluent::ConfigError, "cert_file, key_file and key_passphrase are needed"
          end
        end
      end

      if @security
        if @security.user_auth && @security.users.empty?
          raise Fluent::ConfigError, "<user> sections required if user_auth enabled"
        end
        if !@security.allow_anonymous_source && @security.clients.empty?
          raise Fluent::ConfigError, "<client> sections required if allow_anonymous_source disabled"
        end

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
          @nodes = []
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

      server_keepalive = if @keepalive && @keepalive == -1
                           nil # infinity
                         elsif @keepalive.nil? || @keepalive == 0
                           @keepalive = nil
                           1 # don't do keepalive, but wait 1 second to read at least
                         else
                           @keepalive
                         end

      if @ssl_options # TCP+SSL
        cert, key = prepare_cert_key_pair(@ssl_options)
        version = @ssl_options.version
        ssl_server_listen(ssl_version: version, ciphers: @ssl_options.ciphers, cert: cert, key: key, port: @port, bind: @bind, keepalive: server_keepalive, linger_timeout: @linger_timeout, backlog: @backlog, &method(:handle_connection))
      else # TCP
        tcp_server_listen(port: @port, bind: @bind, keepalive: server_keepalive, linger_timeout: @linger_timeout, backlog: @backlog, &method(:handle_connection))
      end

      unless @disable_udp_heartbeat
        # UDP heartbeat
        udp_server_listen(port: @port, bind: @bind) do |sock|
          sock.read(size_limit: 1024) do |remote_addr, remote_port, data|
            begin
              sock.send(host: remote_addr, port: remote_port, data: "\0")
            rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
              # ignore errors
              # TODO debug log?
            end
          end
        end
      end
    end

    def shutdown
      super
      # nothing to do (maybe)
    end

    def prepare_cert_key_pair(opts)
      if opts.cert_auto_generate
        return ssl_server_generate_cert_key(
          digest: opts.digest,
          algorithm: opts.algorithm,
          key_length: opts.key_length,
          cert_country: opts.cert_country,
          cert_state: opts.cert_state,
          cert_locality: opts.cert_locality,
          cert_common_name: opts.cert_common_name
        )
      else
        return ssl_server_load_cert_key(
          cert_file_path: opts.cert_file,
          algorithm: opts.algorithm,
          key_file_path: opts.key_file,
          key_passphrase: opts.key_passphrase
        )
      end
    end

    def handle_connection(conn)
      send_data = ->(serializer, data){ conn.write serializer.call(data) }

      # TODO: trace logging to be connected this host!
      state = :established
      user_auth_salt = nil

      if @security
        # security enabled session MUST use MessagePack as serialization format
        state = :helo
        user_auth_salt = generate_salt
        send_data.call( :to_msgpack.to_proc, generate_helo(user_auth_salt) )
        state = :pingpong
      end

      read_messages(conn) do |msg, chunk_size, serializer|
        case state
        when :pingpong
          success, reason_or_salt, shared_key = self.check_ping(msg, conn.remote_addr, user_auth_salt)
          if not success
            send_data.call( serializer, generate_pong(false, reason_or_salt, shared_key) )
            conn.close
            next
          end
          send_data.call( serializer, generate_pong(true, reason_or_salt, shared_key) )

          # TODO: log.debug "connection established"
          state = :established
        when :established
          options = emit_message(msg, chunk_size, conn.remote_addr)
          if options && r = response(options)
            send_data.call( serializer, r )
            # TODO: logging message content
            # log.trace "sent response to fluent socket"
          end
          unless @keepalive
            conn.close
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
            parser = MessagePack::Unpacker.new
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

    def emit_message(msg, chunk_size, source)
      if msg.nil? # TCP heartbeat
        return
      end

      unless msg.is_a? Array
        log.warn "Invalid format for input data", source: source, type: msg.class.name
      end

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
        es = Fluent::MessagePackEventStream.new(entries)
        router.emit_stream(tag, es)
        option = msg[2]

      elsif entries.class == Array
        # Forward
        es = Fluent::MultiEventStream.new
        entries.each {|e|
          record = e[1]
          next if record.nil?
          time = e[0].to_i
          time = (now ||= Fluent::Engine.now) if time == 0
          es.add(time, record)
        }
        router.emit_stream(tag, es)
        option = msg[2]

      else
        # Message
        record = msg[2]
        return if record.nil?
        time = msg[1]
        time = Fluent::Engine.now if time == 0
        router.emit(tag, time, record)
        option = msg[3]
      end

      # return option for response
      option
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

    def generate_helo(user_auth_salt)
      log.debug "generating helo"
      # ['HELO', options(hash)]
      [ 'HELO', {'auth' => (@security ? user_auth_salt : ''), 'keepalive' => @allow_keepalive } ]
    end

    def check_ping(message, remote_addr, user_auth_salt)
      log.debug "checking ping"
      # ['PING', self_hostname, shared_key_salt, sha512_hex(shared_key_salt + self_hostname + shared_key), username || '', sha512_hex(auth_salt + username + password) || '']
      unless message.size == 6 && message[0] == 'PING'
        return false, 'invalid ping message'
      end
      ping, hostname, shared_key_salt, shared_key_hexdigest, username, password_digest = message

      node = @nodes.select{|n| n[:address].include?(remote_addr) rescue false }.first
      if !node && !@security.allow_anonymous_source
        log.warn "Anonymous client disallowed", address: remote_addr, hostname: hostname
        return false, "anonymous source host '#{remote_addr}' denied", nil
      end

      shared_key = node ? node[:shared_key] : @security.shared_key
      serverside = Digest::SHA512.new.update(shared_key_salt).update(hostname).update(shared_key).hexdigest
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

      return true, shared_key_salt, node[:shared_key]
    end

    def generate_pong(auth_result, reason_or_salt, shared_key)
      log.debug "generating pong"
      # ['PONG', bool(authentication result), 'reason if authentication failed', self_hostname, sha512_hex(salt + self_hostname + sharedkey)]
      unless auth_result
        return ['PONG', false, reason_or_salt, '', '']
      end

      shared_key_digest_hex = Digest::SHA512.new.update(reason_or_salt).update(@self_hostname).update(shared_key).hexdigest
      [ 'PONG', true, '', @self_hostname, shared_key_digest_hex ]
    end
  end
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
#     * auth: string or blank_string (string: authentication required, and its salt is this value)
#     * keepalive: bool (allowed or not)
# 4. (client) send PING
#   * ['PING', selfhostname, sharedkey_salt, sha512_hex(sharedkey_salt + selfhostname + sharedkey), username || '', sha512_hex(auth_salt + username + password) || '']
# 5. (server) check PING
#   * check sharedkey
#   * check username / password (if required)
#   * send PONG FAILURE if failed
#   * ['PONG', false, 'reason of authentication failure', '', '']
# 6. (server) send PONG
#   * ['PONG', bool(authentication result), 'reason if authentication failed', selfhostname, sha512_hex(salt + selfhostname + sharedkey)]
# 7. (client) check PONG
#   * check sharedkey
#   * disconnect when failed
# 8. connection established
#   * send data from client (until keepalive expiration)
