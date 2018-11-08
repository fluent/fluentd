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
require 'fluent/msgpack_factory'
require 'yajl'
require 'digest'
require 'securerandom'

module Fluent::Plugin
  class ForwardInput < Input
    Fluent::Plugin.register_input('forward', self)

    # See the wiki page below for protocol specification
    # https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1

    helpers :server

    LISTEN_PORT = 24224

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
    desc 'Try to resolve hostname from IP addresses or not.'
    config_param :resolve_hostname, :bool, default: nil
    desc 'Connections will be disconnected right after receiving first message if this value is true.'
    config_param :deny_keepalive, :bool, default: false

    desc 'Log warning if received chunk size is larger than this value.'
    config_param :chunk_size_warn_limit, :size, default: nil
    desc 'Received chunk is dropped if it is larger than this value.'
    config_param :chunk_size_limit, :size, default: nil
    desc 'Skip an event if incoming event is invalid.'
    config_param :skip_invalid_event, :bool, default: false

    desc "The field name of the client's source address."
    config_param :source_address_key, :string, default: nil
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

      if @source_hostname_key
        # TODO: add test
        if @resolve_hostname.nil?
          @resolve_hostname = true
        elsif !@resolve_hostname # user specifies "false" in config
          raise Fluent::ConfigError, "resolve_hostname must be true with source_hostname_key"
        end
      end
      @enable_field_injection = @source_address_key || @source_hostname_key

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

    def multi_workers_ready?
      true
    end

    HEARTBEAT_UDP_PAYLOAD = "\0"

    def start
      super

      shared_socket = system_config.workers > 1

      log.info "listening port", port: @port, bind: @bind
      server_create_connection(
        :in_forward_server, @port,
        bind: @bind,
        shared: shared_socket,
        resolve_name: @resolve_hostname,
        linger_timeout: @linger_timeout,
        backlog: @backlog,
        &method(:handle_connection)
      )

      server_create(:in_forward_server_udp_heartbeat, @port, shared: shared_socket, proto: :udp, bind: @bind, resolve_name: @resolve_hostname, max_bytes: 128) do |data, sock|
        log.trace "heartbeat udp data arrived", host: sock.remote_host, port: sock.remote_port, data: data
        begin
          sock.write HEARTBEAT_UDP_PAYLOAD
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          log.trace "error while heartbeat response", host: sock.remote_host, error: e
        end
      end
    end

    def handle_connection(conn)
      send_data = ->(serializer, data){ conn.write serializer.call(data) }

      log.trace "connected fluent socket", addr: conn.remote_addr, port: conn.remote_port
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

      log.trace "accepted fluent socket", addr: conn.remote_addr, port: conn.remote_port

      read_messages(conn) do |msg, chunk_size, serializer|
        case state
        when :pingpong
          success, reason_or_salt, shared_key = check_ping(msg, conn.remote_addr, user_auth_salt, nonce)
          unless success
            conn.on(:write_complete) { |c| c.close_after_write_complete }
            send_data.call(serializer, generate_pong(false, reason_or_salt, nonce, shared_key))
            next
          end
          send_data.call(serializer, generate_pong(true, reason_or_salt, nonce, shared_key))

          log.debug "connection established", address: conn.remote_addr, port: conn.remote_port
          state = :established
        when :established
          options = on_message(msg, chunk_size, conn)
          if options && r = response(options)
            log.trace "sent response to fluent socket", address: conn.remote_addr, response: r
            conn.on(:write_complete) { |c| c.close } if @deny_keepalive
            send_data.call(serializer, r)
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
      conn.data do |data|
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

    def on_message(msg, chunk_size, conn)
      if msg.nil?
        # for future TCP heartbeat_request
        return
      end

      # TODO: raise an exception if broken chunk is generated by recoverable situation
      unless msg.is_a?(Array)
        log.warn "incoming chunk is broken:", host: conn.remote_host, msg: msg
        return
      end

      tag = msg[0]
      entries = msg[1]

      if @chunk_size_limit && (chunk_size > @chunk_size_limit)
        log.warn "Input chunk size is larger than 'chunk_size_limit', dropped:", tag: tag, host: conn.remote_host, limit: @chunk_size_limit, size: chunk_size
        return
      elsif @chunk_size_warn_limit && (chunk_size > @chunk_size_warn_limit)
        log.warn "Input chunk size is larger than 'chunk_size_warn_limit':", tag: tag, host: conn.remote_host, limit: @chunk_size_warn_limit, size: chunk_size
      end

      case entries
      when String
        # PackedForward
        option = msg[2]
        size = (option && option['size']) || 0
        es_class = (option && option['compressed'] == 'gzip') ? Fluent::CompressedMessagePackEventStream : Fluent::MessagePackEventStream
        es = es_class.new(entries, nil, size.to_i)
        es = check_and_skip_invalid_event(tag, es, conn.remote_host) if @skip_invalid_event
        if @enable_field_injection
          es = add_source_info(es, conn)
        end
        router.emit_stream(tag, es)

      when Array
        # Forward
        es = if @skip_invalid_event
               check_and_skip_invalid_event(tag, entries, conn.remote_host)
             else
               es = Fluent::MultiEventStream.new
               entries.each { |e|
                 record = e[1]
                 next if record.nil?
                 time = e[0]
                 time = Fluent::Engine.now if time.nil? || time.to_i == 0 # `to_i == 0` for empty EventTime
                 es.add(time, record)
               }
               es
             end
        if @enable_field_injection
          es = add_source_info(es, conn)
        end
        router.emit_stream(tag, es)
        option = msg[2]

      else
        # Message
        time = msg[1]
        record = msg[2]
        if @skip_invalid_event && invalid_event?(tag, time, record)
          log.warn "got invalid event and drop it:", host: conn.remote_host, tag: tag, time: time, record: record
          return msg[3] # retry never succeeded so return ack and drop incoming event.
        end
        return if record.nil?
        time = Fluent::Engine.now if time.to_i == 0
        if @enable_field_injection
          record[@source_address_key] = conn.remote_addr if @source_address_key
          record[@source_hostname_key] = conn.remote_host if @source_hostname_key
        end
        router.emit(tag, time, record)
        option = msg[3]
      end

      # return option for response
      option
    end

    def invalid_event?(tag, time, record)
      !((time.is_a?(Integer) || time.is_a?(::Fluent::EventTime)) && record.is_a?(Hash) && tag.is_a?(String))
    end

    def check_and_skip_invalid_event(tag, es, remote_host)
      new_es = Fluent::MultiEventStream.new
      es.each { |time, record|
        if invalid_event?(tag, time, record)
          log.warn "skip invalid event:", host: remote_host, tag: tag, time: time, record: record
          next
        end
        new_es.add(time, record)
      }
      new_es
    end

    def add_source_info(es, conn)
      new_es = Fluent::MultiEventStream.new
      if @source_address_key && @source_hostname_key
        address = conn.remote_addr
        hostname = conn.remote_host
        es.each { |time, record|
          record[@source_address_key] = address
          record[@source_hostname_key] = hostname
          new_es.add(time, record)
        }
      elsif @source_address_key
        address = conn.remote_addr
        es.each { |time, record|
          record[@source_address_key] = address
          new_es.add(time, record)
        }
      elsif @source_hostname_key
        hostname = conn.remote_host
        es.each { |time, record|
          record[@source_hostname_key] = hostname
          new_es.add(time, record)
        }
      else
        raise "BUG: don't call this method in this case"
      end
      new_es
    end

    def select_authenticate_users(node, username)
      if node.nil? || node[:users].empty?
        @security.users.select{|u| u.username == username}
      else
        @security.users.select{|u| node[:users].include?(u.username) && u.username == username}
      end
    end

    def generate_salt
      ::SecureRandom.random_bytes(16)
    end

    def generate_helo(nonce, user_auth_salt)
      log.debug "generating helo"
      # ['HELO', options(hash)]
      ['HELO', {'nonce' => nonce, 'auth' => (@security ? user_auth_salt : ''), 'keepalive' => !@deny_keepalive}]
    end

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
  end
end
