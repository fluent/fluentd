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

module Fluent::Plugin
  class ForwardInput < Fluent::Plugin::Input
    DEFAULT_FORWARD_PORT = 24224

    include Fluent::PluginSupport::TCPServer
    include Fluent::PluginSupport::UDPServer
    include Fluent::PluginSupport::SSLServer

    Fluent::Plugin.register_input('forward', self)

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
    end

    ### User based authentication
    config_section :user, param_name: :users do
      config_param :username, :string
      config_param :password, :string
    end

    ### Client ip/network authentication & per_host shared key
    config_section :client, param_name: :clients do
      config_param :host, :string, default: nil
      config_param :network, :string, default: nil
      config_param :shared_key, :string, default: nil
      config_param :users, :array, default: [] # array of string
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

      connection_handler = ->(conn){
        # TODO: trace logging to be connected this host!
        read_messages(conn) do |msg, chunk_size, serializer|
          # TODO: auth! auth!

          options = emit_message(msg, chunk_size, conn.remote_addr)
          if options && r = response(options)
            conn.write(serializer.call(r))
            # TODO: logging message content
            # log.trace "sent response to fluent socket"
          end
          unless @keepalive
            conn.close
          end
        end
      }

      if @ssl_options # TCP+SSL
        cert = key = nil
        opts = @ssl_options
        if opts.cert_auto_generate
          cert, key = ssl_server_generate_cert_key(
            digest: opts.digest,
            algorithm: opts.algorithm,
            key_length: opts.key_length,
            cert_country: opts.cert_country,
            cert_state: opts.cert_state,
            cert_locality: opts.cert_locality,
            cert_common_name: opts.cert_common_name
          )
        else
          cert, key = ssl_server_load_cert_key(
            cert_file_path: opts.cert_file,
            algorithm: opts.algorithm,
            key_file_path: opts.key_file,
            key_passphrase: opts.key_passphrase
          )
        end
        version = opts.version
        ssl_server_listen(ssl_version: version, ciphers: opts.ciphers, cert: cert, key: key, port: @port, bind: @bind, keepalive: server_keepalive, &connection_handler)
      else # TCP
        tcp_server_listen(port: @port, bind: @bind, keepalive: server_keepalive, backlog: @backlog, &connection_handler)
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
            feeder = ->(data){ parser << data }
          else # msgpack
            parser = MessagePack::Unpacker.new
            serializer = :to_msgpack.to_proc
            feeder = ->(data){
              parser.feed_each(data){|obj|
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

    def emit_message(msg, chunk_size, source) # TODO: fix my name
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
  end
end

