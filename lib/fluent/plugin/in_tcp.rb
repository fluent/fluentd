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

module Fluent::Plugin
  class TcpInput < Input
    Fluent::Plugin.register_input('tcp', self)

    helpers :server, :parser, :extract, :compat_parameters

    desc 'Tag of output events.'
    config_param :tag, :string
    desc 'The port to listen to.'
    config_param :port, :integer, default: 5170
    desc 'The bind address to listen to.'
    config_param :bind, :string, default: '0.0.0.0'

    desc "The field name of the client's hostname."
    config_param :source_host_key, :string, default: nil, deprecated: "use source_hostname_key instead."
    desc "The field name of the client's hostname."
    config_param :source_hostname_key, :string, default: nil
    desc "The field name of the client's address."
    config_param :source_address_key, :string, default: nil

    # Setting default to nil for backward compatibility
    desc "The max bytes of message."
    config_param :message_length_limit, :size, default: nil

    config_param :blocking_timeout, :time, default: 0.5

    desc 'The payload is read up to this character.'
    config_param :delimiter, :string, default: "\n" # syslog family add "\n" to each message and this seems only way to split messages in tcp stream
    desc 'Check the remote connection is still available by sending a keepalive packet if this value is true.'
    config_param :send_keepalive_packet, :bool, default: false

    # in_forward like host/network restriction
    config_section :security, required: false, multi: false do
      config_section :client, param_name: :clients, required: true, multi: true do
        desc 'The IP address or host name of the client'
        config_param :host, :string, default: nil
        desc 'Network address specification'
        config_param :network, :string, default: nil
      end
    end

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      parser_config = conf.elements('parse').first
      unless parser_config
        raise Fluent::ConfigError, "<parse> section is required."
      end
      super
      @_event_loop_blocking_timeout = @blocking_timeout
      @source_hostname_key ||= @source_host_key if @source_host_key

      @nodes = nil
      if @security
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
            rescue SocketError
              raise Fluent::ConfigError, "host '#{client.host}' cannot be resolved"
            end
          end
          source_addr = begin
                          IPAddr.new(source || client.network)
                        rescue ArgumentError
                          raise Fluent::ConfigError, "network '#{client.network}' address format is invalid"
                        end
          @nodes.push(source_addr)
        end
      end

      @parser = parser_create(conf: parser_config)
    end

    def multi_workers_ready?
      true
    end

    def start
      super

      log.info "listening tcp socket", bind: @bind, port: @port
      del_size = @delimiter.length
      discard_till_next_delimiter = false
      if @_extract_enabled && @_extract_tag_key
        server_create(:in_tcp_server_single_emit, @port, bind: @bind, resolve_name: !!@source_hostname_key, send_keepalive_packet: @send_keepalive_packet) do |data, conn|
          unless check_client(conn)
            conn.close
            next
          end

          conn.buffer << data
          buf = conn.buffer
          pos = 0
          while i = buf.index(@delimiter, pos)
            msg = buf[pos...i]
            pos = i + del_size

            if discard_till_next_delimiter
              discard_till_next_delimiter = false
              next
            end

            if !@message_length_limit.nil? && @message_length_limit < msg.bytesize
              log.info "The received data is larger than 'message_length_limit', dropped:", limit: @message_length_limit, size: msg.bytesize, head: msg[...32]
              next
            end

            @parser.parse(msg) do |time, record|
              unless time && record
                log.warn "pattern not matched", message: msg
                next
              end

              tag = extract_tag_from_record(record)
              tag ||= @tag
              time ||= extract_time_from_record(record) || Fluent::EventTime.now
              record[@source_address_key] = conn.remote_addr if @source_address_key
              record[@source_hostname_key] = conn.remote_host if @source_hostname_key
              router.emit(tag, time, record)
            end
          end
          buf.slice!(0, pos) if pos > 0
          # If the buffer size exceeds the limit here, it means that the next message will definitely exceed the limit.
          # So we should clear the buffer here. Otherwise, it will keep storing useless data until the next delimiter comes.
          if !@message_length_limit.nil? && @message_length_limit < buf.bytesize
            log.info "The buffer size exceeds 'message_length_limit', cleared:", limit: @message_length_limit, size: buf.bytesize, head: buf[...32]
            buf.clear
            # We should discard the subsequent data until the next delimiter comes.
            discard_till_next_delimiter = true
            next
          end
        end
      else
        server_create(:in_tcp_server_batch_emit, @port, bind: @bind, resolve_name: !!@source_hostname_key, send_keepalive_packet: @send_keepalive_packet) do |data, conn|
          unless check_client(conn)
            conn.close
            next
          end

          conn.buffer << data
          buf = conn.buffer
          pos = 0
          es = Fluent::MultiEventStream.new
          while i = buf.index(@delimiter, pos)
            msg = buf[pos...i]
            pos = i + del_size

            if discard_till_next_delimiter
              discard_till_next_delimiter = false
              next
            end

            if !@message_length_limit.nil? && @message_length_limit < msg.bytesize
              log.info "The received data is larger than 'message_length_limit', dropped:", limit: @message_length_limit, size: msg.bytesize, head: msg[...32]
              next
            end

            @parser.parse(msg) do |time, record|
              unless time && record
                log.warn "pattern not matched", message: msg
                next
              end

              time ||= extract_time_from_record(record) || Fluent::EventTime.now
              record[@source_address_key] = conn.remote_addr if @source_address_key
              record[@source_hostname_key] = conn.remote_host if @source_hostname_key
              es.add(time, record)
            end
          end
          router.emit_stream(@tag, es)
          buf.slice!(0, pos) if pos > 0
          # If the buffer size exceeds the limit here, it means that the next message will definitely exceed the limit.
          # So we should clear the buffer here. Otherwise, it will keep storing useless data until the next delimiter comes.
          if !@message_length_limit.nil? && @message_length_limit < buf.bytesize
            log.info "The buffer size exceeds 'message_length_limit', cleared:", limit: @message_length_limit, size: buf.bytesize, head: buf[...32]
            buf.clear
            # We should discard the subsequent data until the next delimiter comes.
            discard_till_next_delimiter = true
            next
          end
        end
      end
    end

    private

    def check_client(conn)
      if @nodes
        remote_addr = conn.remote_addr
        node = @nodes.find { |n| n.include?(remote_addr) rescue false }
        unless node
          log.warn "anonymous client '#{remote_addr}' denied"
          return false
        end
      end

      true
    end
  end
end
