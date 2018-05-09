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
  class UdpInput < Input
    Fluent::Plugin.register_input('udp', self)

    helpers :server, :parser, :extract, :compat_parameters

    desc 'Tag of output events.'
    config_param :tag, :string
    desc 'The port to listen to.'
    config_param :port, :integer, default: 5160
    desc 'The bind address to listen to.'
    config_param :bind, :string, default: '0.0.0.0'

    desc "The field name of the client's hostname."
    config_param :source_host_key, :string, default: nil, deprecated: "use source_hostname_key instead."
    desc "The field name of the client's hostname."
    config_param :source_hostname_key, :string, default: nil

    desc "Deprecated parameter. Use message_length_limit instead"
    config_param :body_size_limit, :size, default: nil, deprecated: "use message_length_limit instead."
    desc "The max bytes of message"
    config_param :message_length_limit, :size, default: 4096
    desc "Remove newline from the end of incoming payload"
    config_param :remove_newline, :bool, default: true
    desc "The max size of socket receive buffer. SO_RCVBUF"
    config_param :receive_buffer_size, :size, default: nil

    config_param :blocking_timeout, :time, default: 0.5

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      super
      @_event_loop_blocking_timeout = @blocking_timeout
      @source_hostname_key ||= @source_host_key if @source_host_key
      @message_length_limit = @body_size_limit if @body_size_limit

      @parser = parser_create
    end

    def multi_workers_ready?
      true
    end

    def start
      super

      log.info "listening udp socket", bind: @bind, port: @port
      server_create(:in_udp_server, @port, proto: :udp, bind: @bind, resolve_name: !!@source_hostname_key, max_bytes: @message_length_limit, receive_buffer_size: @receive_buffer_size) do |data, sock|
        data.chomp! if @remove_newline
        begin
          @parser.parse(data) do |time, record|
            unless time && record
              log.warn "pattern not match", data: data
              next
            end

            tag = extract_tag_from_record(record)
            tag ||= @tag
            time ||= extract_time_from_record(record) || Fluent::EventTime.now
            record[@source_hostname_key] = sock.remote_host if @source_hostname_key
            router.emit(tag, time, record)
          end
        end
      end
    end
  end
end
