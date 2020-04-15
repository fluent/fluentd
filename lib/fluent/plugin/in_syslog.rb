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
require 'fluent/config/error'
require 'fluent/plugin/parser'

require 'yajl'

module Fluent::Plugin
  class SyslogInput < Input
    Fluent::Plugin.register_input('syslog', self)

    helpers :parser, :compat_parameters, :server

    DEFAULT_PARSER = 'syslog'
    SYSLOG_REGEXP = /^\<([0-9]+)\>(.*)/

    FACILITY_MAP = {
      0   => 'kern',
      1   => 'user',
      2   => 'mail',
      3   => 'daemon',
      4   => 'auth',
      5   => 'syslog',
      6   => 'lpr',
      7   => 'news',
      8   => 'uucp',
      9   => 'cron',
      10  => 'authpriv',
      11  => 'ftp',
      12  => 'ntp',
      13  => 'audit',
      14  => 'alert',
      15  => 'at',
      16  => 'local0',
      17  => 'local1',
      18  => 'local2',
      19  => 'local3',
      20  => 'local4',
      21  => 'local5',
      22  => 'local6',
      23  => 'local7'
    }

    SEVERITY_MAP = {
      0  => 'emerg',
      1  => 'alert',
      2  => 'crit',
      3  => 'err',
      4  => 'warn',
      5  => 'notice',
      6  => 'info',
      7  => 'debug'
    }

    desc 'The port to listen to.'
    config_param :port, :integer, default: 5140
    desc 'The bind address to listen to.'
    config_param :bind, :string, default: '0.0.0.0'
    desc 'The prefix of the tag. The tag itself is generated by the tag prefix, facility level, and priority.'
    config_param :tag, :string
    desc 'The transport protocol used to receive logs.(udp, tcp)'
    config_param :protocol_type, :enum, list: [:tcp, :udp], default: nil, deprecated: "use transport directive"
    desc 'The message frame type.(traditional, octet_count)'
    config_param :frame_type, :enum, list: [:traditional, :octet_count], default: :traditional

    desc 'If true, add source host to event record.'
    config_param :include_source_host, :bool, default: false, deprecated: 'use "source_hostname_key" or "source_address_key" instead.'
    desc 'Specify key of source host when include_source_host is true.'
    config_param :source_host_key, :string, default: 'source_host'.freeze
    desc 'Enable the option to emit unmatched lines.'
    config_param :emit_unmatched_lines, :bool, default: false

    desc 'The field name of hostname of sender.'
    config_param :source_hostname_key, :string, default: nil
    desc 'Try to resolve hostname from IP addresses or not.'
    config_param :resolve_hostname, :bool, default: nil
    desc 'The field name of source address of sender.'
    config_param :source_address_key, :string, default: nil
    desc 'The field name of the severity.'
    config_param :severity_key, :string, default: nil, alias: :priority_key
    desc 'The field name of the facility.'
    config_param :facility_key, :string, default: nil

    desc "The max bytes of message"
    config_param :message_length_limit, :size, default: 2048

    config_param :blocking_timeout, :time, default: 0.5

    desc 'The delimiter value "\n"'
    config_param :delimiter, :string, default: "\n" # syslog family add "\n" to each message

    config_section :parse do
      config_set_default :@type, DEFAULT_PARSER
      config_param :with_priority, :bool, default: true
    end

    # overwrite server plugin to change default to :udp
    config_section :transport, required: false, multi: false, init: true, param_name: :transport_config do
      config_argument :protocol, :enum, list: [:tcp, :udp, :tls], default: :udp
    end

    def configure(conf)
      compat_parameters_convert(conf, :parser)

      super

      if conf.has_key?('priority_key')
        log.warn "priority_key is deprecated. Use severity_key instead"
      end

      @use_default = false

      @parser = parser_create
      @parser_parse_priority = @parser.respond_to?(:with_priority) && @parser.with_priority

      if @include_source_host
        if @source_address_key
          raise Fluent::ConfigError, "specify either source_address_key or include_source_host"
        end
        @source_address_key = @source_host_key
      end
      if @source_hostname_key
        if @resolve_hostname.nil?
          @resolve_hostname = true
        elsif !@resolve_hostname # user specifies "false" in config
          raise Fluent::ConfigError, "resolve_hostname must be true with source_hostname_key"
        end
      end

      @_event_loop_run_timeout = @blocking_timeout
    end

    def multi_workers_ready?
      true
    end

    def start
      super

      log.info "listening syslog socket on #{@bind}:#{@port} with #{@protocol_type || @transport_config.protocol}"
      case @protocol_type || @transport_config.protocol
      when :udp then start_udp_server
      when :tcp then start_tcp_server
      when :tls then start_tcp_server(tls: true)
      else
        raise "BUG: invalid transport value: #{@protocol_type || @transport_config.protocol}"
      end
    end

    def start_udp_server
      server_create_udp(:in_syslog_udp_server, @port, bind: @bind, max_bytes: @message_length_limit, resolve_name: @resolve_hostname) do |data, sock|
        message_handler(data.chomp, sock)
      end
    end

    def start_tcp_server(tls: false)
      octet_count_frame = @frame_type == :octet_count

      delimiter = octet_count_frame ? " " : @delimiter
      delimiter_size = delimiter.size
      server_create_connection(tls ? :in_syslog_tls_server : :in_syslog_tcp_server, @port, bind: @bind, resolve_name: @resolve_hostname) do |conn|
        conn.data do |data|
          buffer = conn.buffer
          buffer << data
          pos = 0
          if octet_count_frame
            while idx = buffer.index(delimiter, pos)
              num = Integer(buffer[pos..idx])
              msg = buffer[idx + delimiter_size, num]
              if msg.size != num
                break
              end

              pos = idx + delimiter_size + num
              message_handler(msg, conn)
            end
          else
            while idx = buffer.index(delimiter, pos)
              msg = buffer[pos...idx]
              pos = idx + delimiter_size
              message_handler(msg, conn)
            end
          end
          buffer.slice!(0, pos) if pos > 0
        end
      end
    end

    private

    def emit_unmatched(data, sock)
      record = {"unmatched_line" => data}
      record[@source_address_key] = sock.remote_addr if @source_address_key
      record[@source_hostname_key] = sock.remote_host if @source_hostname_key
      emit("#{@tag}.unmatched", Fluent::EventTime.now, record)
    end

    def message_handler(data, sock)
      pri = nil
      text = data
      unless @parser_parse_priority
        m = SYSLOG_REGEXP.match(data)
        unless m
          if @emit_unmatched_lines
            emit_unmatched(data, sock)
          end
          log.warn "invalid syslog message: #{data.dump}"
          return
        end
        pri = m[1].to_i
        text = m[2]
      end

      @parser.parse(text) do |time, record|
        unless time && record
          if @emit_unmatched_lines
            emit_unmatched(data, sock)
          end
          log.warn "failed to parse message", data: data
          return
        end

        pri ||= record.delete('pri').to_i
        facility = FACILITY_MAP[pri >> 3]
        severity = SEVERITY_MAP[pri & 0b111]

        record[@severity_key] = severity if @severity_key
        record[@facility_key] = facility if @facility_key
        record[@source_address_key] = sock.remote_addr if @source_address_key
        record[@source_hostname_key] = sock.remote_host if @source_hostname_key

        tag = "#{@tag}.#{facility}.#{severity}"
        emit(tag, time, record)
      end
    rescue => e
      if @emit_unmatched_lines
        emit_unmatched(data, sock)
      end
      log.error "invalid input", data: data, error: e
      log.error_backtrace
    end

    def emit(tag, time, record)
      router.emit(tag, time, record)
    rescue => e
      log.error "syslog failed to emit", error: e, tag: tag, record: Yajl.dump(record)
    end
  end
end
