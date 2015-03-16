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
require 'fluent/parser'
require 'fluent/plugin_support/tcp_server'
require 'fluent/plugin_support/udp_server'

require 'yajl'

module Fluent::Plugin
  class SyslogInput < Fluent::Plugin::Input
    include Fluent::PluginSupport::UDPServer
    include Fluent::PluginSupport::TCPServer

    Fluent::Plugin.register_input('syslog', self)

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

    PRIORITY_MAP = {
      0  => 'emerg',
      1  => 'alert',
      2  => 'crit',
      3  => 'err',
      4  => 'warn',
      5  => 'notice',
      6  => 'info',
      7  => 'debug'
    }

    config_param :port, :integer, default: 5140
    config_param :bind, :string, default: '0.0.0.0'
    config_param :tag, :string
    config_param :protocol_type, default: :udp do |val|
      case val.downcase
      when 'tcp'
        :tcp
      when 'udp'
        :udp
      else
        raise Fluent::ConfigError, "syslog input protocol type should be 'tcp' or 'udp'"
      end
    end
    config_param :include_source_host, :bool, default: false
    config_param :source_host_key, :string, default: 'source_host'.freeze

    def configure(conf)
      super

      receive_data_method = nil

      if conf.has_key?('format')
        @parser = Fluent::Plugin.new_parser(conf['format'])
        @parser.configure(conf)
        receive_data_method = method(:receive_data_parser)
      else
        conf['with_priority'] = true
        @parser = Fluent::TextParser::SyslogParser.new
        @parser.configure(conf)
        @use_default = true
        receive_data_method = method(:receive_data_default)
      end

      (class << self; self; end).module_eval do
        define_method(:receive_data, receive_data_method)
      end
    end

    def start
      super

      if @protocol_type == :udp
        udp_server_listen(port: @port, bind: @bind, size_limit: 4096) do |remote_addr, remote_port, data| # UDP 1 data is 1 line
          data.chomp!
          receive_data(data, remote_addr)
        end
      else
        # syslog family add "\n" to each message and this seems only way to split messages in tcp/udp stream
        tcp_server_listen(port: @port, bind: @bind, keepalive: false) do |conn|
          conn.on_data(delimiter: "\n") do |line|
            receive_data(line, conn.remote_addr)
          end
        end
      end
    end

    private

    def receive_data_parser(data, addr)
      m = SYSLOG_REGEXP.match(data)
      unless m
        log.warn "invalid syslog message: #{data.dump}"
        return
      end
      pri = m[1].to_i
      text = m[2]

      @parser.parse(text) do |time, record|
        unless time && record
          log.warn "pattern not match: #{text.inspect}"
          return
        end

        record[@source_host_key] = addr if @include_source_host
        emit(pri, time, record)
      end
    rescue => e
      log.error data.dump, :error => e.to_s
      log.error_backtrace
    end

    def receive_data_default(data, addr)
      @parser.parse(data) do |time, record|
        unless time && record
          log.warn "invalid syslog message", data: data
          return
        end

        pri = record.delete('pri')
        record[@source_host_key] = addr if @include_source_host
        emit(pri, time, record)
      end
    rescue => e
      log.error data.dump, error: e.to_s
      log.error_backtrace
    end

    def emit(pri, time, record)
      facility = FACILITY_MAP[pri >> 3]
      priority = PRIORITY_MAP[pri & 0b111]

      tag = "#{@tag}.#{facility}.#{priority}"

      router.emit(tag, time, record)
    rescue => e
      log.error "syslog failed to emit", error: e.to_s, error_class: e.class.to_s, tag: tag, record: Yajl.dump(record)
    end
  end
end
