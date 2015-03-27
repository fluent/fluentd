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

require 'fluent/plugin_support/tcp_server'
require 'fluent/parser'

module Fluent::Plugin
  class TcpInput < Fluent::Plugin::Input
    include Fluent::PluginSupport::TCPServer

    Fluent::Plugin.register_input('tcp', self)

    config_param :tag, :string
    config_param :format, :string
    config_param :port, :integer, :default => 5170
    config_param :bind, :string, :default => '0.0.0.0'
    config_param :source_host_key, :string, :default => nil
    config_param :delimiter, :string, :default => "\n" # syslog family add "\n" to each message and this seems only way to split messages in tcp stream

    def configure(conf)
      super

      @parser = Fluent::Plugin.new_parser(@format)
      @parser.configure(conf)
    end

    def start
      super

      tcp_server_listen(port: @port, bind: @bind, keepalive: false) do |conn|
        conn.on_data(delimiter: @delimiter) do |msg|
          @parser.parse(msg) do |time, record|
            unless time && record
              log.warn "pattern not match: #{msg.inspect}"
              return
            end

            record[@source_host_key] = addr[3] if @source_host_key
            router.emit(@tag, time, record)
          end
        end
      end
    end
  end
end
