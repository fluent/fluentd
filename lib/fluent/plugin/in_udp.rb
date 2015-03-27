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

require 'fluent/plugin_support/udp_server'
require 'fluent/parser'

module Fluent::Plugin
  class UdpInput < Fluent::Plugin::Input
    include Fluent::PluginSupport::UDPServer

    Fluent::Plugin.register_input('udp', self)

    config_param :tag, :string
    config_param :format, :string
    config_param :port, :integer, :default => 5160
    config_param :bind, :string, :default => '0.0.0.0'
    config_param :source_host_key, :string, :default => nil
    config_param :body_size_limit, :size, :default => 4096

    def configure(conf)
      super

      @parser = Fluent::Plugin.new_parser(@format)
      @parser.configure(conf)
    end

    def start
      super

      udp_server_read(port: @port, bind: @bind, size_limit: @body_size_limit) do |remote_addr, remote_port, data| # UDP 1 data is 1 line
        @parser.parse(data) do |time, record|
          data.chomp!
          unless time && record
            log.warn "pattern not match: #{data.inspect}"
            return
          end

          record[@source_host_key] = remote_host if @source_host_key
          router.emit(@tag, time, record)
        end
      end
    end
  end
end
