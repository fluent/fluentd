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

require 'resolv'

require 'fluent/plugin_helper'
require 'fluent/plugin/service_discovery'

module Fluent
  module Plugin
    class SrvServiceDiscovery < ServiceDiscovery
      include PluginHelper::Mixin

      Plugin.register_sd('srv', self)

      helpers :timer

      desc 'Service without underscore in RFC2782'
      config_param :service, :string
      desc 'Proto without underscore in RFC2782'
      config_param :proto, :string, default: 'tcp'
      desc 'Name without underscore in RFC2782'
      config_param :hostname, :string
      desc 'hostname of DNS server to request the SRV record'
      config_param :dns_server_host, :string, default: nil
      desc 'interval of requesting to DNS server'
      config_param :interval, :integer, default: 60
      desc "resolve hostname to IP addr of SRV's Target"
      config_param :dns_lookup, :bool, default: true
      desc 'The shared key per server'
      config_param :shared_key, :string, default: nil, secret: true
      desc 'The username for authentication'
      config_param :username, :string, default: ''
      desc 'The password for authentication'
      config_param :password, :string, default: '', secret: true

      def initialize
        super
        @target = nil
      end

      def configure(conf)
        super

        @target = "_#{@service}._#{@proto}.#{@hostname}"
        @dns_resolve =
          if @dns_server_host.nil?
            Resolv::DNS.new
          elsif @dns_server_host.include?(':') # e.g. 127.0.0.1:8600
            host, port = @dns_server_host.split(':', 2)
            Resolv::DNS.new(nameserver_port: [[host, port.to_i]])
          else
            Resolv::DNS.new(nameserver: @dns_server_host)
          end

        @services = fetch_srv_record
      end

      def start(queue)
        timer_execute(:"sd_srv_record_#{@target}", @interval) do
          refresh_srv_records(queue)
        end

        super()
      end

      private

      def refresh_srv_records(queue)
        s = begin
              fetch_srv_record
            rescue => e
              @log.error("sd_srv: #{e}")
              return
            end

        if s.nil? || s.empty?
          return
        end

        diff = []
        join = s - @services
        # Need service_in first to guarantee that server exist at least one all time.
        join.each do |j|
          diff << ServiceDiscovery.service_in_msg(j)
        end

        drain = @services - s
        drain.each do |d|
          diff << ServiceDiscovery.service_out_msg(d)
        end

        @services = s

        diff.each do |a|
          queue.push(a)
        end
      end

      def fetch_srv_record
        adders = @dns_resolve.getresources(@target, Resolv::DNS::Resource::IN::SRV)

        services = []

        adders.each do |addr|
          host = @dns_lookup ? dns_lookup!(addr.target) : addr.target
          services << [
            addr.priority,
            Service.new(:srv, host.to_s, addr.port.to_i, addr.target.to_s, addr.weight, false, @username, @password, @shared_key)
          ]
        end

        services.sort_by(&:first).flat_map { |s| s[1] }
      end

      def dns_lookup!(host)
        # may need to cache the result
        @dns_resolve.getaddress(host) # get first result for now
      end
    end
  end
end
