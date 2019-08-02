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

require 'fluent/plugin/service_discovery'
require 'fluent/plugin/service_discovery/discovery_message'

module Fluent
  module Plugin
    class SdFile < ServiceDiscovery
      Plugin.register_sd('file', self)

      DEFAULT_FILE_TYPE = 'default'.freeze

      helpers :timer

      DEFAULT_SD_FILE_PATH = ENV['DEFAULT_SD_FILE_PATH'] || '/etc/fluent/sd.yaml'

      config_param :path, :string, default: DEFAULT_SD_FILE_PATH
      config_param :conf_encoding, :string, default: 'utf-8'
      config_param :refresh_interval, :time, default: 3

      def initialize
        super

        @paths = []
        @file_type = nil
        @last_modified = nil
        @diff = []
        @stop = false
      end

      def configure(conf)
        super

        unless File.exist?(@path)
          raise Fluent::ConfigError, "sd_file: path=#{@path} not found"
        end

        @file_type = File.basename(@path).split('.', 2).last.to_sym
        unless %i[yaml yml json].include?(@file_type)
          @file_type = DEFAULT_FILE_TYPE
        end

        @services = fetch_server_info
        @log.debug("sd_file will watch #{@paths.join(', ')}")
      end

      def start(queue)
        super()

        timer_execute(:sd_file_timer, @refresh_interval) do
          refresh_file(queue)
        end
      end

      def stop
        @stop = true
        super
      end

      private

      def parser
        @parser ||=
          case @file_type
          when :yaml, :yml
            require 'yaml'
            -> (v) { YAML.safe_load(v).map }
          when :json
            require 'json'
            -> (v) { JSON.parse(v) }
          else
            require 'fluent/config'
            config_fname = File.basename(@path)
            config_basedir = File.dirname(@path)
            -> (v) { Fluent::Config.parse(v, config_fname, config_basedir, :v1) }
          end
      end

      def refresh_file(queue)
        s =
          begin
            fetch_server_info
          rescue => e
            @log.warn("sd_file: #{e}")
          end

        if s.nil?
          return
        end

        diff = []
        join = s - @services
        # Need service_in first to ensure that server exist as least one.
        join.each do |j|
          diff << ServiceDiscovery::DiscoveryMessage.service_in(j)
        end

        drain = @services - s
        drain.each do |d|
          diff << ServiceDiscovery::DiscoveryMessage.service_out(d)
        end

        @services = s

        diff.each do |a|
          queue.push(a)
        end
      end

      def fetch_server_info
        if File.mtime(@path) == @last_modified
          @log.info('skip refresh server since not modified_chunks')
          return nil
        end

        log.info('fetch_server_info!!!!!!!!')
        @last_modified = File.mtime(@path)

        config_data =
          begin
            File.open(path, "r:#{@conf_encoding}:utf-8", &:read)
          rescue => e
            raise Fluent::ConfigError, "sd_file: path=#{path} can not open #{e}"
          end

        parser.call(config_data).map do |s|
          Service.new(
            :file,
            s.fetch('host'),
            s.fetch('port'),
            s['name'],
            s['weight'],
            s['standby'],
            s['username'],
            s['password'],
            s['shared_key'],
          )
        end
      end
    end
  end
end
