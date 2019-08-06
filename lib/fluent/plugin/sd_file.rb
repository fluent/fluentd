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

require 'cool.io'

require 'fluent/plugin/service_discovery'
require 'fluent/plugin/service_discovery/discovery_message'

module Fluent
  module Plugin
    class SdFile < ServiceDiscovery
      Plugin.register_sd('file', self)

      DEFAULT_FILE_TYPE = :yaml
      DEFAUT_WEIGHT = 60
      DEFAULT_SD_FILE_PATH = ENV['DEFAULT_SD_FILE_PATH'] || '/etc/fluent/sd.yaml'

      helpers :event_loop

      config_param :path, :string, default: DEFAULT_SD_FILE_PATH
      config_param :conf_encoding, :string, default: 'utf-8'

      def initialize
        super

        @file_type = nil
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
      end

      def start(queue)
        watcher = StatWatcher.new(@path, @log) do |_prev, _cur|
          refresh_file(queue)
        end
        event_loop_attach(watcher)

        super()
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
          end
      end

      def refresh_file(queue)
        s =
          begin
            fetch_server_info
          rescue => e
            @log.error("sd_file: #{e}")
          end

        if s.nil?
          # if any error occurs, skip this turn
          return
        end

        diff = []
        join = s - @services
        # Need service_in first to guarantee that server exist at least one all time.
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
        config_data =
          begin
            File.open(@path, "r:#{@conf_encoding}:utf-8", &:read)
          rescue => e
            raise Fluent::ConfigError, "sd_file: path=#{@path} couldn't open #{e}"
          end

        parser.call(config_data).map do |s|
          Service.new(
            :file,
            s.fetch('host'),
            s.fetch('port'),
            s['name'],
            s.fetch('weight', DEFAUT_WEIGHT),
            s['standby'],
            s['username'],
            s['password'],
            s['shared_key'],
          )
        end
      rescue KeyError => e
        raise Fluent::ConfigError, e
      end

      class StatWatcher < Coolio::StatWatcher
        def initialize(path, log, &callback)
          @path = path
          @log = log
          @callback = callback
          super(@path)
        end

        def on_change(prev_stat, cur_stat)
          @callback.call(prev_stat, cur_stat)
        rescue => e
          @log.error(e)
        end
      end
    end
  end
end
