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

require 'fluent/plugin'
require 'fluent/configurable'
require 'fluent/timezone'

module Fluent
  module PluginHelper
    module TimeFormatter
      def format_time(time)
        @_time_formatter.call(time)
      end

      module TimeFormatterParams
        include Fluent::Configurable
        config_section :time_format, required: false, multi: false, param_name: :time_format_config do
          config_param :type, :enum, list: [:float, :unixtime, :string], default: :float
          config_param :format, :string, default: nil
          config_param :localtime, :bool, default: true # if localtime is false and timezone is nil, then utc
          config_param :utc, :bool, default: false # placeholder to turn localtime to false
          config_param :timezone, :string, default: nil
        end
      end

      def self.included(mod)
        mod.include TimeFormatterParams
      end

      def initialize
        super
        @_time_formatter = nil
      end

      def configure(conf)
        conf.elements('time_format').each do |e|
          if e.has_key?('utc') && Fluent::Config.bool_value(e['utc'])
            e['localtime'] = 'false'
          end
        end

        super

        if @time_format_config
          @_time_formatter = case @time_format_config.type
                             when :float then ->(time) { time.to_r.truncate(+6).to_f } # microsecond floating point value
                             when :unixtime then ->(time) { time.to_i }
                             else
                               Fluent::TimeFormatter.new(@time_format_config.format,
                                                         @time_format_config.localtime,
                                                         @time_format_config.timezone)
                             end
        end
      end
    end
  end
end
