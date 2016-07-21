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

require 'fluent/plugin/formatter'
require 'fluent/time'
require 'yajl'

module Fluent
  module Plugin
    class OutFileFormatter < Formatter
      Plugin.register_formatter('out_file', self)

      config_param :output_time, :bool, default: true
      config_param :output_tag, :bool, default: true
      config_param :delimiter, default: "\t" do |val|
        case val
        when /SPACE/i then ' '
        when /COMMA/i then ','
        else "\t"
        end
      end
      config_param :time_type, :enum, list: [:float, :unixtime, :string], default: :string
      config_param :time_format, :string, default: nil
      config_param :localtime, :bool, default: true # if localtime is false and timezone is nil, then utc
      config_param :timezone, :string, default: nil

      def configure(conf)
        if conf.has_key?('time_as_epoch') && Fluent::Config.bool_value(conf['time_as_epoch'])
          conf['localtime'] = 'false'
          conf['timezone'] = "+0000"
        end

        super
        @timef = case @time_type
                 when :float then ->(time){ time.to_r.to_f }
                 when :unixtime then ->(time){ time.to_i }
                 else
                   Fluent::TimeFormatter.new(@time_format, @localtime, @timezone)
                 end
      end

      def format(tag, time, record)
        header = ''
        header << "#{@timef.format(time)}#{@delimiter}" if @output_time
        header << "#{tag}#{@delimiter}" if @output_tag
        "#{header}#{Yajl.dump(record)}\n"
      end
    end
  end
end
