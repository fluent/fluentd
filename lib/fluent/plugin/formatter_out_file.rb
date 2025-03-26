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
require 'json'

module Fluent
  module Plugin
    class OutFileFormatter < Formatter
      include Fluent::Plugin::Newline::Mixin

      Plugin.register_formatter('out_file', self)

      config_param :output_time, :bool, default: true
      config_param :output_tag, :bool, default: true
      config_param :delimiter, default: "\t" do |val|
        case val
        when /SPACE/i then ' '.freeze
        when /COMMA/i then ','.freeze
        else "\t".freeze
        end
      end
      config_set_default :time_type, :string
      config_set_default :time_format, nil # time_format nil => iso8601

      def configure(conf)
        super
        @timef = time_formatter_create
      end

      def format(tag, time, record)
        json_str = JSON.generate(record)
        header = ''
        header << "#{@timef.format(time)}#{@delimiter}" if @output_time
        header << "#{tag}#{@delimiter}" if @output_tag
        "#{header}#{json_str}#{@newline}"
      ensure
        json_str&.clear
      end
    end
  end
end
