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

require 'fluent/plugin/parser'
require 'fluent/time'

require 'yajl'

module Fluent
  module Plugin
    class JSONParser < Parser
      Plugin.register_parser('json', self)

      config_param :time_key, :string, default: 'time'
      config_param :time_format, :string, default: nil
      config_param :json_parser, :string, default: 'oj'

      def configure(conf)
        super

        unless @time_format.nil?
          @time_parser = TimeParser.new(@time_format)
          @mutex = Mutex.new
        end

        begin
          raise LoadError unless @json_parser == 'oj'
          require 'oj'
          Oj.default_options = {bigdecimal_load: :float}
          @load_proc = Oj.method(:load)
          @error_class = Oj::ParseError
        rescue LoadError
          @load_proc = Yajl.method(:load)
          @error_class = Yajl::ParseError
        end
      end

      def parse(text)
        record = @load_proc.call(text)

        value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
        if value
          if @time_format
            time = @mutex.synchronize { @time_parser.parse(value) }
          else
            begin
              time = Fluent::EventTime.from_time(Time.at(value.to_f))
            rescue => e
              raise ParserError, "invalid time value: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
            end
          end
        else
          if @estimate_current_event
            time = Fluent::EventTime.now
          else
            time = nil
          end
        end

        yield time, record
      rescue @error_class
        yield nil, nil
      end
    end
  end
end
