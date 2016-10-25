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
require 'fluent/env'
require 'fluent/time'

require 'yajl'
require 'json'

module Fluent
  module Plugin
    class JSONParser < Parser
      Plugin.register_parser('json', self)

      config_set_default :time_key, 'time'
      config_param :json_parser, :enum, list: [:oj, :yajl, :json], default: :oj

      config_set_default :time_type, :float

      def configure(conf)
        if conf.has_key?('time_format')
          conf['time_type'] ||= 'string'
        end

        super
        @load_proc, @error_class = configure_json_parser(@json_parser)
      end

      def configure_json_parser(name)
        case name
        when :oj
          require 'oj'
          Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
          [Oj.method(:load), Oj::ParseError]
        when :json then [JSON.method(:load), JSON::ParserError]
        when :yajl then [Yajl.method(:load), Yajl::ParseError]
        else
          raise "BUG: unknown json parser specified: #{name}"
        end
      rescue LoadError
        name = :yajl
        log.info "Oj is not installed, and failing back to Yajl for json parser"
        retry
      end

      def parse(text)
        r = @load_proc.call(text)
        time, record = convert_values(parse_time(r), r)
        yield time, record
      rescue @error_class
        yield nil, nil
      end
    end
  end
end
