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
require 'fluent/oj_options'

require 'yajl'
require 'json'

module Fluent
  module Plugin
    class JSONParser < Parser
      Plugin.register_parser('json', self)

      config_set_default :time_key, 'time'
      desc 'Set JSON parser'
      config_param :json_parser, :enum, list: [:oj, :yajl, :json], default: :oj

      # The Yajl library defines a default buffer size of 8KiB when parsing
      # from IO streams, so maintain this for backwards-compatibility.
      # https://www.rubydoc.info/github/brianmario/yajl-ruby/Yajl%2FParser:parse
      desc 'Set the buffer size that Yajl will use when parsing streaming input'
      config_param :stream_buffer_size, :integer, default: 8192

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
          return [Oj.method(:load), Oj::ParseError] if Fluent::OjOptions.available?

          log&.info "Oj is not installed, and failing back to JSON for json parser"
          configure_json_parser(:json)
        when :json then [JSON.method(:parse), JSON::ParserError]
        when :yajl then [Yajl.method(:load), Yajl::ParseError]
        else
          raise "BUG: unknown json parser specified: #{name}"
        end
      end

      def parse(text)
        parsed_json = @load_proc.call(text)

        if parsed_json.is_a?(Hash)
          time, record = parse_one_record(parsed_json)
          yield time, record
          parsed_json.clear
        elsif parsed_json.is_a?(Array)
          parsed_json.each do |record|
            unless record.is_a?(Hash)
              yield nil, nil
              next
            end
            time, parsed_record = parse_one_record(record)
            yield time, parsed_record
            record.clear
          end
          parsed_json.clear
        else
          yield nil, nil
        end

      rescue @error_class, EncodingError # EncodingError is for oj 3.x or later
        yield nil, nil
      end

      def parse_one_record(record)
        time = parse_time(record)
        convert_values(time, record)
      end

      def parser_type
        :text
      end

      def parse_io(io, &block)
        y = Yajl::Parser.new
        y.on_parse_complete = ->(record){
          block.call(parse_time(record), record)
        }
        y.parse(io, @stream_buffer_size)
      end
    end
  end
end
