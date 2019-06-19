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
      desc 'Set JSON parser'
      config_param :json_parser, :enum, list: [:oj, :yajl, :json], default: :oj

      # The Yajl library defines a default buffer size of 8KiB when parsing
      # from IO streams, so maintain this for backwards-compatibility.
      # https://www.rubydoc.info/github/brianmario/yajl-ruby/Yajl%2FParser:parse
      desc 'Set the buffer size that Yajl will use when parsing streaming input'
      config_param :stream_buffer_size, :integer, default: 8192

      desc 'Whether to flatten nested JSON.'
      config_param :flatten_json, :bool, default: false

      desc 'The delimiter character (or string) of key when flatten nested JSON.'
      config_param :flatten_key_delimiter, :string, default: '.'

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
      rescue LoadError => ex
        name = :yajl
        if log
          if /\boj\z/ =~ ex.message
            log.info "Oj is not installed, and failing back to Yajl for json parser"
          else
            log.warn ex.message
          end
        end
        retry
      end

      def parse(text)
        r = @load_proc.call(text)
        time, record = convert_values(parse_time(r), @flatten_json ? flatten_hash_from(r) : r)
        yield time, record
      rescue @error_class, EncodingError # EncodingError is for oj 3.x or later
        yield nil, nil
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

      private
      def flatten_hash_from(hash_or_array)
        if hash_or_array.is_a?(Array)
          tmp_hash = {}
          hash_or_array.each_with_index { |value, index| tmp_hash["#{index}"] = value }
        end
        hash = hash_or_array.is_a?(Array) ? tmp_hash : hash_or_array
        hash.each_with_object({}) do |(key, value), flat_hash|
          next flatten_hash_from(value).each do |k, v|
            flat_hash["#{key}#{@flatten_key_delimiter}#{k}"] = v
          end if value.is_a?(Hash) || value.is_a?(Array)
          flat_hash[key] = value
        end
      end
    end
  end
end
