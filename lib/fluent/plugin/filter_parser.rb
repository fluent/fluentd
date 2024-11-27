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

require 'fluent/time'
require 'fluent/config/error'
require 'fluent/plugin/filter'
require 'fluent/plugin_helper/parser'
require 'fluent/plugin_helper/compat_parameters'

module Fluent::Plugin
  class ParserFilter < Filter
    Fluent::Plugin.register_filter('parser', self)

    helpers :parser, :record_accessor, :compat_parameters

    desc 'Specify field name in the record to parse.'
    config_param :key_name, :string
    desc 'Keep original key-value pair in parsed result.'
    config_param :reserve_data, :bool, default: false
    desc 'Keep original event time in parsed result.'
    config_param :reserve_time, :bool, default: false
    desc 'Remove "key_name" field from the record when parsing is succeeded'
    config_param :remove_key_name_field, :bool, default: false
    desc 'Store parsed values with specified key name prefix.'
    config_param :inject_key_prefix, :string, default: nil
    desc 'If true, invalid string is replaced with safe characters and re-parse it.'
    config_param :replace_invalid_sequence, :bool, default: false
    desc 'Store parsed values as a hash value in a field.'
    config_param :hash_value_field, :string, default: nil
    desc 'Emit invalid record to @ERROR label'
    config_param :emit_invalid_record_to_error, :bool, default: true

    attr_reader :parser

    def configure(conf)
      compat_parameters_convert(conf, :parser)

      super

      @accessor = record_accessor_create(@key_name)
      @parser = parser_create
    end

    REPLACE_CHAR = '?'.freeze

    def filter_stream(tag, es)
      new_es = Fluent::MultiEventStream.new
      es.each do |time, record|
        begin
          raw_value = @accessor.call(record)
          if raw_value.nil?
            new_es.add(time, handle_parsed(tag, record, time, {})) if @reserve_data
            raise ArgumentError, "#{@key_name} does not exist"
          else
            filter_one_record(tag, time, record, raw_value) do |result_time, result_record|
              new_es.add(result_time, result_record)
            end
          end
        rescue => e
          router.emit_error_event(tag, time, record, e) if @emit_invalid_record_to_error
        end
      end
      new_es
    end

    private

    def filter_one_record(tag, time, record, raw_value)
      begin
        @parser.parse(raw_value) do |t, values|
          if values
            t = if @reserve_time
                  time
                else
                  t.nil? ? time : t
                end
            @accessor.delete(record) if @remove_key_name_field
          else
            router.emit_error_event(tag, time, record, Fluent::Plugin::Parser::ParserError.new("pattern not matched with data '#{raw_value}'")) if @emit_invalid_record_to_error
            next unless @reserve_data
            t = time
            values = {}
          end
          yield(t, handle_parsed(tag, record, t, values))
        end

      rescue Fluent::Plugin::Parser::ParserError => e
        raise e
      rescue ArgumentError => e
        raise unless @replace_invalid_sequence
        raise unless e.message.index("invalid byte sequence in") == 0

        raw_value = raw_value.scrub(REPLACE_CHAR)
        retry
      rescue => e
        raise Fluent::Plugin::Parser::ParserError, "parse failed #{e.message}"
      end
    end

    def handle_parsed(tag, record, t, values)
      if values && @inject_key_prefix
        values = Hash[values.map { |k, v| [@inject_key_prefix + k, v] }]
      end
      r = @hash_value_field ? {@hash_value_field => values} : values
      if @reserve_data
        r = r ? record.merge(r) : record
      end
      r
    end
  end
end
