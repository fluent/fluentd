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

require 'fluent/plugin/base'
require 'fluent/plugin/owned_by_mixin'

require 'fluent/mixin' # for TypeConverter
require 'fluent/time'
require 'fluent/plugin/string_util'

module Fluent
  module Plugin
    class Parser < Base
      include OwnedByMixin
      include TimeMixin::Parser

      class ParserError < StandardError; end

      configured_in :parse

      ### types can be specified as string-based hash style
      # field1:type, field2:type, field3:type:option, field4:type:option
      ### or, JSON format
      # {"field1":"type", "field2":"type", "field3":"type:option", "field4":"type:option"}
      config_param :types, :hash, value_type: :string, default: nil

      # available options are:
      # array: (1st) delimiter
      # time : type[, format, timezone] -> type should be a valid "time_type"(string/unixtime/float)
      #      : format[, timezone]

      config_param :time_key, :string, default: nil
      config_param :null_value_pattern, :string, default: nil
      config_param :null_empty_string, :bool, default: false
      config_param :estimate_current_event, :bool, default: true
      config_param :keep_time_key, :bool, default: false

      AVAILABLE_PARSER_VALUE_TYPES = ['string', 'integer', 'float', 'bool', 'time', 'array']

      # for tests
      attr_reader :type_converters

      PARSER_TYPES = [:text_per_line, :text, :binary]
      def parser_type
        :text_per_line
      end

      def configure(conf)
        super

        @time_parser = time_parser_create
        @null_value_regexp = @null_value_pattern && Regexp.new(@null_value_pattern)
        @type_converters = build_type_converters(@types)
        @execute_convert_values = @type_converters || @null_value_regexp || @null_empty_string
      end

      def parse(text, &block)
        raise NotImplementedError, "Implement this method in child class"
      end

      def call(*a, &b)
        # Keep backward compatibility for existing plugins
        # TODO: warn when deprecated
        parse(*a, &b)
      end

      def implement?(feature)
        methods_of_plugin = self.class.instance_methods(false)
        case feature
        when :parse_io then methods_of_plugin.include?(:parse_io)
        when :parse_partial_data then methods_of_plugin.include?(:parse_partial_data)
        else
          raise ArgumentError, "Unknown feature for parser plugin: #{feature}"
        end
      end

      def parse_io(io, &block)
        raise NotImplementedError, "Optional API #parse_io is not implemented"
      end

      def parse_partial_data(data, &block)
        raise NotImplementedError, "Optional API #parse_partial_data is not implemented"
      end

      def parse_time(record)
        if @time_key && record.respond_to?(:has_key?) && record.has_key?(@time_key)
          src = if @keep_time_key
                  record[@time_key]
                else
                  record.delete(@time_key)
                end
          @time_parser.parse(src)
        elsif @estimate_current_event
          Fluent::EventTime.now
        else
          nil
        end
      rescue Fluent::TimeParser::TimeParseError => e
        raise ParserError, e.message
      end

      # def parse(text, &block)
      #   time, record = convert_values(time, record)
      #   yield time, record
      # end
      def convert_values(time, record)
        return time, record unless @execute_convert_values

        record.each_key do |key|
          value = record[key]
          next unless value # nil/null value is always left as-is.

          if value.is_a?(String) && string_like_null(value)
            record[key] = nil
            next
          end

          if @type_converters && @type_converters.has_key?(key)
            record[key] = @type_converters[key].call(value)
          end
        end

        return time, record
      end

      def string_like_null(value, null_empty_string = @null_empty_string, null_value_regexp = @null_value_regexp)
        null_empty_string && value.empty? || null_value_regexp && string_safe_encoding(value){|s| null_value_regexp.match(s) }
      end

      TRUTHY_VALUES = ['true', 'yes', '1']

      def build_type_converters(types)
        return nil unless types

        converters = {}

        types.each_pair do |field_name, type_definition|
          type, option = type_definition.split(":", 2)
          unless AVAILABLE_PARSER_VALUE_TYPES.include?(type)
            raise Fluent::ConfigError, "unknown value conversion for key:'#{field_name}', type:'#{type}'"
          end

          conv = case type
                 when 'string' then ->(v){ v.to_s }
                 when 'integer' then ->(v){ v.to_i rescue v.to_s.to_i }
                 when 'float' then ->(v){ v.to_f rescue v.to_s.to_f }
                 when 'bool' then ->(v){ TRUTHY_VALUES.include?(v.to_s.downcase) }
                 when 'time'
                   # comma-separated: time:[timezone:]time_format
                   # time_format is unixtime/float/string-time-format
                   timep = if option
                             time_type = 'string' # estimate
                             timezone, time_format = option.split(':', 2)
                             unless Fluent::Timezone.validate(timezone)
                               timezone, time_format = nil, option
                             end
                             if Fluent::TimeMixin::TIME_TYPES.include?(time_format)
                               time_type, time_format = time_format, nil # unixtime/float
                             end
                             time_parser_create(type: time_type.to_sym, format: time_format, timezone: timezone)
                           else
                             time_parser_create(type: :string, format: nil, timezone: nil)
                           end
                   ->(v){ timep.parse(v) rescue nil }
                 when 'array'
                   delimiter = option ? option.to_s : ','
                   ->(v){ string_safe_encoding(v.to_s){|s| s.split(delimiter) } }
                 else
                   raise "BUG: unknown type even after check: #{type}"
                 end
          converters[field_name] = conv
        end

        converters
      end
    end
  end
end
