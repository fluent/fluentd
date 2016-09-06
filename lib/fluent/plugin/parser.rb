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

      # SET false BEFORE CONFIGURE, to return nil when time not parsed
      attr_accessor :estimate_current_event

      config_param :keep_time_key, :bool, default: false

      def initialize
        super
        @estimate_current_event = true
      end

      def parse(text)
        raise NotImplementedError, "Implement this method in child class"
      end

      def call(*a, &b)
        # Keep backward compatibility for existing plugins
        # TODO: warn when deprecated
        parse(*a, &b)
      end

      TimeParser = Fluent::TimeParser
    end

    class ValuesParser < Parser
      include Fluent::TypeConverter

      config_param :keys, :array, default: []
      config_param :time_key, :string, default: nil
      config_param :null_value_pattern, :string, default: nil
      config_param :null_empty_string, :bool, default: false

      def configure(conf)
        super

        if @time_key && !@keys.include?(@time_key) && @estimate_current_event
          raise ConfigError, "time_key (#{@time_key.inspect}) is not included in keys (#{@keys.inspect})"
        end

        if @time_format && !@time_key
          raise ConfigError, "time_format parameter is ignored because time_key parameter is not set. at #{conf.inspect}"
        end

        @time_parser = time_parser_create

        if @null_value_pattern
          @null_value_pattern = Regexp.new(@null_value_pattern)
        end

        @mutex = Mutex.new
      end

      def values_map(values)
        record = Hash[keys.zip(values.map { |value| convert_value_to_nil(value) })]

        if @time_key
          value = @keep_time_key ? record[@time_key] : record.delete(@time_key)
          time = if value.nil?
                   if @estimate_current_event
                     Fluent::EventTime.now
                   else
                     nil
                   end
                 else
                   @mutex.synchronize { @time_parser.parse(value) }
                 end
        elsif @estimate_current_event
          time = Fluent::EventTime.now
        else
          time = nil
        end

        convert_field_type!(record) if @type_converters

        return time, record
      end

      private

      def convert_field_type!(record)
        @type_converters.each_key { |key|
          if value = record[key]
            record[key] = convert_type(key, value)
          end
        }
      end

      def convert_value_to_nil(value)
        if value and @null_empty_string
          value = (value == '') ? nil : value
        end
        if value and @null_value_pattern
          value = ::Fluent::StringUtil.match_regexp(@null_value_pattern, value) ? nil : value
        end
        value
      end
    end
  end
end
