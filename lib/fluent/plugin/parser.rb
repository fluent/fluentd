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

require 'strptime'

module Fluent
  module Plugin
    class Parser < Base
      include OwnedByMixin

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

      class TimeParser
        def initialize(time_format)
          @cache1_key = nil
          @cache1_time = nil
          @cache2_key = nil
          @cache2_time = nil
          @parser =
            if time_format
              begin
                strptime = Strptime.new(time_format)
                Proc.new { |value| Fluent::EventTime.from_time(strptime.exec(value)) }
              rescue
                Proc.new { |value| Fluent::EventTime.from_time(Time.strptime(value, time_format)) }
              end
            else
              Proc.new { |value| Fluent::EventTime.parse(value) }
            end
        end

        # TODO: new cache mechanism using format string
        def parse(value)
          unless value.is_a?(String)
            raise ParserError, "value must be string: #{value}"
          end

          if @cache1_key == value
            return @cache1_time
          elsif @cache2_key == value
            return @cache2_time
          else
            begin
              time = @parser.call(value)
            rescue => e
              raise ParserError, "invalid time format: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
            end
            @cache1_key = @cache2_key
            @cache1_time = @cache2_time
            @cache2_key = value
            @cache2_time = time
            return time
          end
        end
      end
    end

    class RegexpParser < Parser
      include Fluent::TypeConverter

      config_param :time_key, :string, default: 'time'
      config_param :time_format, :string, default: nil

      def initialize(regexp, conf={})
        super()

        unless conf.empty?
          unless conf.is_a?(Config::Element)
            conf = Config::Element.new('default_regexp_conf', '', conf, [])
          end
          configure(conf)
        end

        @regexp = regexp
        @time_parser = TimeParser.new(@time_format)
        @mutex = Mutex.new
      end

      def configure(conf)
        super
        @time_parser = TimeParser.new(@time_format)
      end

      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        m = @regexp.match(text)
        unless m
          yield nil, nil
          return
        end

        time = nil
        record = {}

        m.names.each do |name|
          if value = m[name]
            if name == @time_key
              time = @mutex.synchronize { @time_parser.parse(value) }
              if @keep_time_key
                record[name] = if @type_converters.nil?
                                 value
                               else
                                 convert_type(name, value)
                               end
              end
            else
              record[name] = if @type_converters.nil?
                               value
                             else
                               convert_type(name, value)
                             end
            end
          end
        end

        if @estimate_current_event
          time ||= Fluent::EventTime.now
        end

        yield time, record
      end
    end

    class ValuesParser < Parser
      include Fluent::TypeConverter

      config_param :keys, :array, default: []
      config_param :time_key, :string, default: nil
      config_param :time_format, :string, default: nil
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

        @time_parser = TimeParser.new(@time_format)

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
