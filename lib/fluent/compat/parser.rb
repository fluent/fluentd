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

require 'fluent/plugin'
require 'fluent/plugin/parser'
require 'fluent/mixin'

require 'fluent/config'
require 'fluent/compat/type_converter'

require 'fluent/plugin/parser_regexp'
require 'fluent/plugin/parser_json'
require 'fluent/plugin/parser_tsv'
require 'fluent/plugin/parser_ltsv'
require 'fluent/plugin/parser_csv'
require 'fluent/plugin/parser_none'
require 'fluent/plugin/parser_apache2'
require 'fluent/plugin/parser_syslog'
require 'fluent/plugin/parser_multiline'

module Fluent
  module Compat
    class Parser < Fluent::Plugin::Parser
      # TODO: warn when deprecated
    end

    class TextParser
      # Keep backward compatibility for existing plugins
      ParserError = Fluent::Plugin::Parser::ParserError
      # TODO: will be removed at v1
      TypeConverter = Fluent::TypeConverter

      def initialize
        # TODO: warn when deprecated
        @parser = nil
        @estimate_current_event = nil
      end

      attr_reader :parser

      # SET false BEFORE CONFIGURE, to return nil when time not parsed
      # 'configure()' may raise errors for unexpected configurations
      attr_accessor :estimate_current_event

      def configure(conf, required=true)
        format = conf['format']

        @parser = TextParser.lookup(format)

        if @parser.respond_to?(:configure)
          @parser.configure(conf)
        end
        if !@estimate_current_event.nil? && @parser.respond_to?(:'estimate_current_event=')
          # external code sets parser.estimate_current_event = false
          @parser.estimate_current_event = @estimate_current_event
        end

        return true
      end

      def parse(text, &block)
        if block
          @parser.parse(text, &block)
        else
          @parser.parse(text) { |time, record|
            return time, record
          }
        end
      end

      def self.register_template(type, template, time_format=nil)
        # TODO: warn when deprecated to use Plugin.register_parser directly
        if template.is_a?(Class) || template.respond_to?(:call)
          Fluent::Plugin.register_parser(type, template)
        elsif template.is_a?(Regexp)
          Fluent::Plugin.register_parser(type, Proc.new { RegexpParser.new(template, {'time_format' => time_format}) })
        else
          raise ArgumentError, "Template for parser must be a Class, callable object or regular expression object"
        end
      end

      def self.lookup(format)
        # TODO: warn when deprecated to use Plugin.new_parser or RegexpParser.new directly
        if format.nil?
          raise ConfigError, "'format' parameter is required"
        end

        if format[0] == ?/ && format[format.length-1] == ?/
          # regexp
          begin
            regexp = Regexp.new(format[1..-2])
            if regexp.named_captures.empty?
              raise "No named captures"
            end
          rescue
            raise ConfigError, "Invalid regexp '#{format[1..-2]}': #{$!}"
          end

          RegexpParser.new(regexp)
        else
          # built-in template
          begin
            Fluent::Plugin.new_parser(format)
          rescue ConfigError # keep same error message
            raise ConfigError, "Unknown format template '#{format}'"
          end
        end
      end

      module TypeConverterCompatParameters
        def convert_type_converter_parameters!(conf)
          if conf["types"]
            delimiter = conf["types_delimiter"] || ','
            label_delimiter = conf["types_label_delimiter"] || ':'
            types = {}
            conf['types'].split(delimiter).each do |pair|
              key, value = pair.split(label_delimiter, 2)
              if value.start_with?("time#{label_delimiter}")
                value = value.split(label_delimiter, 2).join(':')
              elsif value.start_with?("array#{label_delimiter}")
                value = value.split(label_delimiter, 2).join(':')
              end
              types[key] = value
            end
            conf["types"] = JSON.dump(types)
          end
        end
      end

      class TimeParser < Fluent::TimeParser
        # TODO: warn when deprecated
      end

      class RegexpParser < Fluent::Plugin::RegexpParser
        include TypeConverterCompatParameters

        # TODO: warn when deprecated
        def initialize(regexp, conf = {})
          super()

          @stored_regexp = regexp
          @manually_configured = false
          unless conf.empty?
            conf_init = if conf.is_a?(Fluent::Config::Element)
                          conf
                        else
                          Fluent::Config::Element.new('parse', '', conf, [])
                        end
            self.configure(conf_init)
            @manually_configured = true
          end
        end

        def configure(conf)
          return if @manually_configured # not to run twice

          conf['expression'] ||= @stored_regexp.source
          conf['ignorecase'] ||= @stored_regexp.options & Regexp::IGNORECASE != 0
          conf['multiline'] ||= @stored_regexp.options & Regexp::MULTILINE != 0
          convert_type_converter_parameters!(conf)

          super
        end

        def patterns
          {'format' => @regexp, 'time_format' => @time_format}
        end
      end

      class ValuesParser < Parser
        include Fluent::Compat::TypeConverter

        config_param :keys, :array, default: []
        config_param :time_key, :string, default: nil
        config_param :null_value_pattern, :string, default: nil
        config_param :null_empty_string, :bool, default: false

        def configure(conf)
          super

          if @time_key && !@keys.include?(@time_key) && @estimate_current_event
            raise Fluent::ConfigError, "time_key (#{@time_key.inspect}) is not included in keys (#{@keys.inspect})"
          end

          if @time_format && !@time_key
            raise Fluent::ConfigError, "time_format parameter is ignored because time_key parameter is not set. at #{conf.inspect}"
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

      class JSONParser < Fluent::Plugin::JSONParser
        include TypeConverterCompatParameters
        # TODO: warn when deprecated
        def configure(conf)
          convert_type_converter_parameters!(conf)
          super
        end
      end

      class TSVParser < Fluent::Plugin::TSVParser
        include TypeConverterCompatParameters
        # TODO: warn when deprecated
        def configure(conf)
          convert_type_converter_parameters!(conf)
          super
        end
      end

      class LabeledTSVParser < Fluent::Plugin::LabeledTSVParser
        include TypeConverterCompatParameters
        # TODO: warn when deprecated
        def configure(conf)
          convert_type_converter_parameters!(conf)
          super
        end
      end

      class CSVParser < Fluent::Plugin::CSVParser
        include TypeConverterCompatParameters
        # TODO: warn when deprecated
        def configure(conf)
          convert_type_converter_parameters!(conf)
          super
        end
      end

      class NoneParser < Fluent::Plugin::NoneParser
        # TODO: warn when deprecated
      end

      class ApacheParser < Fluent::Plugin::Apache2Parser
        # TODO: warn when deprecated
      end

      class SyslogParser < Fluent::Plugin::SyslogParser
        # TODO: warn when deprecated
      end

      class MultilineParser < Fluent::Plugin::MultilineParser
        # TODO: warn when deprecated
      end
    end
  end
end
