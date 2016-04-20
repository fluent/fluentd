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
        if ! @estimate_current_event.nil? && @parser.respond_to?(:'estimate_current_event=')
          @parser.estimate_current_event = @estimate_current_event
        end

        if @parser.respond_to?(:configure)
          @parser.configure(conf)
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

      class TimeParser < Fluent::Plugin::Parser::TimeParser
        # TODO: warn when deprecated
      end

      class RegexpParser < Fluent::Plugin::RegexpParser
        # TODO: warn when deprecated
      end

      class ValuesParser < Fluent::Plugin::ValuesParser
        # TODO: warn when deprecated
      end

      class JSONParser < Fluent::Plugin::JSONParser
        # TODO: warn when deprecated
      end

      class TSVParser < Fluent::Plugin::TSVParser
        # TODO: warn when deprecated
      end

      class LabeledTSVParser < Fluent::Plugin::LabeledTSVParser
        # TODO: warn when deprecated
      end

      class CSVParser < Fluent::Plugin::CSVParser
        # TODO: warn when deprecated
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
