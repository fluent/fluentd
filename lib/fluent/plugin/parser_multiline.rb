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
require 'fluent/plugin/parser_regexp'

module Fluent
  module Plugin
    class MultilineParser < Parser
      Plugin.register_parser('multiline', self)

      desc 'Specify regexp pattern for start line of multiple lines'
      config_param :format_firstline, :string, default: nil

      FORMAT_MAX_NUM = 20

      def configure(conf)
        super

        formats = parse_formats(conf).compact.map { |f| f[1..-2] }.join
        begin
          regexp = Regexp.new(formats, Regexp::MULTILINE)
          if regexp.named_captures.empty?
            raise "No named captures"
          end
          regexp_conf = Fluent::Config::Element.new("", "", { "expression" => "/#{formats}/m" }, [])
          @parser = Fluent::Plugin::RegexpParser.new
          @parser.configure(conf + regexp_conf)
        rescue => e
          raise Fluent::ConfigError, "Invalid regexp '#{formats}': #{e}"
        end

        if @format_firstline
          check_format_regexp(@format_firstline, 'format_firstline')
          @firstline_regex = Regexp.new(@format_firstline[1..-2])
        end
      end

      def parse(text, &block)
        @parser.call(text, &block)
      end

      def has_firstline?
        !!@format_firstline
      end

      def firstline?(text)
        @firstline_regex.match(text)
      end

      private

      def parse_formats(conf)
        check_format_range(conf)

        prev_format = nil
        (1..FORMAT_MAX_NUM).map { |i|
          format = conf["format#{i}"]
          if (i > 1) && prev_format.nil? && !format.nil?
            raise Fluent::ConfigError, "Jump of format index found. format#{i - 1} is missing."
          end
          prev_format = format
          next if format.nil?

          check_format_regexp(format, "format#{i}")
          format
        }
      end

      def check_format_range(conf)
        invalid_formats = conf.keys.select { |k|
          m = k.match(/^format(\d+)$/)
          m ? !((1..FORMAT_MAX_NUM).include?(m[1].to_i)) : false
        }
        unless invalid_formats.empty?
          raise Fluent::ConfigError, "Invalid formatN found. N should be 1 - #{FORMAT_MAX_NUM}: " + invalid_formats.join(",")
        end
      end

      def check_format_regexp(format, key)
        if format[0] == '/' && format[-1] == '/'
          begin
            Regexp.new(format[1..-2], Regexp::MULTILINE)
          rescue => e
            raise Fluent::ConfigError, "Invalid regexp in #{key}: #{e}"
          end
        else
          raise Fluent::ConfigError, "format should be Regexp, need //, in #{key}: '#{format}'"
        end
      end
    end
  end
end
