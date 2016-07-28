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
require 'fluent/plugin/formatter'
require 'fluent/compat/handle_tag_and_time_mixin'
require 'fluent/compat/structured_format_mixin'

require 'fluent/plugin/formatter_out_file'
require 'fluent/plugin/formatter_stdout'
require 'fluent/plugin/formatter_json'
require 'fluent/plugin/formatter_hash'
require 'fluent/plugin/formatter_msgpack'
require 'fluent/plugin/formatter_ltsv'
require 'fluent/plugin/formatter_csv'
require 'fluent/plugin/formatter_single_value'

module Fluent
  module Compat
    class Formatter < Fluent::Plugin::Formatter
      # TODO: warn when deprecated
    end

    module TextFormatter
      def self.register_template(type, template)
        # TODO: warn when deprecated to use Plugin.register_formatter directly
        if template.is_a?(Class)
          Fluent::Plugin.register_formatter(type, template)
        elsif template.respond_to?(:call) && template.arity == 3 # Proc.new { |tag, time, record| }
          Fluent::Plugin.register_formatter(type, Proc.new { ProcWrappedFormatter.new(template) })
        elsif template.respond_to?(:call)
          Fluent::Plugin.register_formatter(type, template)
        else
          raise ArgumentError, "Template for formatter must be a Class or callable object"
        end
      end

      def self.lookup(type)
        # TODO: warn when deprecated to use Plugin.new_formatter(type, parent: plugin)
        Fluent::Plugin.new_formatter(type)
      end

      # Keep backward-compatibility
      def self.create(conf)
        # TODO: warn when deprecated
        format = conf['format']
        if format.nil?
          raise ConfigError, "'format' parameter is required"
        end

        formatter = lookup(format)
        if formatter.respond_to?(:configure)
          formatter.configure(conf)
        end
        formatter
      end

      HandleTagAndTimeMixin = Fluent::Compat::HandleTagAndTimeMixin
      StructuredFormatMixin = Fluent::Compat::StructuredFormatMixin

      class ProcWrappedFormatter < Fluent::Plugin::ProcWrappedFormatter
        # TODO: warn when deprecated
      end

      class OutFileFormatter < Fluent::Plugin::OutFileFormatter
        # TODO: warn when deprecated
      end

      class StdoutFormatter < Fluent::Plugin::StdoutFormatter
        # TODO: warn when deprecated
      end

      class JSONFormatter < Fluent::Plugin::JSONFormatter
        # TODO: warn when deprecated
      end

      class HashFormatter < Fluent::Plugin::HashFormatter
        # TODO: warn when deprecated
      end

      class MessagePackFormatter < Fluent::Plugin::MessagePackFormatter
        # TODO: warn when deprecated
      end

      class LabeledTSVFormatter < Fluent::Plugin::LabeledTSVFormatter
        # TODO: warn when deprecated
      end

      class CsvFormatter < Fluent::Plugin::CsvFormatter
        # TODO: warn when deprecated
      end

      class SingleValueFormatter < Fluent::Plugin::SingleValueFormatter
        # TODO: warn when deprecated
      end
    end
  end
end
