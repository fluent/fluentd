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

require 'fluent/plugin_helper'
require 'fluent/plugin/formatter'
require 'csv'

module Fluent
  module Plugin
    class CsvFormatter < Formatter
      Plugin.register_formatter('csv', self)

      include PluginHelper::Mixin
      helpers :record_accessor

      config_param :delimiter, default: ',' do |val|
        ['\t', 'TAB'].include?(val) ? "\t".freeze : val.freeze
      end
      config_param :force_quotes, :bool, default: true
      # "array" looks good for type of :fields, but this implementation removes tailing comma
      # TODO: Is it needed to support tailing comma?
      config_param :fields, :array, value_type: :string
      config_param :add_newline, :bool, default: true

      def csv_cacheable?
        owner ? true : false
      end

      def csv_thread_key
        csv_cacheable? ? "#{owner.plugin_id}_csv_formatter_#{@usage}_csv" : nil
      end

      def csv_for_thread
        if csv_cacheable?
          Thread.current[csv_thread_key] ||= CSV.new("".force_encoding(Encoding::ASCII_8BIT), **@generate_opts)
        else
          CSV.new("".force_encoding(Encoding::ASCII_8BIT), **@generate_opts)
        end
      end

      def configure(conf)
        super

        @fields = fields.select{|f| !f.empty? }
        raise ConfigError, "empty value is specified in fields parameter" if @fields.empty?

        if @fields.any? { |f| record_accessor_nested?(f) }
          @accessors = @fields.map { |f| record_accessor_create(f) }
          mformat = method(:format_with_nested_fields)
          singleton_class.module_eval do
            define_method(:format, mformat)
          end
        end

        @generate_opts = {col_sep: @delimiter, force_quotes: @force_quotes, headers: @fields,
                          row_sep: @add_newline ? :auto : "".force_encoding(Encoding::ASCII_8BIT)}
      end

      def format(tag, time, record)
        csv = csv_for_thread
        line = (csv << record).string.dup
        # Need manual cleanup because CSV writer doesn't provide such method.
        csv.rewind
        csv.truncate(0)
        line
      end

      def format_with_nested_fields(tag, time, record)
        csv = csv_for_thread
        values = @accessors.map { |a| a.call(record) }
        line = (csv << values).string.dup
        # Need manual cleanup because CSV writer doesn't provide such method.
        csv.rewind
        csv.truncate(0)
        line
      end
    end
  end
end
