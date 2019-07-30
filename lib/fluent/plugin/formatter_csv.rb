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

require 'fluent/plugin/formatter'
require 'csv'

module Fluent
  module Plugin
    class CsvFormatter < Formatter
      Plugin.register_formatter('csv', self)

      config_param :delimiter, default: ',' do |val|
        ['\t', 'TAB'].include?(val) ? "\t" : val
      end
      config_param :force_quotes, :bool, default: true
      # "array" looks good for type of :fields, but this implementation removes tailing comma
      # TODO: Is it needed to support tailing comma?
      config_param :fields, :array, value_type: :string
      config_param :add_newline, :bool, default: true

      def configure(conf)
        super

        @fields = fields.select{|f| !f.empty? }
        raise ConfigError, "empty value is specified in fields parameter" if @fields.empty?

        @generate_opts = {col_sep: @delimiter, force_quotes: @force_quotes, headers: @fields,
                          row_sep: @add_newline ? :auto : "".force_encoding(Encoding::ASCII_8BIT)}
        # Cache CSV object per thread to avoid internal state sharing
        @cache = {}
      end

      def format(tag, time, record)
        csv = (@cache[Thread.current] ||= CSV.new("".force_encoding(Encoding::ASCII_8BIT), @generate_opts))
        line = (csv << record).string.dup
        # Need manual cleanup because CSV writer doesn't provide such method.
        csv.rewind
        csv.truncate(0)
        line
      end
    end
  end
end
