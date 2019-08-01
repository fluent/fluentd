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

require 'csv'

module Fluent
  module Plugin
    class CSVParser < Parser
      Plugin.register_parser('csv', self)

      desc 'Names of fields included in each lines'
      config_param :keys, :array, value_type: :string
      desc 'The delimiter character (or string) of CSV values'
      config_param :delimiter, :string, default: ','
      desc 'The parser type used to parse CSV line'
      config_param :parser_type, :enum, list: [:normal, :fast], default: :normal

      def configure(conf)
        super

        @quote_char = '"'
        @escape_pattern = Regexp.compile(@quote_char * 2)

        if @parser_type == :fast
          m = method(:parse_fast)
          self.singleton_class.module_eval do
            define_method(:parse, m)
          end
        end
      end

      def parse(text, &block)
        values = CSV.parse_line(text, col_sep: @delimiter)
        r = Hash[@keys.zip(values)]
        time, record = convert_values(parse_time(r), r)
        yield time, record
      end

      def parse_fast(text, &block)
        r = parse_fast_internal(text)
        time, record = convert_values(parse_time(r), r)
        yield time, record
      end

      # CSV.parse_line is too slow due to initialize lots of object and
      # CSV module doesn't provide the efficient method for parsing single line.
      # This method avoids the overhead of CSV.parse_line for typical patterns
      def parse_fast_internal(text)
        record = {}
        text.chomp!

        return record if text.empty?

        # use while because while is now faster than each_with_index
        columns = text.split(@delimiter, -1)
        num_columns = columns.size
        i = 0
        j = 0
        while j < num_columns
          column = columns[j]

          case column.count(@quote_char)
          when 0
            if column.empty?
              column = nil
            end
          when 1
            if column.start_with?(@quote_char)
              to_merge = [column]
              j += 1
              while j < num_columns
                merged_col = columns[j]
                to_merge << merged_col
                break if merged_col.end_with?(@quote_char)
                j += 1
              end
              column = to_merge.join(@delimiter)[1..-2]
            end
          when 2
            if column.start_with?(@quote_char) && column.end_with?(@quote_char)
              column = column[1..-2]
            end
          else
            if column.start_with?(@quote_char) && column.end_with?(@quote_char)
              column = column[1..-2]
            end
            column.gsub!(@escape_pattern, @quote_char)
          end

          record[@keys[i]] = column
          j += 1
          i += 1
        end
        record
      end
    end
  end
end
