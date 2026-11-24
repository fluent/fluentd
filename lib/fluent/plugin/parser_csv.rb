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
      config_param :parser_engine, :enum, list: [:normal, :fast], default: :normal, alias: :parser_type

      def configure(conf)
        super
        @key_num = @keys.length

        if @parser_engine == :fast
          @quote_char = '"'
          @escape_pattern = Regexp.compile(@quote_char * 2)

          m = method(:parse_fast)
          self.singleton_class.module_eval do
            define_method(:parse, m)
          end
        end
      end

      def parse(text, &block)
        r = {}
        i = 0
        values = CSV.parse_line(text, col_sep: @delimiter)
        unless values
          yield nil, nil
          return
        end

        while i < @key_num
          r[@keys[i]] = values[i]
          i += 1
        end
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

          # A quoted value with the delimiter inside it was split into several columns.
          # Escaped quotes ("") next to the delimiter can make the pieces look closed too
          # early, so join them only when the result is a well-formed quoted value.
          # Anything else is left to the logic below, as before.
          if column.start_with?(@quote_char) && column.count(@quote_char).odd?
            merged, last = join_quoted_columns(columns, j)
            if merged
              record[@keys[i]] = merged
              j = last + 1
              i += 1
              next
            end
          end

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

      # Joins columns from `from` until they make one well-formed quoted value, in which
      # every quote inside is doubled. Returns the unescaped value and the index of its
      # last column, or nil when there is no such value. The work is linear in the length
      # of the line: each column is scanned once and the value is joined once.
      def join_quoted_columns(columns, from)
        return nil unless quote_state(columns[from], 1) == :open

        k = from
        while (k += 1) < columns.size
          case quote_state(columns[k], 0)
          when :open
            next
          when :closed
            inner = columns[from..k].join(@delimiter)[1..-2]
            return inner.gsub(@escape_pattern, @quote_char), k
          else
            return nil
          end
        end
        nil
      end

      # Reads the quotes of a column from `pos` as adjacent pairs. :open means that all of
      # them are paired, so the value goes on in the next column. :closed means that
      # exactly one is left and it is the last character. Anything else is :malformed.
      def quote_state(column, pos)
        while (idx = column.index(@quote_char, pos))
          if column[idx + 1] == @quote_char
            pos = idx + 2
          elsif idx == column.size - 1
            return :closed
          else
            return :malformed
          end
        end
        :open
      end
    end
  end
end
