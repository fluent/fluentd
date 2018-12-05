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

      def parse(text)
        values = CSV.parse_line(text, col_sep: @delimiter)
        r = Hash[@keys.zip(values)]
        time, record = convert_values(parse_time(r), r)
        yield time, record
      end
    end
  end
end
