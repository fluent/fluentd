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

module Fluent
  module Plugin
    class TSVFormatter < Formatter
      Plugin.register_formatter('tsv', self)

      desc 'Field names included in each lines'
      config_param :keys, :array, value_type: :string
      desc 'The delimiter character (or string) of TSV values'
      config_param :delimiter, :string, default: "\t"
      desc 'The parameter to enable writing to new lines'
      config_param :add_newline, :bool, default: true

      def format(tag, time, record)
        formatted = @keys.map{|k| record[k].to_s }.join(@delimiter)
        formatted << "\n".freeze if @add_newline
        formatted
      end
    end
  end
end
