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

module Fluent
  module Plugin
    class LabeledTSVParser < Parser
      Plugin.register_parser('ltsv', self)

      desc 'The delimiter character (or string) of TSV values'
      config_param :delimiter, :string, default: "\t"
      desc 'The delimiter pattern of TSV values'
      config_param :delimiter_pattern, :regexp, default: nil
      desc 'The delimiter character between field name and value'
      config_param :label_delimiter, :string, default: ":"

      config_set_default :time_key, 'time'

      def configure(conf)
        super
        @delimiter = @delimiter_pattern || @delimiter
      end

      def parse(text)
        r = {}
        text.split(@delimiter).each do |pair|
          key, value = pair.split(@label_delimiter, 2)
          r[key] = value
        end
        time, record = convert_values(parse_time(r), r)
        yield time, record
      end
    end
  end
end
