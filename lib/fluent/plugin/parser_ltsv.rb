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
require 'fluent/time'

module Fluent
  module Plugin
    class LabeledTSVParser < ValuesParser
      Plugin.register_parser('ltsv', self)

      config_param :delimiter,       :string, default: "\t"
      config_param :label_delimiter, :string, default: ":"
      config_param :time_key, :string, default: "time"

      def configure(conf)
        # this assignment is not to raise ConfigError in ValuesParser#configure
        conf['keys'] = conf['time_key'] || 'time'
        super(conf)
      end

      def parse(text)
        # TODO: thread unsafe: @keys might be changed by other threads
        @keys  = []
        values = []

        text.split(delimiter).each do |pair|
          key, value = pair.split(label_delimiter, 2)
          @keys.push(key)
          values.push(value)
        end

        yield values_map(values)
      end
    end
  end
end
