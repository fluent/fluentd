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
    class LabeledTSVFormatter < Formatter
      Plugin.register_formatter('ltsv', self)

      # http://ltsv.org/

      config_param :delimiter, :string, default: "\t"
      config_param :label_delimiter, :string, default: ":"
      config_param :add_newline, :bool, default: true

      # TODO: escaping for \t in values
      def format(tag, time, record)
        formatted = ""
        record.each do |label, value|
          formatted << @delimiter if formatted.length.nonzero?
          formatted << "#{label}#{@label_delimiter}#{value}"
        end
        formatted << "\n".freeze if @add_newline
        formatted
      end
    end
  end
end
