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
    class RegexpParser < Parser
      Plugin.register_parser("regexp", self)

      desc 'Regular expression for matching logs'
      config_param :expression, :string
      desc 'Ignore case in matching'
      config_param :ignorecase, :bool, default: false
      desc 'Build regular expression as a multline mode'
      config_param :multiline, :bool, default: false

      config_set_default :time_key, 'time'

      def configure(conf)
        super

        expr = if @expression[0] == "/" && @expression[-1] == "/"
                 @expression[1..-2]
               else
                 @expression
               end
        regexp_option = 0
        regexp_option |= Regexp::IGNORECASE if @ignorecase
        regexp_option |= Regexp::MULTILINE if @multiline
        @regexp = Regexp.new(expr.force_encoding('ASCII-8BIT'), regexp_option)
      end

      def parse(text)
        m = @regexp.match(text)
        unless m
          yield nil, nil
          return
        end

        r = {}
        m.names.each do |name|
          if value = m[name]
            r[name] = value
          end
        end

        time, record = convert_values(parse_time(r), r)
        yield time, record
      end
    end
  end
end
