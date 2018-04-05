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
      config_param :expression, :regexp
      desc 'Ignore case in matching'
      config_param :ignorecase, :bool, default: false, deprecated: "Use /pattern/i instead, this option is no longer effective"
      desc 'Build regular expression as a multline mode'
      config_param :multiline, :bool, default: false, deprecated: "Use /pattern/m instead, this option is no longer effective"

      config_set_default :time_key, 'time'

      def configure(conf)
        super
        # For compat layer
        if @ignorecase || @multiline
          options = 0
          options |= Regexp::IGNORECASE if @ignorecase
          options |= Regexp::MULTILINE if @multiline
          @expression = Regexp.compile(@expression.source, options)
        end
        @regexp = @expression # For backward compatibility
      end

      def parse(text)
        m = @expression.match(text)
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
