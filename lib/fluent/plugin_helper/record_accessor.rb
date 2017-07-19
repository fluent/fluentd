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

require 'fluent/config/error'
unless {}.respond_to?(:dig)
  begin
    # backport_dig is faster than ruby_dig so prefer backport_dig.
    require 'backport_dig'
  rescue LoadError
    require 'ruby_dig'
  end
end

module Fluent
  module PluginHelper
    module RecordAccessor
      def record_accessor_create(param)
        Accessor.new(param)
      end

      class Accessor
        def initialize(param)
          @keys = Accessor.parse_parameter(param)
        end

        # To optimize the performance, use class_eval with pre-expanding @keys
        # See https://gist.github.com/repeatedly/ab553ed260cd080bd01ec71da9427312
        def call(r)
          r.dig(*@keys)
        end

        def self.parse_parameter(param)
          if param.start_with?('$.')
            parse_dot_notation(param)
          elsif param.start_with?('$[')
            parse_bracket_notation(param)
          else
            param
          end
        end

        def self.parse_dot_notation(param)
          result = []
          keys = param[2..-1].split('.')
          keys.each { |key|
            if key.include?('[')
              result.concat(parse_dot_array_op(key, param))
            else
              result << key
            end
          }

          raise Fluent::ConfigError, "empty keys" if result.empty?

          result
        end

        def self.parse_dot_array_op(key, param)
          start = key.index('[')
          result = [key[0..start - 1]]
          key = key[start + 1..-1]
          in_bracket = true

          until key.empty?
            if in_bracket
              if i = key.index(']')
                result << Integer(key[0..i - 1])
                key = key[i + 1..-1]
                in_bracket = false
              else
                raise Fluent::ConfigError, "'[' found but ']' not found. Invalid syntax: #{param}"
              end
            else
              if i = key.index('[')
                key = key[i + 1..-1]
                in_bracket = true
              else
                raise Fluent::ConfigError, "found more characters after ']'. Invalid syntax: #{param}"
              end
            end
          end

          result
        end

        def self.parse_bracket_notation(param)
          orig_param = param
          result = []
          param = param[1..-1]
          in_bracket = false

          until param.empty?
            if in_bracket
              if param[0] == "'"
                if i = param.index("']")
                  result << param[1..i - 1]
                  param = param[i + 2..-1]
                  in_bracket = false
                else
                  raise Fluent::ConfigError, "Incomplete bracket. Invalid syntax: #{orig_param}"
                end
              else
                if i = param.index(']')
                  result << Integer(param[0..i - 1])
                  param = param[i + 1..-1]
                  in_bracket = false
                else
                  raise Fluent::ConfigError, "'[' found but ']' not found. Invalid syntax: #{orig_param}"
                end
              end
            else
              if i = param.index('[')
                param = param[i + 1..-1]
                in_bracket = true
              else
                raise Fluent::ConfigError, "found more characters after ']'. Invalid syntax: #{orig_param}"
              end
            end
          end

          raise Fluent::ConfigError, "empty keys" if result.empty?

          result
        end
      end
    end
  end
end
