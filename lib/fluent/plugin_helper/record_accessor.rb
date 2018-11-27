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
    # backport_dig is faster than dig_rb so prefer backport_dig.
    require 'backport_dig'
  rescue LoadError
    require 'dig_rb'
  end
end

module Fluent
  module PluginHelper
    module RecordAccessor
      def record_accessor_create(param)
        Accessor.new(param)
      end

      class Accessor
        attr_reader :keys

        def initialize(param)
          @keys = Accessor.parse_parameter(param)

          if @keys.is_a?(Array)
            @last_key = @keys.last
            @dig_keys = @keys[0..-2]
            if @dig_keys.empty?
              @keys = @keys.first
              mcall = method(:call_index)
              mdelete = method(:delete_top)
            else
              mcall = method(:call_dig)
              mdelete = method(:delete_nest)
            end
          else
            # Call [] for single key to reduce dig overhead
            mcall = method(:call_index)
            mdelete = method(:delete_top)
          end

          singleton_class.module_eval do
            define_method(:call, mcall)
            define_method(:delete, mdelete)
          end
        end

        def call(r)
        end

        # To optimize the performance, use class_eval with pre-expanding @keys
        # See https://gist.github.com/repeatedly/ab553ed260cd080bd01ec71da9427312
        def call_dig(r)
          r.dig(*@keys)
        end

        def call_index(r)
          r[@keys]
        end

        def delete(r)
        end

        def delete_nest(r)
          if target = r.dig(*@dig_keys)
            if target.is_a?(Array)
              target.delete_at(@last_key)
            else
              target.delete(@last_key)
            end
          end
        rescue
          nil
        end

        def delete_top(r)
          r.delete(@keys)
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

          raise Fluent::ConfigError, "empty keys in dot notation" if result.empty?
          validate_dot_keys(result)

          result
        end

        def self.validate_dot_keys(keys)
          keys.each { |key|
            next unless key.is_a?(String)
            if /\s+/.match(key)
              raise Fluent::ConfigError, "whitespace character is not allowed in dot notation. Use bracket notation: #{key}"
            end
          }
        end

        def self.parse_dot_array_op(key, param)
          start = key.index('[')
          result = if start.zero?
                     []
                   else
                     [key[0..start - 1]]
                   end
          key = key[start + 1..-1]
          in_bracket = true

          until key.empty?
            if in_bracket
              if i = key.index(']')
                index_value = key[0..i - 1]
                raise Fluent::ConfigError, "missing array index in '[]'. Invalid syntax: #{param}" if index_value == ']'
                result << Integer(index_value)
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
              if param[0] == "'" || param[0] == '"'
                if i = param.index("']") || param.index('"]')
                  raise Fluent::ConfigError, "Mismatched quotes. Invalid syntax: #{orig_param}" unless param[0] == param[i]
                  result << param[1..i - 1]
                  param = param[i + 2..-1]
                  in_bracket = false
                else
                  raise Fluent::ConfigError, "Incomplete bracket. Invalid syntax: #{orig_param}"
                end
              else
                if i = param.index(']')
                  index_value = param[0..i - 1]
                  raise Fluent::ConfigError, "missing array index in '[]'. Invalid syntax: #{param}" if index_value == ']'
                  result << Integer(index_value)
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

          raise Fluent::ConfigError, "empty keys in bracket notation" if result.empty?

          result
        end
      end
    end
  end
end
