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

module Fluent
  module Compat
    module TypeConverter
      Converters = {
        'string' => lambda { |v| v.to_s },
        'integer' => lambda { |v| v.to_i },
        'float' => lambda { |v| v.to_f },
        'bool' => lambda { |v|
          case v.downcase
          when 'true', 'yes', '1'
            true
          else
            false
          end
        },
        'time' => lambda { |v, time_parser|
          time_parser.parse(v)
        },
        'array' => lambda { |v, delimiter|
          v.to_s.split(delimiter)
        }
      }

      def self.included(klass)
        klass.instance_eval {
          config_param :types, :string, default: nil
          config_param :types_delimiter, :string, default: ','
          config_param :types_label_delimiter, :string, default: ':'
        }
      end

      def configure(conf)
        super

        @type_converters = nil
        @type_converters = parse_types_parameter unless @types.nil?
      end

      private

      def convert_type(name, value)
        converter = @type_converters[name]
        converter.nil? ? value : converter.call(value)
      end

      def parse_types_parameter
        converters = {}

        @types.split(@types_delimiter).each { |pattern_name|
          name, type, format = pattern_name.split(@types_label_delimiter, 3)
          raise ConfigError, "Type is needed" if type.nil?

          case type
          when 'time'
            require 'fluent/parser'
            t_parser = Fluent::TextParser::TimeParser.new(format)
            converters[name] = lambda { |v|
              Converters[type].call(v, t_parser)
            }
          when 'array'
            delimiter = format || ','
            converters[name] = lambda { |v|
              Converters[type].call(v, delimiter)
            }
          else
            converters[name] = Converters[type]
          end
        }

        converters
      end
    end
  end
end
