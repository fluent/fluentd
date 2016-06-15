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

require 'fluent/timezone'
require 'fluent/time'
require 'fluent/config/error'
require 'fluent/compat/record_filter_mixin'
require 'fluent/compat/handle_tag_name_mixin'
require 'fluent/compat/set_time_key_mixin'
require 'fluent/compat/set_tag_key_mixin'

module Fluent
  class TimeFormatter
    def initialize(format, localtime, timezone = nil)
      @tc1 = 0
      @tc1_str = nil
      @tc2 = 0
      @tc2_str = nil

      if format && format =~ /(^|[^%])(%%)*%L|(^|[^%])(%%)*%\d*N/
        define_singleton_method(:format) {|time|
          format_with_subsec(time)
        }
      else
        define_singleton_method(:format) {|time|
          format_without_subsec(time)
        }
      end

      if formatter = Fluent::Timezone.formatter(timezone, format)
        define_singleton_method(:format_nocache) {|time|
          formatter.call(time)
        }
        return
      end

      if format
        if localtime
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).strftime(format)
          }
        else
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).utc.strftime(format)
          }
        end
      else
        if localtime
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).iso8601
          }
        else
          define_singleton_method(:format_nocache) {|time|
            Time.at(time).utc.iso8601
          }
        end
      end
    end

    def format_without_subsec(time)
      if @tc1 == time
        return @tc1_str
      elsif @tc2 == time
        return @tc2_str
      else
        str = format_nocache(time)
        if @tc1 < @tc2
          @tc1 = time
          @tc1_str = str
        else
          @tc2 = time
          @tc2_str = str
        end
        return str
      end
    end

    def format_with_subsec(time)
      if Fluent::EventTime.eq?(@tc1, time)
        return @tc1_str
      elsif Fluent::EventTime.eq?(@tc2, time)
        return @tc2_str
      else
        str = format_nocache(time)
        if @tc1 < @tc2
          @tc1 = time
          @tc1_str = str
        else
          @tc2 = time
          @tc2_str = str
        end
        return str
      end
    end

    def format(time)
      # will be overridden in initialize
    end

    def format_nocache(time)
      # will be overridden in initialize
    end
  end

  RecordFilterMixin = Fluent::Compat::RecordFilterMixin
  HandleTagNameMixin = Fluent::Compat::HandleTagNameMixin
  SetTimeKeyMixin = Fluent::Compat::SetTimeKeyMixin
  SetTagKeyMixin = Fluent::Compat::SetTagKeyMixin

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
