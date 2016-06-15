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

module Fluent
  module Compat
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
  end
end
