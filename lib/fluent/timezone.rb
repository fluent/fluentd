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

require 'time'
require 'tzinfo'
require 'tzinfo/timezone_offset'

module Fluent
  class TimeZone
    class << TimeZone
      #
      # Additional time zone abbreviations which are not listed in time.rb of
      # Ruby source tree.
      #
      ZoneOffset = {
        'JST' => 32400
      }

      #
      # Get the offset of the specified time zone in seconds.
      #
      # Accepted formats are as follows.
      #
      #   1. +HH:MM, -HH:MM  (e.g. "+09:00")
      #   2. Some time zone abbreviations  (e.g. "JST")
      #   3. Region/Zone  (e.g. "Asia/Tokyo")
      #
      # When the given time zone is unknown or nil, nil is returned.
      #
      def offset(timezone)
        if timezone == nil
          return nil
        end

        # First, try to get the offset of the specified time zone using
        # Time.zone_offset method. This method understands [+-]HH:MM format
        # and some time zone abbreviations listed in Time.ZoneOffset.
        # See "lib/time.rb" in Ruby source tree for details.
        offset = Time.zone_offset(timezone)
        if offset != nil
          return offset
        end

        # Second, check the additional time zone abbreviations.
        if ZoneOffset.include?(timezone)
          return ZoneOffset[timezone]
        end

        begin
          # Get the offset using TZInfo library.
          return TZInfo::Timezone.get(timezone).current_period.offset.utc_total_offset
        rescue
          # The specified time zone is unknown.
          return nil
        end
      end
    end
  end
end
