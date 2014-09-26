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
      # Get the offset of the specified time zone in seconds.
      #
      # Accepted formats are as follows. Note that time zone abbreviations
      # such as PST and JST are not supported intentionally.
      #
      #   1. [+-]HH:MM    (e.g. "+09:00")
      #   2. [+-]HHMM     (e.g. "+0900")
      #   3. [+-]HH       (e.g. "+09")
      #   4. Region/Zone  (e.g. "Asia/Tokyo")
      #
      # When the specified time zone is unknown or nil, nil is returned.
      #
      def offset(timezone)
        # [+-]HH:MM, [+-]HH
        if %r{\A[+-]\d\d(:?\d\d)?\z} =~ timezone
          return Time.zone_offset(timezone)
        end

        # Region/Zone
        if %r{\A[^/]+/[^/]+\z} =~ timezone
          begin
            # Get the offset using TZInfo library.
            return TZInfo::Timezone.get(timezone).current_period.offset.utc_total_offset
          rescue
            # The specified time zone is unknown.
            return nil
          end
        end

        # The specified time zone is unknown or nil.
        return nil
      end
    end
  end
end
