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
        # If the specified time zone is nil.
        if timezone.nil?
          return nil
        end

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

        # The specified time zone is unknown.
        return nil
      end

      #
      # Get the offset of the specified time zone in seconds.
      #
      # If the given time zone is not null and failed to be converted into
      # an offset value, ConfigError is raised.
      #
      def offset_or_config_error(timezone)
        # If the specified time zone is nil.
        if timezone.nil?
          return nil
        end

        # Get the offset value of the time zone.
        value = offset(timezone)

        # If the specified time zone failed to be parsed.
        if value.nil?
          raise ConfigError, "Unsupported timezone '#{timezone}'"
        end

        # Return the offset value of the time zone.
        return value
      end

      #
      # Validate the given time zone.
      #
      alias_method :validate, :offset_or_config_error
    end
  end
end
