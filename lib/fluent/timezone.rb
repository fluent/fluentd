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

require 'tzinfo'

module Fluent
  class Timezone
    # [+-]HH:MM, [+-]HHMM, [+-]HH
    NUMERIC_PATTERN = %r{\A[+-]\d\d(:?\d\d)?\z}

    # Region/Zone, Region/Zone/Zone
    NAME_PATTERN = %r{\A[^/]+/[^/]+(/[^/]+)?\z}

    # Validate the format of the specified timezone.
    #
    # Valid formats are as follows. Note that timezone abbreviations
    # such as PST and JST are not supported intentionally.
    #
    #   1. [+-]HH:MM         (e.g. "+09:00")
    #   2. [+-]HHMM          (e.g. "+0900")
    #   3. [+-]HH            (e.g. "+09")
    #   4. Region/Zone       (e.g. "Asia/Tokyo")
    #   5. Region/Zone/Zone  (e.g. "America/Argentina/Buenos_Aires")
    #
    # In the 4th and 5th cases, it is checked whether the specified
    # timezone exists in the timezone database.
    #
    # When the given timezone is valid, true is returned. Otherwise,
    # false is returned. When nil is given, false is returned.
    def self.validate(timezone)
      # If the specified timezone is nil.
      if timezone.nil?
        # Invalid.
        return false
      end

      # [+-]HH:MM, [+-]HHMM, [+-]HH
      if NUMERIC_PATTERN === timezone
        # Valid. It can be parsed by Time.zone_offset method.
        return true
      end

      # Region/Zone, Region/Zone/Zone
      if NAME_PATTERN === timezone
        begin
          # Get a Timezone instance for the specified timezone.
          TZInfo::Timezone.get(timezone)
        rescue
          # Invalid. The string does not exist in the timezone database.
          return false
        else
          # Valid. The string was found in the timezone database.
          return true
        end
      else
        # Invalid. Timezone abbreviations are not supported.
        return false
      end
    end

    # Validate the format of the specified timezone.
    #
    # The implementation of this method calls validate(timezone) method
    # to check whether the given timezone is valid. When invalid, this
    # method raises a ConfigError.
    def self.validate!(timezone)
      unless validate(timezone)
        raise ConfigError, "Unsupported timezone '#{timezone}'"
      end
    end

    # Create a formatter for a timezone and optionally a format.
    #
    # An Proc object is returned. If the given timezone is invalid,
    # nil is returned.
    def self.formatter(timezone, format = nil)
      if timezone.nil?
        return nil
      end

      # [+-]HH:MM, [+-]HHMM, [+-]HH
      if NUMERIC_PATTERN === timezone
        offset = Time.zone_offset(timezone)

        if format
          return Proc.new {|time|
            Time.at(time).localtime(offset).strftime(format)
          }
        else
          return Proc.new {|time|
            Time.at(time).localtime(offset).iso8601
          }
        end
      end

      # Region/Zone, Region/Zone/Zone
      if NAME_PATTERN === timezone
        begin
          tz = TZInfo::Timezone.get(timezone)
        rescue
          return nil
        end

        if format
          return Proc.new {|time|
            Time.at(time).localtime(tz.period_for_utc(time).utc_total_offset).strftime(format)
          }
        else
          return Proc.new {|time|
            Time.at(time).localtime(tz.period_for_utc(time).utc_total_offset).iso8601
          }
        end
      end

      return nil
    end
  end
end
