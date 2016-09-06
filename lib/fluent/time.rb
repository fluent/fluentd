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
require 'msgpack'
require 'strptime'
require 'fluent/timezone'
require 'fluent/configurable'
require 'fluent/config/error'

module Fluent
  class EventTime
    TYPE = 0

    def initialize(sec, nsec = 0)
      @sec = sec
      @nsec = nsec
    end

    def ==(other)
      if other.is_a?(Fluent::EventTime)
        @sec == other.sec
      else
        @sec == other
      end
    end

    def sec
      @sec
    end

    def nsec
      @nsec
    end

    def to_int
      @sec
    end

    # for Time.at
    def to_r
      Rational(@sec * 1_000_000_000 + @nsec, 1_000_000_000)
    end

    # for > and others
    def coerce(other)
      [other, @sec]
    end

    def to_s
      @sec.to_s
    end

    def to_json(*args)
      @sec.to_s
    end

    def to_msgpack(io = nil)
      @sec.to_msgpack(io)
    end

    def to_msgpack_ext
      [@sec, @nsec].pack('NN')
    end

    def self.from_msgpack_ext(data)
      new(*data.unpack('NN'))
    end

    def self.from_time(time)
      Fluent::EventTime.new(time.to_i, time.nsec)
    end

    def self.eq?(a, b)
      if a.is_a?(Fluent::EventTime) && b.is_a?(Fluent::EventTime)
        a.sec == b.sec && a.nsec == b.nsec
      else
        a == b
      end
    end

    def self.now
      from_time(Time.now)
    end

    def self.parse(*args)
      from_time(Time.parse(*args))
    end

    ## TODO: For performance, implement +, -, and so on
    def method_missing(name, *args, &block)
      @sec.send(name, *args, &block)
    end
  end

  module TimeMixin
    TIME_PARAMETERS = [
      [:time_format, :string, {default: nil}],
      [:localtime, :bool, {default: true}],  # UTC if :localtime is false and :timezone is nil
      [:utc,       :bool, {default: false}], # to turn :localtime false
      [:timezone, :string, {default: nil}],
    ]
    module TimeParameters
      include Fluent::Configurable
      TIME_PARAMETERS.each do |name, type, opts|
        config_param name, type, opts
      end
    end

    module Parser
      def self.included(mod)
        mod.include TimeParameters
      end

      def time_parser_create(format: @time_format, timezone: @timezone, force_localtime: false)
        return TimeParser.new(format, true, nil) if force_localtime

        localtime = @localtime && (timezone.nil? && !@utc)
        TimeParser.new(format, localtime, timezone)
      end
    end

    module Formatter
      def self.included(mod)
        mod.include TimeParameters
      end

      def time_formatter_create(format: @time_format, timezone: @timezone, force_localtime: false)
        return TimeFormatter.new(format, true, nil) if force_localtime

        localtime = @localtime && (timezone.nil? && !@utc)
        TimeFormatter.new(format, localtime, timezone)
      end
    end
  end

  class TimeParser
    class TimeParseError < StandardError; end

    def initialize(format = nil, localtime = true, timezone = nil)
      if format.nil? && (timezone || !localtime)
        raise Fluent::ConfigError, "specifying timezone requires time format"
      end

      @cache1_key = nil
      @cache1_time = nil
      @cache2_key = nil
      @cache2_time = nil

      format_with_timezone = format && (format.include?("%z") || format.include?("%Z"))

      # unixtime_in_expected_tz = unixtime_in_localtime + offset_diff
      offset_diff = case
                    when format_with_timezone then nil
                    when timezone  then Time.now.localtime.utc_offset - Time.zone_offset(timezone)
                    when localtime then 0
                    else Time.now.localtime.utc_offset # utc
                    end

      strptime = format && (Strptime.new(time_format) rescue nil)

      @parse = case
               when format_with_timezone && strptime then ->(v){ Fluent::EventTime.from_time(strptime.exec(v)) }
               when format_with_timezone             then ->(v){ Fluent::EventTime.from_time(Time.strptime(v, format)) }
               when strptime then ->(v){ t = strptime.exec(v);         Fluent::EventTime.new(t.to_i + offset_diff, t.nsec) }
               when format   then ->(v){ t = Time.strptime(v, format); Fluent::EventTime.new(t.to_i + offset_diff, t.nsec) }
               else ->(v){ Fluent::EventTime.parse(v) }
               end
    end

    # TODO: new cache mechanism using format string
    def parse(value)
      unless value.is_a?(String)
        raise TimeParseError, "value must be string: #{value}"
      end

      if @cache1_key == value
        return @cache1_time
      elsif @cache2_key == value
        return @cache2_time
      else
        begin
          time = @parse.call(value)
        rescue => e
          raise TimeParseError, "invalid time format: value = #{value}, error_class = #{e.class.name}, error = #{e.message}"
        end
        @cache1_key = @cache2_key
        @cache1_time = @cache2_time
        @cache2_key = value
        @cache2_time = time
        return time
      end
    end
    alias :call :parse
  end

  class TimeFormatter
    def initialize(format = nil, localtime = true, timezone = nil)
      @tc1 = 0
      @tc1_str = nil
      @tc2 = 0
      @tc2_str = nil

      if format && format =~ /(^|[^%])(%%)*%L|(^|[^%])(%%)*%\d*N/
        define_singleton_method(:format, method(:format_with_subsec))
        define_singleton_method(:call, method(:format_with_subsec))
      else
        define_singleton_method(:format, method(:format_without_subsec))
        define_singleton_method(:call, method(:format_with_subsec))
      end

      formatter = Fluent::Timezone.formatter(timezone, format)
      @format_nocache = case
                        when formatter           then formatter
                        when format && localtime then ->(time){ Time.at(time).strftime(format) }
                        when format              then ->(time){ Time.at(time).utc.strftime(format) }
                        when localtime           then ->(time){ Time.at(time).iso8601 }
                        else                          ->(time){ Time.at(time).utc.iso8601 }
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

    ## Dynamically defined in #initialize
    # def format(time)
    # end

    def format_nocache(time)
      @format_nocache.call(time)
    end
  end
end
