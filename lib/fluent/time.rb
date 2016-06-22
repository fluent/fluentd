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
require 'fluent/timezone'

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

  class TimeFormatter
    def initialize(format, localtime, timezone = nil)
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
