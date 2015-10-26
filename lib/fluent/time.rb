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
end
