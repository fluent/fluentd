class Fluent::NTime
  def initialize(sec, nsec = 0)
    @sec = sec
    @nsec = nsec
  end

  def ==(other)
    if other.is_a?(Fluent::NTime)
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

  def to_msec
    to_nsec / 1000000
  end

  def to_usec
    to_nsec / 1000
  end

  def to_nsec
    @sec * 1000000000 + @nsec
  end

  def self.now
    time = Time.now
    Fluent::NTime.new(time.to_i, time.nsec)
  end

  def method_missing(name, *args, &block)
    @sec.send(name, *args, &block)
  end
end
