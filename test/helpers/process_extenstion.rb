require 'timecop'

module Process
  class << self
    alias_method :clock_gettime_original, :clock_gettime

    def clock_gettime(clock_id, unit = :float_second)
      # now only support CLOCK_REALTIME
      if Process::CLOCK_REALTIME == clock_id
        t = Time.now

        case unit
        when :float_second
          t.to_i + t.nsec / 1_000_000_000.0
        when :float_millisecond
          t.to_i * 1_000 + t.nsec / 1_000_000.0
        when :float_microsecond
          t.to_i * 1_000_000 + t.nsec / 1_000.0
        when :second
          t.to_i
        when :millisecond
          t.to_i * 1000 + t.nsec / 1_000_000
        when :microsecond
          t.to_i * 1_000_000 + t.nsec / 1_000
        when :nanosecond
          t.to_i * 1_000_000_000 + t.nsec
        end
      else
        Process.clock_gettime_original(clock_id, unit)
      end
    end
  end
end
