require_relative 'helper'
require 'fluent/test'
require 'fluent/time'

class TimeFormatterTest < ::Test::Unit::TestCase
  def with_timezone(tz)
    oldtz, ENV['TZ'] = ENV['TZ'], tz
    yield
  ensure
    ENV['TZ'] = oldtz
  end

  def setup
    @time = Time.new(2014, 9, 27, 0, 0, 0, 0).to_i
    @fmt  = "%Y%m%d %H%M%z"  # YYYYMMDD HHMM[+-]HHMM
  end

  def format(format, localtime, timezone)
    formatter = Fluent::TimeFormatter.new(format, localtime, timezone)
    formatter.format(@time)
  end

  def test_default_utc_nil
    assert_equal("2014-09-27T00:00:00Z", format(nil, false, nil))
  end

  def test_default_utc_pHH_MM
    assert_equal("2014-09-27T01:30:00+01:30", format(nil, false, "+01:30"))
  end

  def test_default_utc_nHH_MM
    assert_equal("2014-09-26T22:30:00-01:30", format(nil, false, "-01:30"))
  end

  def test_default_utc_pHHMM
    assert_equal("2014-09-27T02:30:00+02:30", format(nil, false, "+0230"))
  end

  def test_default_utc_nHHMM
    assert_equal("2014-09-26T21:30:00-02:30", format(nil, false, "-0230"))
  end

  def test_default_utc_pHH
    assert_equal("2014-09-27T03:00:00+03:00", format(nil, false, "+03"))
  end

  def test_default_utc_nHH
    assert_equal("2014-09-26T21:00:00-03:00", format(nil, false, "-03"))
  end

  def test_default_utc_timezone_1
    # Asia/Tokyo (+09:00) does not have daylight saving time.
    assert_equal("2014-09-27T09:00:00+09:00", format(nil, false, "Asia/Tokyo"))
  end

  def test_default_utc_timezone_2
    # Pacific/Honolulu (-10:00) does not have daylight saving time.
    assert_equal("2014-09-26T14:00:00-10:00", format(nil, false, "Pacific/Honolulu"))
  end

  def test_default_utc_timezone_3
    # America/Argentina/Buenos_Aires (-03:00) does not have daylight saving time.
    assert_equal("2014-09-26T21:00:00-03:00", format(nil, false, "America/Argentina/Buenos_Aires"))
  end

  def test_default_utc_timezone_4
    # Europe/Paris has daylight saving time. Its UTC offset is +01:00 and its
    # UTC offset in DST is +02:00. In September, Europe/Paris is in DST.
    assert_equal("2014-09-27T02:00:00+02:00", format(nil, false, "Europe/Paris"))
  end

  def test_default_utc_timezone_5
    # Europe/Paris has daylight saving time. Its UTC offset is +01:00 and its
    # UTC offset in DST is +02:00. In January, Europe/Paris is not in DST.
    @time = Time.new(2014, 1, 24, 0, 0, 0, 0).to_i
    assert_equal("2014-01-24T01:00:00+01:00", format(nil, false, "Europe/Paris"))
  end

  def test_default_utc_invalid
    assert_equal("2014-09-27T00:00:00Z", format(nil, false, "Invalid"))
  end

  def test_default_localtime_nil_1
    with_timezone("UTC-04") do
      assert_equal("2014-09-27T04:00:00+04:00", format(nil, true, nil))
    end
  end

  def test_default_localtime_nil_2
    with_timezone("UTC+05") do
      assert_equal("2014-09-26T19:00:00-05:00", format(nil, true, nil))
    end
  end

  def test_default_localtime_timezone
    # 'timezone' takes precedence over 'localtime'.
    with_timezone("UTC-06") do
      assert_equal("2014-09-27T07:00:00+07:00", format(nil, true, "+07"))
    end
  end

  def test_specific_utc_nil
    assert_equal("20140927 0000+0000", format(@fmt, false, nil))
  end

  def test_specific_utc_pHH_MM
    assert_equal("20140927 0830+0830", format(@fmt, false, "+08:30"))
  end

  def test_specific_utc_nHH_MM
    assert_equal("20140926 1430-0930", format(@fmt, false, "-09:30"))
  end

  def test_specific_utc_pHHMM
    assert_equal("20140927 1030+1030", format(@fmt, false, "+1030"))
  end

  def test_specific_utc_nHHMM
    assert_equal("20140926 1230-1130", format(@fmt, false, "-1130"))
  end

  def test_specific_utc_pHH
    assert_equal("20140927 1200+1200", format(@fmt, false, "+12"))
  end

  def test_specific_utc_nHH
    assert_equal("20140926 1100-1300", format(@fmt, false, "-13"))
  end

  def test_specific_utc_timezone_1
    # Europe/Moscow (+04:00) does not have daylight saving time.
    assert_equal("20140927 0400+0400", format(@fmt, false, "Europe/Moscow"))
  end

  def test_specific_utc_timezone_2
    # Pacific/Galapagos (-06:00) does not have daylight saving time.
    assert_equal("20140926 1800-0600", format(@fmt, false, "Pacific/Galapagos"))
  end

  def test_specific_utc_timezone_3
    # America/Argentina/Buenos_Aires (-03:00) does not have daylight saving time.
    assert_equal("20140926 2100-0300", format(@fmt, false, "America/Argentina/Buenos_Aires"))
  end

  def test_specific_utc_timezone_4
    # America/Los_Angeles has daylight saving time. Its UTC offset is -08:00 and its
    # UTC offset in DST is -07:00. In September, America/Los_Angeles is in DST.
    assert_equal("20140926 1700-0700", format(@fmt, false, "America/Los_Angeles"))
  end

  def test_specific_utc_timezone_5
    # America/Los_Angeles has daylight saving time. Its UTC offset is -08:00 and its
    # UTC offset in DST is -07:00. In January, America/Los_Angeles is not in DST.
    @time = Time.new(2014, 1, 24, 0, 0, 0, 0).to_i
    assert_equal("20140123 1600-0800", format(@fmt, false, "America/Los_Angeles"))
  end

  def test_specific_utc_invalid
    assert_equal("20140927 0000+0000", format(@fmt, false, "Invalid"))
  end

  def test_specific_localtime_nil_1
    with_timezone("UTC-07") do
      assert_equal("20140927 0700+0700", format(@fmt, true, nil))
    end
  end

  def test_specific_localtime_nil_2
    with_timezone("UTC+08") do
      assert_equal("20140926 1600-0800", format(@fmt, true, nil))
    end
  end

  def test_specific_localtime_timezone
    # 'timezone' takes precedence over 'localtime'.
    with_timezone("UTC-09") do
      assert_equal("20140926 1400-1000", format(@fmt, true, "-10"))
    end
  end

  def test_format_with_subsec
    time = Fluent::EventTime.new(@time)
    formatter = Fluent::TimeFormatter.new("%Y%m%d %H%M.%N", false, nil)
    assert_equal("20140927 0000.000000000", formatter.format(time))
  end
end
