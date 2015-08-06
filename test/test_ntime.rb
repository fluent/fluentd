require_relative 'helper'

class NTimeTest < Test::Unit::TestCase
  include Fluent

  test '#sec' do
    assert_equal(1, NTime.new(1, 2).sec)
  end

  test '#nsec' do
    assert_equal(2, NTime.new(1, 2).nsec)
    assert_equal(0, NTime.new(1).nsec)
  end

  test '#to_int' do
    assert_equal(1, NTime.new(1, 2).to_int)
  end

  test '#to_r' do
    assert_equal(Rational(1_000_000_002, 1_000_000_000), NTime.new(1, 2).to_r)
  end

  test '.from_time' do
    sec = 1000
    usec = 2
    time = NTime.from_time(Time.at(sec, usec))
    assert_equal(time.sec, sec)
    assert_equal(time.nsec, usec * 1000)
  end

  test '.now' do
    sec = 1000
    usec = 2
    time = Time.at(sec, usec)
    Timecop.freeze(time)
    assert_equal(sec, NTime.now.sec)
    assert_equal(usec * 1000, NTime.now.nsec)
    Timecop.return
  end

  test '==' do
    assert(NTime.new(1, 2) == NTime.new(1, 2))
    assert(NTime.new(1, 2) == NTime.new(1, 3))
    refute(NTime.new(1, 2) == NTime.new(3, 2))
    refute(NTime.new(1, 2) == NTime.new(3, 4))
    
    assert(NTime.new(1, 2) == 1)
    refute(NTime.new(1, 2) == 2)

    assert(1 == NTime.new(1, 2))
    refute(2 == NTime.new(1, 2))
  end

  test '+' do
    assert_equal(4, NTime.new(1, 2) + NTime.new(3, 4))
    assert_equal(6, NTime.new(1, 2) + 5)
    assert_equal(6, 5 + NTime.new(1, 2))
  end

  test '-' do
    assert_equal(-2, NTime.new(1, 2) - NTime.new(3, 4))
    assert_equal(-4, NTime.new(1, 2) - 5)
    assert_equal(4, 5 - NTime.new(1, 2))
  end

  test '>' do
    assert(NTime.new(2) > NTime.new(1))
    refute(NTime.new(1) > NTime.new(1))
    refute(NTime.new(1) > NTime.new(2))

    assert(NTime.new(2) > 1)
    refute(NTime.new(1) > 1)
    refute(NTime.new(1) > 2)

    assert(2 > NTime.new(1))
    refute(1 > NTime.new(1))
    refute(1 > NTime.new(2))
  end

  test '>=' do
    assert(NTime.new(2) >= NTime.new(1))
    assert(NTime.new(1) >= NTime.new(1))
    refute(NTime.new(1) >= NTime.new(2))

    assert(NTime.new(2) >= 1)
    assert(NTime.new(1) >= 1)
    refute(NTime.new(1) >= 2)

    assert(2 >= NTime.new(1))
    assert(1 >= NTime.new(1))
    refute(1 >= NTime.new(2))
  end

  test '<' do
    assert(NTime.new(1) < NTime.new(2))
    refute(NTime.new(1) < NTime.new(1))
    refute(NTime.new(2) < NTime.new(1))

    assert(NTime.new(1) < 2)
    refute(NTime.new(1) < 1)
    refute(NTime.new(2) < 1)

    assert(1 < NTime.new(2))
    refute(1 < NTime.new(1))
    refute(2 < NTime.new(1))
  end

  test '=<' do
    assert(NTime.new(1) <= NTime.new(2))
    assert(NTime.new(1) <= NTime.new(1))
    refute(NTime.new(2) <= NTime.new(1))

    assert(NTime.new(1) <= 2)
    assert(NTime.new(1) <= 1)
    refute(NTime.new(2) <= 1)

    assert(1 <= NTime.new(2))
    assert(1 <= NTime.new(1))
    refute(2 <= NTime.new(1))
  end

  test 'Time.at' do
    sec = 1000
    nsec = 2000
    ntime = NTime.new(sec, nsec)
    time = Time.at(ntime)
    assert_equal(sec, time.to_i)
    assert_equal(nsec, time.nsec)
  end
end
