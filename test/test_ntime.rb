require_relative 'helper'

class NanoTimeTest < Test::Unit::TestCase
  include Fluent

  test '#sec' do
    assert_equal(1, NanoTime.new(1, 2).sec)
  end

  test '#nsec' do
    assert_equal(2, NanoTime.new(1, 2).nsec)
    assert_equal(0, NanoTime.new(1).nsec)
  end

  test '#to_int' do
    assert_equal(1, NanoTime.new(1, 2).to_int)
  end

  test '#to_r' do
    assert_equal(Rational(1_000_000_002, 1_000_000_000), NanoTime.new(1, 2).to_r)
  end

  test '#to_s' do
    time = NanoTime.new(100)
    assert_equal('100', time.to_s)
    assert_equal('100', "#{time}")
  end

  test '.from_time' do
    sec = 1000
    usec = 2
    time = NanoTime.from_time(Time.at(sec, usec))
    assert_equal(time.sec, sec)
    assert_equal(time.nsec, usec * 1000)
  end

  test '.now' do
    sec = 1000
    usec = 2
    time = Time.at(sec, usec)
    Timecop.freeze(time)
    assert_equal(sec, NanoTime.now.sec)
    assert_equal(usec * 1000, NanoTime.now.nsec)
    Timecop.return
  end

  test '==' do
    assert(NanoTime.new(1, 2) == NanoTime.new(1, 2))
    assert(NanoTime.new(1, 2) == NanoTime.new(1, 3))
    refute(NanoTime.new(1, 2) == NanoTime.new(3, 2))
    refute(NanoTime.new(1, 2) == NanoTime.new(3, 4))
    
    assert(NanoTime.new(1, 2) == 1)
    refute(NanoTime.new(1, 2) == 2)

    assert(1 == NanoTime.new(1, 2))
    refute(2 == NanoTime.new(1, 2))
  end

  test '+' do
    assert_equal(4, NanoTime.new(1, 2) + NanoTime.new(3, 4))
    assert_equal(6, NanoTime.new(1, 2) + 5)
    assert_equal(6, 5 + NanoTime.new(1, 2))
  end

  test '-' do
    assert_equal(-2, NanoTime.new(1, 2) - NanoTime.new(3, 4))
    assert_equal(-4, NanoTime.new(1, 2) - 5)
    assert_equal(4, 5 - NanoTime.new(1, 2))
  end

  test '>' do
    assert(NanoTime.new(2) > NanoTime.new(1))
    refute(NanoTime.new(1) > NanoTime.new(1))
    refute(NanoTime.new(1) > NanoTime.new(2))

    assert(NanoTime.new(2) > 1)
    refute(NanoTime.new(1) > 1)
    refute(NanoTime.new(1) > 2)

    assert(2 > NanoTime.new(1))
    refute(1 > NanoTime.new(1))
    refute(1 > NanoTime.new(2))
  end

  test '>=' do
    assert(NanoTime.new(2) >= NanoTime.new(1))
    assert(NanoTime.new(1) >= NanoTime.new(1))
    refute(NanoTime.new(1) >= NanoTime.new(2))

    assert(NanoTime.new(2) >= 1)
    assert(NanoTime.new(1) >= 1)
    refute(NanoTime.new(1) >= 2)

    assert(2 >= NanoTime.new(1))
    assert(1 >= NanoTime.new(1))
    refute(1 >= NanoTime.new(2))
  end

  test '<' do
    assert(NanoTime.new(1) < NanoTime.new(2))
    refute(NanoTime.new(1) < NanoTime.new(1))
    refute(NanoTime.new(2) < NanoTime.new(1))

    assert(NanoTime.new(1) < 2)
    refute(NanoTime.new(1) < 1)
    refute(NanoTime.new(2) < 1)

    assert(1 < NanoTime.new(2))
    refute(1 < NanoTime.new(1))
    refute(2 < NanoTime.new(1))
  end

  test '=<' do
    assert(NanoTime.new(1) <= NanoTime.new(2))
    assert(NanoTime.new(1) <= NanoTime.new(1))
    refute(NanoTime.new(2) <= NanoTime.new(1))

    assert(NanoTime.new(1) <= 2)
    assert(NanoTime.new(1) <= 1)
    refute(NanoTime.new(2) <= 1)

    assert(1 <= NanoTime.new(2))
    assert(1 <= NanoTime.new(1))
    refute(2 <= NanoTime.new(1))
  end

  test 'Time.at' do
    sec = 1000
    nsec = 2000
    ntime = NanoTime.new(sec, nsec)
    time = Time.at(ntime)
    assert_equal(sec, time.to_i)
    assert_equal(nsec, time.nsec)
  end
end
