require_relative 'helper'
require 'timecop'

class NanoTimeTest < Test::Unit::TestCase
  setup do
    @now = Time.now
    Timecop.freeze(@now)
  end

  teardown do
    Timecop.return
  end

  test '#sec' do
    assert_equal(1, Fluent::NanoTime.new(1, 2).sec)
  end

  test '#nsec' do
    assert_equal(2, Fluent::NanoTime.new(1, 2).nsec)
    assert_equal(0, Fluent::NanoTime.new(1).nsec)
  end

  test '#to_int' do
    assert_equal(1, Fluent::NanoTime.new(1, 2).to_int)
  end

  test '#to_r' do
    assert_equal(Rational(1_000_000_002, 1_000_000_000), Fluent::NanoTime.new(1, 2).to_r)
  end

  test '#to_s' do
    time = Fluent::NanoTime.new(100)
    assert_equal('100', time.to_s)
    assert_equal('100', "#{time}")
  end

  test '.from_time' do
    sec = 1000
    usec = 2
    time = Fluent::NanoTime.from_time(Time.at(sec, usec))
    assert_equal(time.sec, sec)
    assert_equal(time.nsec, usec * 1000)
  end

  test 'now' do
    assert_equal(@now.to_i, Fluent::NanoTime.now.sec)
    assert_equal(@now.nsec, Fluent::NanoTime.now.nsec)
  end

  test 'parse' do
    assert_equal(Time.parse("2011-01-02 13:14:15").to_i, Fluent::NanoTime.parse("2011-01-02 13:14:15").sec)
    assert_equal(Time.parse("2011-01-02 13:14:15").nsec, Fluent::NanoTime.parse("2011-01-02 13:14:15").nsec)
  end

  test 'eq?' do
    assert(Fluent::NanoTime.eq?(Fluent::NanoTime.new(1, 2), Fluent::NanoTime.new(1, 2)))
    refute(Fluent::NanoTime.eq?(Fluent::NanoTime.new(1, 2), Fluent::NanoTime.new(1, 3)))
    refute(Fluent::NanoTime.eq?(Fluent::NanoTime.new(1, 2), Fluent::NanoTime.new(3, 2)))
    refute(Fluent::NanoTime.eq?(Fluent::NanoTime.new(1, 2), Fluent::NanoTime.new(3, 4)))

    assert(Fluent::NanoTime.eq?(Fluent::NanoTime.new(1, 2), 1))
    refute(Fluent::NanoTime.eq?(Fluent::NanoTime.new(1, 2), 2))

    assert(Fluent::NanoTime.eq?(1, Fluent::NanoTime.new(1, 2)))
    refute(Fluent::NanoTime.eq?(2, Fluent::NanoTime.new(1, 2)))
  end

  test '==' do
    assert(Fluent::NanoTime.new(1, 2) == Fluent::NanoTime.new(1, 2))
    assert(Fluent::NanoTime.new(1, 2) == Fluent::NanoTime.new(1, 3))
    refute(Fluent::NanoTime.new(1, 2) == Fluent::NanoTime.new(3, 2))
    refute(Fluent::NanoTime.new(1, 2) == Fluent::NanoTime.new(3, 4))

    assert(Fluent::NanoTime.new(1, 2) == 1)
    refute(Fluent::NanoTime.new(1, 2) == 2)

    assert(1 == Fluent::NanoTime.new(1, 2))
    refute(2 == Fluent::NanoTime.new(1, 2))
  end

  test '+' do
    assert_equal(4, Fluent::NanoTime.new(1, 2) + Fluent::NanoTime.new(3, 4))
    assert_equal(6, Fluent::NanoTime.new(1, 2) + 5)
    assert_equal(6, 5 + Fluent::NanoTime.new(1, 2))
  end

  test '-' do
    assert_equal(-2, Fluent::NanoTime.new(1, 2) - Fluent::NanoTime.new(3, 4))
    assert_equal(-4, Fluent::NanoTime.new(1, 2) - 5)
    assert_equal(4, 5 - Fluent::NanoTime.new(1, 2))
  end

  test '>' do
    assert(Fluent::NanoTime.new(2) > Fluent::NanoTime.new(1))
    refute(Fluent::NanoTime.new(1) > Fluent::NanoTime.new(1))
    refute(Fluent::NanoTime.new(1) > Fluent::NanoTime.new(2))

    assert(Fluent::NanoTime.new(2) > 1)
    refute(Fluent::NanoTime.new(1) > 1)
    refute(Fluent::NanoTime.new(1) > 2)

    assert(2 > Fluent::NanoTime.new(1))
    refute(1 > Fluent::NanoTime.new(1))
    refute(1 > Fluent::NanoTime.new(2))
  end

  test '>=' do
    assert(Fluent::NanoTime.new(2) >= Fluent::NanoTime.new(1))
    assert(Fluent::NanoTime.new(1) >= Fluent::NanoTime.new(1))
    refute(Fluent::NanoTime.new(1) >= Fluent::NanoTime.new(2))

    assert(Fluent::NanoTime.new(2) >= 1)
    assert(Fluent::NanoTime.new(1) >= 1)
    refute(Fluent::NanoTime.new(1) >= 2)

    assert(2 >= Fluent::NanoTime.new(1))
    assert(1 >= Fluent::NanoTime.new(1))
    refute(1 >= Fluent::NanoTime.new(2))
  end

  test '<' do
    assert(Fluent::NanoTime.new(1) < Fluent::NanoTime.new(2))
    refute(Fluent::NanoTime.new(1) < Fluent::NanoTime.new(1))
    refute(Fluent::NanoTime.new(2) < Fluent::NanoTime.new(1))

    assert(Fluent::NanoTime.new(1) < 2)
    refute(Fluent::NanoTime.new(1) < 1)
    refute(Fluent::NanoTime.new(2) < 1)

    assert(1 < Fluent::NanoTime.new(2))
    refute(1 < Fluent::NanoTime.new(1))
    refute(2 < Fluent::NanoTime.new(1))
  end

  test '=<' do
    assert(Fluent::NanoTime.new(1) <= Fluent::NanoTime.new(2))
    assert(Fluent::NanoTime.new(1) <= Fluent::NanoTime.new(1))
    refute(Fluent::NanoTime.new(2) <= Fluent::NanoTime.new(1))

    assert(Fluent::NanoTime.new(1) <= 2)
    assert(Fluent::NanoTime.new(1) <= 1)
    refute(Fluent::NanoTime.new(2) <= 1)

    assert(1 <= Fluent::NanoTime.new(2))
    assert(1 <= Fluent::NanoTime.new(1))
    refute(2 <= Fluent::NanoTime.new(1))
  end

  test 'Time.at' do
    sec = 1000
    nsec = 2000
    ntime = Fluent::NanoTime.new(sec, nsec)
    time = Time.at(ntime)
    assert_equal(sec, time.to_i)
    assert_equal(nsec, time.nsec)
  end
end
