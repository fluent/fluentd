require_relative 'helper'
require 'timecop'
require 'oj'
require 'yajl'

class EventTimeTest < Test::Unit::TestCase
  setup do
    @now = Time.now
    Timecop.freeze(@now)
  end

  teardown do
    Timecop.return
  end

  test '#sec' do
    assert_equal(1, Fluent::EventTime.new(1, 2).sec)
  end

  test '#nsec' do
    assert_equal(2, Fluent::EventTime.new(1, 2).nsec)
    assert_equal(0, Fluent::EventTime.new(1).nsec)
  end

  test '#to_int' do
    assert_equal(1, Fluent::EventTime.new(1, 2).to_int)
  end

  test '#to_r' do
    assert_equal(Rational(1_000_000_002, 1_000_000_000), Fluent::EventTime.new(1, 2).to_r)
  end

  test '#to_s' do
    time = Fluent::EventTime.new(100)
    assert_equal('100', time.to_s)
    assert_equal('100', "#{time}")
  end

  test '#to_json' do
    time = Fluent::EventTime.new(100)
    assert_equal('100', time.to_json)
    assert_equal('{"time":100}', {'time' => time}.to_json)
    assert_equal('["tag",100,{"key":"value"}]', ["tag", time, {"key" => "value"}].to_json)
  end

  test 'JSON.dump' do
    time = Fluent::EventTime.new(100)
    assert_equal('{"time":100}', JSON.dump({'time' => time}))
    assert_equal('["tag",100,{"key":"value"}]', JSON.dump(["tag", time, {"key" => "value"}]))
  end

  test 'Oj.dump' do
    time = Fluent::EventTime.new(100)
    require 'fluent/env'
    Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
    assert_equal('{"time":100}', Oj.dump({'time' => time}))
    assert_equal('["tag",100,{"key":"value"}]', Oj.dump(["tag", time, {"key" => "value"}], mode: :compat))
  end

  test 'Yajl.dump' do
    time = Fluent::EventTime.new(100)
    assert_equal('{"time":100}', Yajl.dump({'time' => time}))
    assert_equal('["tag",100,{"key":"value"}]', Yajl.dump(["tag", time, {"key" => "value"}]))
  end

  test '.from_time' do
    sec = 1000
    usec = 2
    time = Fluent::EventTime.from_time(Time.at(sec, usec))
    assert_equal(time.sec, sec)
    assert_equal(time.nsec, usec * 1000)
  end

  test 'now' do
    assert_equal(@now.to_i, Fluent::EventTime.now.sec)
    assert_equal(@now.nsec, Fluent::EventTime.now.nsec)
  end

  test 'parse' do
    assert_equal(Time.parse("2011-01-02 13:14:15").to_i, Fluent::EventTime.parse("2011-01-02 13:14:15").sec)
    assert_equal(Time.parse("2011-01-02 13:14:15").nsec, Fluent::EventTime.parse("2011-01-02 13:14:15").nsec)
  end

  test 'eq?' do
    assert(Fluent::EventTime.eq?(Fluent::EventTime.new(1, 2), Fluent::EventTime.new(1, 2)))
    refute(Fluent::EventTime.eq?(Fluent::EventTime.new(1, 2), Fluent::EventTime.new(1, 3)))
    refute(Fluent::EventTime.eq?(Fluent::EventTime.new(1, 2), Fluent::EventTime.new(3, 2)))
    refute(Fluent::EventTime.eq?(Fluent::EventTime.new(1, 2), Fluent::EventTime.new(3, 4)))

    assert(Fluent::EventTime.eq?(Fluent::EventTime.new(1, 2), 1))
    refute(Fluent::EventTime.eq?(Fluent::EventTime.new(1, 2), 2))

    assert(Fluent::EventTime.eq?(1, Fluent::EventTime.new(1, 2)))
    refute(Fluent::EventTime.eq?(2, Fluent::EventTime.new(1, 2)))
  end

  test '==' do
    assert(Fluent::EventTime.new(1, 2) == Fluent::EventTime.new(1, 2))
    assert(Fluent::EventTime.new(1, 2) == Fluent::EventTime.new(1, 3))
    refute(Fluent::EventTime.new(1, 2) == Fluent::EventTime.new(3, 2))
    refute(Fluent::EventTime.new(1, 2) == Fluent::EventTime.new(3, 4))

    assert(Fluent::EventTime.new(1, 2) == 1)
    refute(Fluent::EventTime.new(1, 2) == 2)

    assert(1 == Fluent::EventTime.new(1, 2))
    refute(2 == Fluent::EventTime.new(1, 2))
  end

  test '+' do
    assert_equal(4, Fluent::EventTime.new(1, 2) + Fluent::EventTime.new(3, 4))
    assert_equal(6, Fluent::EventTime.new(1, 2) + 5)
    assert_equal(6, 5 + Fluent::EventTime.new(1, 2))
  end

  test '-' do
    assert_equal(-2, Fluent::EventTime.new(1, 2) - Fluent::EventTime.new(3, 4))
    assert_equal(-4, Fluent::EventTime.new(1, 2) - 5)
    assert_equal(4, 5 - Fluent::EventTime.new(1, 2))
  end

  test '>' do
    assert(Fluent::EventTime.new(2) > Fluent::EventTime.new(1))
    refute(Fluent::EventTime.new(1) > Fluent::EventTime.new(1))
    refute(Fluent::EventTime.new(1) > Fluent::EventTime.new(2))

    assert(Fluent::EventTime.new(2) > 1)
    refute(Fluent::EventTime.new(1) > 1)
    refute(Fluent::EventTime.new(1) > 2)

    assert(2 > Fluent::EventTime.new(1))
    refute(1 > Fluent::EventTime.new(1))
    refute(1 > Fluent::EventTime.new(2))
  end

  test '>=' do
    assert(Fluent::EventTime.new(2) >= Fluent::EventTime.new(1))
    assert(Fluent::EventTime.new(1) >= Fluent::EventTime.new(1))
    refute(Fluent::EventTime.new(1) >= Fluent::EventTime.new(2))

    assert(Fluent::EventTime.new(2) >= 1)
    assert(Fluent::EventTime.new(1) >= 1)
    refute(Fluent::EventTime.new(1) >= 2)

    assert(2 >= Fluent::EventTime.new(1))
    assert(1 >= Fluent::EventTime.new(1))
    refute(1 >= Fluent::EventTime.new(2))
  end

  test '<' do
    assert(Fluent::EventTime.new(1) < Fluent::EventTime.new(2))
    refute(Fluent::EventTime.new(1) < Fluent::EventTime.new(1))
    refute(Fluent::EventTime.new(2) < Fluent::EventTime.new(1))

    assert(Fluent::EventTime.new(1) < 2)
    refute(Fluent::EventTime.new(1) < 1)
    refute(Fluent::EventTime.new(2) < 1)

    assert(1 < Fluent::EventTime.new(2))
    refute(1 < Fluent::EventTime.new(1))
    refute(2 < Fluent::EventTime.new(1))
  end

  test '=<' do
    assert(Fluent::EventTime.new(1) <= Fluent::EventTime.new(2))
    assert(Fluent::EventTime.new(1) <= Fluent::EventTime.new(1))
    refute(Fluent::EventTime.new(2) <= Fluent::EventTime.new(1))

    assert(Fluent::EventTime.new(1) <= 2)
    assert(Fluent::EventTime.new(1) <= 1)
    refute(Fluent::EventTime.new(2) <= 1)

    assert(1 <= Fluent::EventTime.new(2))
    assert(1 <= Fluent::EventTime.new(1))
    refute(2 <= Fluent::EventTime.new(1))
  end

  test 'Time.at' do
    sec = 1000
    nsec = 2000
    ntime = Fluent::EventTime.new(sec, nsec)
    time = Time.at(ntime)
    assert_equal(sec, time.to_i)
    assert_equal(nsec, time.nsec)
  end
end
