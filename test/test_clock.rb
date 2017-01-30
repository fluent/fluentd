require_relative 'helper'
require 'fluent/clock'

require 'timecop'

class ClockTest < ::Test::Unit::TestCase
  teardown do
    Fluent::Clock.return # call it always not to affect other tests
  end

  sub_test_case 'without any pre-operation' do
    test 'clock can provides incremental floating point number based on second' do
      c1 = Fluent::Clock.now
      assert_kind_of Float, c1
      sleep 1.1
      c2 = Fluent::Clock.now
      assert{ c2 >= c1 + 1.0 && c2 < c1 + 9.0 } # if clock returns deci-second (fantastic!), c2 should be larger than c1 + 10
    end

    test 'clock value will proceed even if timecop freezes Time' do
      Timecop.freeze(Time.now) do
        c1 = Fluent::Clock.now
        assert_kind_of Float, c1
        sleep 1.1
        c2 = Fluent::Clock.now
        assert{ c2 >= c1 + 1.0 && c2 < c1 + 9.0 }
      end
    end
  end

  sub_test_case 'using #freeze without any arguments' do
    test 'Clock.freeze without arguments freezes clock with current clock value' do
      c0 = Fluent::Clock.now
      Fluent::Clock.freeze
      c1 = Fluent::Clock.now
      Fluent::Clock.return
      c2 = Fluent::Clock.now
      assert{ c0 <= c1 && c1 <= c2 }
    end

    test 'Clock.return raises an error if it is called in block' do
      assert_raise RuntimeError.new("invalid return while running code in blocks") do
        Fluent::Clock.freeze do
          Fluent::Clock.return
        end
      end
    end
  end

  sub_test_case 'using #freeze with clock value' do
    test 'Clock.now always returns frozen time until #return called' do
      c0 = Fluent::Clock.now
      Fluent::Clock.freeze(c0)
      assert_equal c0, Fluent::Clock.now
      sleep 0.5
      assert_equal c0, Fluent::Clock.now
      sleep 0.6
      assert_equal c0, Fluent::Clock.now

      Fluent::Clock.return
      c1 = Fluent::Clock.now
      assert{ c1 >= c0 + 1.0 }
    end

    test 'Clock.now returns frozen time in the block argument of #freeze' do
      c0 = Fluent::Clock.now
      Fluent::Clock.freeze(c0) do
        assert_equal c0, Fluent::Clock.now
        sleep 0.5
        assert_equal c0, Fluent::Clock.now
        sleep 0.6
        assert_equal c0, Fluent::Clock.now
      end
      c1 = Fluent::Clock.now
      assert{ c1 >= c0 + 1.0 }
    end

    test 'Clock.now returns unfrozen value after jumping out from block by raising errors' do
      c0 = Fluent::Clock.now
      rescued_error = nil
      begin
        Fluent::Clock.freeze(c0) do
          assert_equal c0, Fluent::Clock.now
          sleep 0.5
          assert_equal c0, Fluent::Clock.now
          sleep 0.6
          assert_equal c0, Fluent::Clock.now
          raise "bye!"
        end
      rescue => e
        rescued_error = e
      end
      assert rescued_error # ensure to rescue an error
      c1 = Fluent::Clock.now
      assert{ c1 >= c0 + 1.0 }
    end

    test 'Clock.return cancels all Clock.freeze effects by just once' do
      c0 = Fluent::Clock.now
      sleep 0.1
      c1 = Fluent::Clock.now
      sleep 0.1
      c2 = Fluent::Clock.now
      Fluent::Clock.freeze(c0)
      sleep 0.1
      assert_equal c0, Fluent::Clock.now
      Fluent::Clock.freeze(c1)
      sleep 0.1
      assert_equal c1, Fluent::Clock.now
      Fluent::Clock.freeze(c2)
      sleep 0.1
      assert_equal c2, Fluent::Clock.now

      Fluent::Clock.return
      assert{ Fluent::Clock.now > c2 }
    end

    test 'Clock.freeze allows nested blocks by itself' do
      c0 = Fluent::Clock.now
      sleep 0.1
      c1 = Fluent::Clock.now
      sleep 0.1
      c2 = Fluent::Clock.now
      Fluent::Clock.freeze(c0) do
        sleep 0.1
        assert_equal c0, Fluent::Clock.now
        Fluent::Clock.freeze(c1) do
          sleep 0.1
          assert_equal c1, Fluent::Clock.now
          Fluent::Clock.freeze(c2) do
            sleep 0.1
            assert_equal c2, Fluent::Clock.now
          end
          assert_equal c1, Fluent::Clock.now
        end
        assert_equal c0, Fluent::Clock.now
      end
      assert{ Fluent::Clock.now > c0 }
    end
  end

  sub_test_case 'using #freeze with Time argument' do
    test 'Clock.freeze returns the clock value which should be produced when the time is at the specified time' do
      c0 = Fluent::Clock.now
      t0 = Time.now
      t1 = t0 - 30
      assert_kind_of Time, t1
      t2 = t0 + 30
      assert_kind_of Time, t2

      # 31 is for error of floating point value
      Fluent::Clock.freeze(t1) do
        c1 = Fluent::Clock.now
        assert{ c1 >= c0 - 31 && c1 <= c0 - 31 + 10 } # +10 is for threading schedule error
      end

      # 29 is for error of floating point value
      Fluent::Clock.freeze(t2) do
        c2 = Fluent::Clock.now
        assert{ c2 >= c0 + 29 && c2 <= c0 + 29 + 10 } # +10 is for threading schedule error
      end
    end
  end
end
