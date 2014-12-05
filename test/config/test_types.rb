require 'helper'
require 'fluent/config/types'

class TestConfigTypes < ::Test::Unit::TestCase
  include Fluent

  sub_test_case 'Config.size_value' do
    test 'normal case' do
      assert_equal(2048, Config.size_value("2k"))
      assert_equal(2048, Config.size_value("2K"))
      assert_equal(3145728, Config.size_value("3m"))
      assert_equal(3145728, Config.size_value("3M"))
      assert_equal(4294967296, Config.size_value("4g"))
      assert_equal(4294967296, Config.size_value("4G"))
      assert_equal(5497558138880, Config.size_value("5t"))
      assert_equal(5497558138880, Config.size_value("5T"))
      assert_equal(6, Config.size_value("6"))
    end

    test 'not assumed case' do
      assert_equal(6, Config.size_value(6))
      assert_equal(0, Config.size_value("hoge"))
      assert_equal(0, Config.size_value(""))
      assert_equal(0, Config.size_value(nil))
    end
  end

  sub_test_case 'Config.time_value' do
    test 'normal case' do
      assert_equal(10, Config.time_value("10s"))
      assert_equal(10, Config.time_value("10sec"))
      assert_equal(120, Config.time_value("2m"))
      assert_equal(10800, Config.time_value("3h"))
      assert_equal(345600, Config.time_value("4d"))
    end

    test 'not assumed case' do
      assert_equal(4.0, Config.time_value(4))
      assert_equal(0.4, Config.time_value(0.4))
      assert_equal(0.0, Config.time_value("hoge"))
      assert_equal(0.0, Config.time_value(""))
      assert_equal(0.0, Config.time_value(nil))
    end
  end

  sub_test_case 'Config.bool_value' do
    test 'normal case' do
      assert_true Config.bool_value("true")
      assert_true Config.bool_value("yes")
      assert_true Config.bool_value("")
      assert_false Config.bool_value("false")
      assert_false Config.bool_value("no")
    end

    test 'not assumed case' do
      assert_true Config.bool_value(true)
      assert_false Config.bool_value(false)
      assert_nil Config.bool_value("hoge")
      assert_nil Config.bool_value(nil)
      assert_nil Config.bool_value(10)
    end
  end
end
