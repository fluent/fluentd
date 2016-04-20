require_relative '../helper'
require 'fluent/plugin/string_util'

class StringUtilTest < Test::Unit::TestCase
  def setup
    @null_value_pattern = Regexp.new("^(-|null|NULL)$")
  end

  sub_test_case 'valid string' do
    test 'null string' do
      assert_equal Fluent::StringUtil.match_regexp(@null_value_pattern, "null").to_s, "null"
      assert_equal Fluent::StringUtil.match_regexp(@null_value_pattern, "NULL").to_s, "NULL"
      assert_equal Fluent::StringUtil.match_regexp(@null_value_pattern, "-").to_s, "-"
    end

    test 'normal string' do
      assert_equal Fluent::StringUtil.match_regexp(@null_value_pattern, "fluentd"), nil
    end
  end

  sub_test_case 'invalid string' do
    test 'normal string' do
      assert_equal Fluent::StringUtil.match_regexp(@null_value_pattern, "\xff"), nil
    end
  end
end
