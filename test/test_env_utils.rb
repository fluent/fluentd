require_relative 'helper'
require 'fluent/test'
require 'fluent/env_utils'

class EnvUtilsTest < ::Test::Unit::TestCase
  setup do
    @oj = Fluent::EnvUtils::OjOptions.new
  end

  sub_test_case "module methods" do
    test "convert_env_var_to_type" do
      assert_equal :conor, Fluent::EnvUtils.convert_env_var_to_type("conor", :symbol)
      assert_equal 8933, Fluent::EnvUtils.convert_env_var_to_type("8933", :integer)
      assert_equal nil, Fluent::EnvUtils.convert_env_var_to_type("conor", :integer)
      assert_equal true, Fluent::EnvUtils.convert_env_var_to_type("true", :bool)
      assert_equal nil, Fluent::EnvUtils.convert_env_var_to_type("conor", :bool)
    end
  end

  sub_test_case "OjOptions" do
    test "when no env vars set, returns default options" do
      ENV.clear
      assert_equal Fluent::EnvUtils::OjOptions::OJ_OPTIONS_DEFAULTS, @oj.get_options
    end

    test "valid env var passed with valid value, default is overridden" do
      ENV["FLUENT_OJ_OPTION_BIGDECIMAL_LOAD"] = ":bigdecimal"
      assert_equal :bigdecimal, @oj.get_options[:bigdecimal_load]
    end

    test "valid env var passed with invalid value, default is not overriden" do
      ENV["FLUENT_OJ_OPTION_BIGDECIMAL_LOAD"] = ":conor"
      assert_equal :float, @oj.get_options[:bigdecimal_load]
    end

    test "invalid env var passed, nothing done with it" do
      ENV["FLUENT_OJ_OPTION_CONOR"] = ":conor"
      assert_equal nil, @oj.get_options[:conor]
    end
  end
end
