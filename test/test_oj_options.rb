require_relative 'helper'
require 'fluent/test'
require 'fluent/oj_options'

class OjOptionsTest < ::Test::Unit::TestCase
  setup do
    @oj = Fluent::OjOptions.new
    @orig_env = {}
    ENV.each do |key, value|
      @orig_env[key] = value if key.start_with?("FLUENT_OJ_OPTION_")
    end
  end

  teardown do
    ENV.delete_if { |key| key.start_with?("FLUENT_OJ_OPTION_") }
    @orig_env.each { |key, value| ENV[key] = value }
  end

  sub_test_case "OjOptions" do
    test "when no env vars set, returns default options" do
      ENV.delete_if { |key| key.start_with?("FLUENT_OJ_OPTION_") }
      assert_equal Fluent::OjOptions::OJ_OPTIONS_DEFAULTS, @oj.get_options
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
