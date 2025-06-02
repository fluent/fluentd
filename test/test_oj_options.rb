require_relative 'helper'
require 'fluent/test'
require 'fluent/oj_options'

class OjOptionsTest < ::Test::Unit::TestCase
  begin
    require 'oj'
    @@oj_is_available = true
  rescue LoadError
    @@oj_is_available = false
  end

  setup do
    @orig_env = {}
    ENV.each do |key, value|
      @orig_env[key] = value if key.start_with?("FLUENT_OJ_OPTION_")
    end
  end

  teardown do
    ENV.delete_if { |key| key.start_with?("FLUENT_OJ_OPTION_") }
    @orig_env.each { |key, value| ENV[key] = value }
  end

  test "available?" do
    assert_equal(@@oj_is_available, Fluent::OjOptions.available?)
  end

  sub_test_case "set by environment variable" do
    test "when no env vars set, returns default options" do
      ENV.delete_if { |key| key.start_with?("FLUENT_OJ_OPTION_") }
      defaults = Fluent::OjOptions::DEFAULTS
      assert_equal(defaults, Fluent::OjOptions.load_env)
      assert_equal(defaults, Oj.default_options.slice(*defaults.keys)) if @@oj_is_available
    end

    test "valid env var passed with valid value, default is overridden" do
      ENV["FLUENT_OJ_OPTION_BIGDECIMAL_LOAD"] = ":bigdecimal"
      assert_equal(:bigdecimal, Fluent::OjOptions.load_env[:bigdecimal_load])
      assert_equal(:bigdecimal, Oj.default_options[:bigdecimal_load]) if @@oj_is_available
    end

    test "valid env var passed with invalid value, default is not overridden" do
      ENV["FLUENT_OJ_OPTION_BIGDECIMAL_LOAD"] = ":conor"
      assert_equal(:float, Fluent::OjOptions.load_env[:bigdecimal_load])
      assert_equal(:float, Oj.default_options[:bigdecimal_load]) if @@oj_is_available
    end

    test "invalid env var passed, nothing done with it" do
      ENV["FLUENT_OJ_OPTION_CONOR"] = ":conor"
      assert_equal(nil, Fluent::OjOptions.load_env[:conor])
      assert_equal(nil, Oj.default_options[:conor]) if @@oj_is_available
    end
  end
end
