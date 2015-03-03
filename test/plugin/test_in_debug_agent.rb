require_relative '../helper'
require 'fileutils'
require 'fluent/plugin/in_debug_agent'

class DebugAgentInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/in_debug_agent")

  def create_driver(conf = '')
    Fluent::Test::InputTestDriver.new(Fluent::DebugAgentInput).configure(conf)
  end

  def test_unix_path_writable
    assert_nothing_raised do
      create_driver %[unix_path #{TMP_DIR}/test_path]
    end

    assert_raise(Fluent::ConfigError) do
      create_driver %[unix_path #{TMP_DIR}/does_not_exist/test_path]
    end
  end
end
