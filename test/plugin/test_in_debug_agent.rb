require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_debug_agent'
require 'fileutils'

class DebugAgentInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/in_debug_agent")

  def create_driver(conf = '')
    Fluent::Test::Driver::Input.new(Fluent::Plugin::DebugAgentInput).configure(conf)
  end

  def test_unix_path_writable
    assert_nothing_raised do
      create_driver %[unix_path #{TMP_DIR}/test_path]
    end

    assert_raise(Fluent::ConfigError) do
      create_driver %[unix_path #{TMP_DIR}/does_not_exist/test_path]
    end
  end

  def test_multi_worker_environment_with_port
    Fluent::SystemConfig.overwrite_system_config('workers' => 4) do
      d = Fluent::Test::Driver::Input.new(Fluent::Plugin::DebugAgentInput)
      d.instance.instance_eval { @_fluentd_worker_id = 2 }
      d.configure('port 24230')

      assert_true d.instance.multi_workers_ready?
      assert_equal(24232, d.instance.instance_variable_get(:@port))
    end
  end

  def test_multi_worker_environment_with_unix_path
    Fluent::SystemConfig.overwrite_system_config('workers' => 4) do
      d = Fluent::Test::Driver::Input.new(Fluent::Plugin::DebugAgentInput)
      d.instance.instance_eval { @_fluentd_worker_id = 2 }
      d.configure("unix_path #{TMP_DIR}/test_path")

      assert_false d.instance.multi_workers_ready?
    end
  end
end
