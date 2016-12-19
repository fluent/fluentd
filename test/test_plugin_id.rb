require_relative 'helper'
require 'fluent/plugin/base'
require 'fluent/system_config'
require 'fileutils'

class PluginIdTest < Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/plugin_id/#{ENV['TEST_ENV_NUMBER']}")

  class MyPlugin < Fluent::Plugin::Base
    include Fluent::PluginId
  end

  setup do
    @p = MyPlugin.new
  end

  sub_test_case '#plugin_id_for_test?' do
    test 'returns true always in test files' do
      assert @p.plugin_id_for_test?
    end

    test 'returns false always out of test files' do
      # TODO: no good way to write this test....
    end
  end

  sub_test_case 'configured without @id' do
    setup do
      @p.configure(config_element())
    end

    test '#plugin_id_configured? returns false' do
      assert_false @p.plugin_id_configured?
    end

    test '#plugin_id returns object_id based string' do
      assert_kind_of String, @p.plugin_id
      assert @p.plugin_id =~ /^object:[0-9a-f]+/
    end

    test '#plugin_root_dir returns nil' do
      assert_nil @p.plugin_root_dir
    end
  end

  sub_test_case 'configured with @id' do
    setup do
      FileUtils.rm_rf(TMP_DIR)
      FileUtils.mkdir_p(TMP_DIR)
      @p.configure(config_element('ROOT', '', {'@id' => 'testing_plugin_id'}))
    end

    test '#plugin_id_configured? returns true' do
      assert @p.plugin_id_configured?
    end

    test '#plugin_id returns the configured value' do
      assert_equal 'testing_plugin_id', @p.plugin_id
    end

    test '#plugin_root_dir returns nil without system root directory configuration' do
      assert_nil @p.plugin_root_dir
    end

    test '#plugin_root_dir returns an existing directory path frozen String' do
      root_dir = Fluent::SystemConfig.overwrite_system_config('root_dir' => File.join(TMP_DIR, "myroot")) do
        @p.plugin_root_dir
      end
      assert_kind_of String, root_dir
      assert Dir.exist?(root_dir)
      assert root_dir =~ %r!/worker0/!
      assert root_dir.frozen?
    end

    test '#plugin_root_dir returns the same value for 2nd or more call' do
      root_dir = Fluent::SystemConfig.overwrite_system_config('root_dir' => File.join(TMP_DIR, "myroot")) do
        @p.plugin_root_dir
      end
      twice = Fluent::SystemConfig.overwrite_system_config('root_dir' => File.join(TMP_DIR, "myroot")) do
        @p.plugin_root_dir
      end
      assert_equal root_dir.object_id, twice.object_id
    end

    test '#plugin_root_dir referres SERVERENGINE_WORKER_ID environment path to create it' do
      prev_env_val = ENV['SERVERENGINE_WORKER_ID']
      begin
        ENV['SERVERENGINE_WORKER_ID'] = '7'
        root_dir = Fluent::SystemConfig.overwrite_system_config('root_dir' => File.join(TMP_DIR, "myroot")) do
          @p.plugin_root_dir
        end
        assert_kind_of String, root_dir
        assert Dir.exist?(root_dir)
        assert root_dir =~ %r!/worker7/!
        assert root_dir.frozen?
      ensure
        ENV['SERVERENGINE_WORKER_ID'] = prev_env_val
      end
    end
  end
end
