require_relative '../helper'
require 'fluent/plugin/storage_local'
require 'fluent/plugin/input'
require 'fluent/system_config'
require 'fileutils'

class LocalStorageTest < Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/storage_local#{ENV['TEST_ENV_NUMBER']}")

  class MyInput < Fluent::Plugin::Input
    helpers :storage
    config_section :storage do
      config_set_default :@type, 'local'
    end
  end

  setup do
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
    Fluent::Test.setup
    @d = MyInput.new
  end

  teardown do
    @d.stop unless @d.stopped?
    @d.before_shutdown unless @d.before_shutdown?
    @d.shutdown unless @d.shutdown?
    @d.after_shutdown unless @d.after_shutdown?
    @d.close unless @d.closed?
    @d.terminate unless @d.terminated?
  end

  sub_test_case 'without any configuration' do
    test 'works as on-memory storage' do
      conf = config_element()

      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_nil @p.path
      assert @p.store.empty?

      assert_nil @p.get('key1')
      assert_equal 'EMPTY', @p.fetch('key1', 'EMPTY')

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.update('key1') do |v|
        (v.to_i * 2).to_s
      end
      assert_equal '2', @p.get('key1')

      @p.save # on-memory storage does nothing...

      @d.stop; @d.before_shutdown; @d.shutdown; @d.after_shutdown; @d.close; @d.terminate

      # re-create to reload storage contents
      @d = MyInput.new
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert @p.store.empty?
    end

    test 'warns about on-memory storage if autosave is true' do
      @d.configure(config_element())
      @d.start
      @p = @d.storage_create()

      logs = @d.log.out.logs
      assert{ logs.any?{|log| log.include?("[warn]: both of Plugin @id and path for <storage> are not specified. Using on-memory store.") } }
    end

    test 'show info log about on-memory storage if autosave is false' do
      @d.configure(config_element('ROOT', '', {}, [config_element('storage', '', {'autosave' => 'false'})]))
      @d.start
      @p = @d.storage_create()

      logs = @d.log.out.logs
      assert{ logs.any?{|log| log.include?("[info]: both of Plugin @id and path for <storage> are not specified. Using on-memory store.") } }
    end
  end

  sub_test_case 'configured with file path' do
    test 'works as storage which stores data on disk' do
      storage_path = File.join(TMP_DIR, 'my_store.json')
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {'path' => storage_path})])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_equal storage_path, @p.path
      assert @p.store.empty?

      assert_nil @p.get('key1')
      assert_equal 'EMPTY', @p.fetch('key1', 'EMPTY')

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.update('key1') do |v|
        (v.to_i * 2).to_s
      end
      assert_equal '2', @p.get('key1')

      @p.save # stores all data into file

      assert File.exist?(storage_path)

      @p.put('key2', 4)

      @d.stop; @d.before_shutdown; @d.shutdown; @d.after_shutdown; @d.close; @d.terminate

      assert_equal({'key1' => '2', 'key2' => 4}, File.open(storage_path){|f| JSON.parse(f.read) })

      # re-create to reload storage contents
      @d = MyInput.new
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_false @p.store.empty?

      assert_equal '2', @p.get('key1')
      assert_equal 4, @p.get('key2')
    end

    test 'raise configuration error if a file specified with multi worker configuration' do
      storage_path = File.join(TMP_DIR, 'my_store.json')
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {'path' => storage_path})])
      @d.system_config_override('workers' => 3)
      assert_raise Fluent::ConfigError.new("Plugin 'local' does not support multi workers configuration (Fluent::Plugin::LocalStorage)") do
        @d.configure(conf)
      end
    end
  end

  sub_test_case 'configured with root-dir and plugin id' do
    test 'works as storage which stores data under root dir' do
      root_dir = File.join(TMP_DIR, 'root')
      expected_storage_path = File.join(root_dir, 'worker0', 'local_storage_test', 'storage.json')
      conf = config_element('ROOT', '', {'@id' => 'local_storage_test'})
      Fluent::SystemConfig.overwrite_system_config('root_dir' => root_dir) do
        @d.configure(conf)
      end
      @d.start
      @p = @d.storage_create()

      assert_equal expected_storage_path, @p.path
      assert @p.store.empty?

      assert_nil @p.get('key1')
      assert_equal 'EMPTY', @p.fetch('key1', 'EMPTY')

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.update('key1') do |v|
        (v.to_i * 2).to_s
      end
      assert_equal '2', @p.get('key1')

      @p.save # stores all data into file

      assert File.exist?(expected_storage_path)

      @p.put('key2', 4)

      @d.stop; @d.before_shutdown; @d.shutdown; @d.after_shutdown; @d.close; @d.terminate

      assert_equal({'key1' => '2', 'key2' => 4}, File.open(expected_storage_path){|f| JSON.parse(f.read) })

      # re-create to reload storage contents
      @d = MyInput.new
      Fluent::SystemConfig.overwrite_system_config('root_dir' => root_dir) do
        @d.configure(conf)
      end
      @d.start
      @p = @d.storage_create()

      assert_false @p.store.empty?

      assert_equal '2', @p.get('key1')
      assert_equal 4, @p.get('key2')
    end

    test 'works with customized path by specified usage' do
      root_dir = File.join(TMP_DIR, 'root')
      expected_storage_path = File.join(root_dir, 'worker0', 'local_storage_test', 'storage.usage.json')
      conf = config_element('ROOT', 'usage', {'@id' => 'local_storage_test'})
      Fluent::SystemConfig.overwrite_system_config('root_dir' => root_dir) do
        @d.configure(conf)
      end
      @d.start
      @p = @d.storage_create(usage: 'usage', type: 'local')

      assert_equal expected_storage_path, @p.path
      assert @p.store.empty?
    end
  end

  sub_test_case 'configured with root-dir and plugin id, and multi workers' do
    test 'works as storage which stores data under root dir, also in workers' do
      root_dir = File.join(TMP_DIR, 'root')
      expected_storage_path = File.join(root_dir, 'worker1', 'local_storage_test', 'storage.json')
      conf = config_element('ROOT', '', {'@id' => 'local_storage_test'})
      with_worker_config(root_dir: root_dir, workers: 2, worker_id: 1) do
        @d.configure(conf)
      end
      @d.start
      @p = @d.storage_create()

      assert_equal expected_storage_path, @p.path
      assert @p.store.empty?
    end
  end

  sub_test_case 'persistent specified' do
    test 'works well with path' do
      omit "It's hard to test on Windows" if Fluent.windows?

      storage_path = File.join(TMP_DIR, 'my_store.json')
      conf = config_element('ROOT', '', {}, [config_element('storage', '', {'path' => storage_path, 'persistent' => 'true'})])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_equal storage_path, @p.path
      assert @p.store.empty?

      assert_nil @p.get('key1')
      assert_equal 'EMPTY', @p.fetch('key1', 'EMPTY')

      @p.put('key1', '1')
      assert_equal({'key1' => '1'}, File.open(storage_path){|f| JSON.parse(f.read) })

      @p.update('key1') do |v|
        (v.to_i * 2).to_s
      end
      assert_equal({'key1' => '2'}, File.open(storage_path){|f| JSON.parse(f.read) })
    end

    test 'works well with root-dir and plugin id' do
      omit "It's hard to test on Windows" if Fluent.windows?

      root_dir = File.join(TMP_DIR, 'root')
      expected_storage_path = File.join(root_dir, 'worker0', 'local_storage_test', 'storage.json')
      conf = config_element('ROOT', '', {'@id' => 'local_storage_test'}, [config_element('storage', '', {'persistent' => 'true'})])
      Fluent::SystemConfig.overwrite_system_config('root_dir' => root_dir) do
        @d.configure(conf)
      end
      @d.start
      @p = @d.storage_create()

      assert_equal expected_storage_path, @p.path
      assert @p.store.empty?

      assert_nil @p.get('key1')
      assert_equal 'EMPTY', @p.fetch('key1', 'EMPTY')

      @p.put('key1', '1')
      assert_equal({'key1' => '1'}, File.open(expected_storage_path){|f| JSON.parse(f.read) })

      @p.update('key1') do |v|
        (v.to_i * 2).to_s
      end
      assert_equal({'key1' => '2'}, File.open(expected_storage_path){|f| JSON.parse(f.read) })
    end

    test 'raises error if it is configured to use on-memory storage' do
      assert_raise Fluent::ConfigError.new("Plugin @id or path for <storage> required when 'persistent' is true") do
        @d.configure(config_element('ROOT', '', {}, [config_element('storage', '', {'persistent' => 'true'})]))
      end
    end
  end

  sub_test_case 'with various configurations' do
    test 'mode and dir_mode controls permissions of stored data' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      storage_path = File.join(TMP_DIR, 'dir', 'my_store.json')
      storage_conf = {
        'path' => storage_path,
        'mode' => '0600',
        'dir_mode' => '0777',
      }
      conf = config_element('ROOT', '', {}, [config_element('storage', '', storage_conf)])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_equal storage_path, @p.path
      assert @p.store.empty?

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.save # stores all data into file

      assert File.exist?(storage_path)
      assert_equal 0600, (File.stat(storage_path).mode % 01000)
      assert_equal 0777, (File.stat(File.dirname(storage_path)).mode % 01000)
    end

    test 'pretty_print controls to write data in files as human-easy-to-read' do
      storage_path = File.join(TMP_DIR, 'dir', 'my_store.json')
      storage_conf = {
        'path' => storage_path,
        'pretty_print' => 'true',
      }
      conf = config_element('ROOT', '', {}, [config_element('storage', '', storage_conf)])
      @d.configure(conf)
      @d.start
      @p = @d.storage_create()

      assert_equal storage_path, @p.path
      assert @p.store.empty?

      @p.put('key1', '1')
      assert_equal '1', @p.get('key1')

      @p.save # stores all data into file

      expected_pp_json = <<JSON.chomp
{
  "key1": "1"
}
JSON
      assert_equal expected_pp_json, File.read(storage_path)
    end
  end
end
