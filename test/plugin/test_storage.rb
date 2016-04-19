require_relative '../helper'
require 'fluent/plugin/storage'
require 'fluent/plugin/base'

class DummyPlugin < Fluent::Plugin::TestBase
end

class BareStorage < Fluent::Plugin::Storage
  Fluent::Plugin.register_storage('bare', self)
end

class BasicStorage < Fluent::Plugin::Storage
  Fluent::Plugin.register_storage('example', self)

  attr_reader :data, :saved

  def initialize
    super
    @data = @saved = nil
  end
  def load
    @data = {}
  end
  def save
    @saved = @data.dup
  end
  def get(key)
    @data[key]
  end
  def fetch(key, defval)
    @data.fetch(key, defval)
  end
  def put(key, value)
    @data[key] = value
  end
  def delete(key)
    @data.delete(key)
  end
  def update(key, &block)
    @data[key] = block.call(@data[key])
  end
  def close
    @data = {}
    super
  end
  def terminate
    @saved = {}
    super
  end
end

class StorageTest < Test::Unit::TestCase
  sub_test_case 'BareStorage' do
    setup do
      plugin = DummyPlugin.new
      @s = BareStorage.new
      @s.configure(config_element())
      @s.owner = plugin
    end

    test 'is configured with plugin information and system config' do
      plugin = DummyPlugin.new
      plugin.system_config_override({'process_name' => 'mytest'})
      plugin.configure(config_element('ROOT', '', {'@id' => '1'}))
      s = BareStorage.new
      s.configure(config_element())
      s.owner = plugin

      assert_equal 'mytest', s.owner.system_config.process_name
      assert_equal '1', s.instance_eval{ @_plugin_id }
      assert_equal true, s.instance_eval{ @_plugin_id_configured }
    end

    test 'does NOT have features for high-performance/high-consistent storages' do
      assert_equal false, @s.persistent_always?
      assert_equal false, @s.synchronized?
    end

    test 'does have default values which is conservative for almost all users' do
      assert_equal false, @s.persistent
      assert_equal true, @s.autosave
      assert_equal 10,   @s.autosave_interval
      assert_equal true, @s.save_at_shutdown
    end

    test 'load/save doesn NOT anything: just as memory storage' do
      assert_nothing_raised{ @s.load }
      assert_nothing_raised{ @s.save }
    end

    test 'all operations are not defined yet' do
      assert_raise NotImplementedError do
        @s.get('key')
      end
      assert_raise NotImplementedError do
        @s.fetch('key', 'value')
      end
      assert_raise NotImplementedError do
        @s.put('key', 'value')
      end
      assert_raise NotImplementedError do
        @s.delete('key')
      end
      assert_raise NotImplementedError do
        @s.update('key'){ |v| v + '2' }
      end
    end
  end

  sub_test_case 'ExampleStorage' do
    setup do
      plugin = DummyPlugin.new
      plugin.configure(config_element('ROOT', '', {'@id' => '1'}))
      @s = BasicStorage.new
      @s.configure(config_element())
      @s.owner = plugin
    end

    test 'load/save works well as plugin internal state operations' do
      plugin = DummyPlugin.new
      plugin.configure(config_element('ROOT', '', {'@id' => '0'}))
      s = BasicStorage.new
      s.owner = plugin

      assert_nothing_raised{ s.load }
      assert s.data
      assert_nil s.saved

      assert_nothing_raised{ s.save }
      assert s.saved
      assert{ s.data == s.saved }
      assert{ s.data.object_id != s.saved.object_id }
    end

    test 'all operations work well' do
      @s.load

      assert_nil @s.get('key')
      assert_equal 'value', @s.fetch('key', 'value')
      assert_nil @s.get('key')

      assert_equal 'value', @s.put('key', 'value')
      assert_equal 'value', @s.get('key')

      assert_equal 'valuevalue', @s.update('key'){|v| v * 2 }

      assert_equal 'valuevalue', @s.delete('key')
    end

    test 'close and terminate work to operate internal states' do
      @s.load
      @s.put('k1', 'v1')
      @s.put('k2', 'v2')
      assert_equal 2, @s.data.size
      @s.save
      assert_equal @s.data.size, @s.saved.size

      assert_nothing_raised{ @s.close }
      assert @s.data.empty?
      assert !@s.saved.empty?

      assert_nothing_raised{ @s.terminate }
      assert @s.data.empty?
      assert @s.saved.empty?
    end
  end
end
