require_relative '../helper'
require 'fluent/plugin_helper/storage'
require 'fluent/plugin/base'

class ExampleStorage < Fluent::Plugin::Storage
  Fluent::Plugin.register_storage('example', self)

  attr_reader :data, :saved, :load_times, :save_times

  def initialize
    super
    @data = {}
    @saved = {}
    @load_times = 0
    @save_times = 0
  end
  def load
    @data ||= {}
    @load_times += 1
  end
  def save
    @saved = @data.dup
    @save_times += 1
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
    @load_times = @save_times = 0
    super
  end
end

class Example2Storage < ExampleStorage
  Fluent::Plugin.register_storage('ex2', self)
  config_param :dummy_path, :string, default: 'dummy'
end
class Example3Storage < ExampleStorage
  Fluent::Plugin.register_storage('ex3', self)
  def synchronized?
    true
  end
end
class Example4Storage < ExampleStorage
  Fluent::Plugin.register_storage('ex4', self)
  def persistent_always?
    true
  end
  def synchronized?
    true
  end
end

class StorageHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :storage
  end

  class Dummy2 < Fluent::Plugin::TestBase
    helpers :storage
    config_section :storage do
      config_set_default :@type, 'ex2'
      config_set_default :dummy_path, '/tmp/yay'
    end
  end

  setup do
    @d = nil
  end

  teardown do
    if @d
      @d.stop unless @d.stopped?
      @d.shutdown unless @d.shutdown?
      @d.close unless @d.closed?
      @d.terminate unless @d.terminated?
    end
  end

  test 'can be initialized without any storages at first' do
    d = Dummy.new
    assert_equal 0, d._storages.size
  end

  test 'can be configured with hash' do
    d = Dummy.new
    d.configure(config_element())
    conf = { '@type' => 'example' }
    assert_nothing_raised do
      d.storage_create(conf: conf)
    end
  end

  test 'can override default configuration parameters, but not overwrite whole definition' do
    d = Dummy.new
    d.configure(config_element())
    assert_equal 1, d.storage_configs.size
    assert_equal 'local', d.storage_configs.first[:@type]

    d = Dummy2.new
    d.configure(config_element('ROOT', '', {}, [config_element('storage', '', {}, [])]))
    assert_raise NoMethodError do
      d.storage
    end
    assert_equal 1, d.storage_configs.size
    assert_equal 'ex2', d.storage_configs.first[:@type]
    assert_equal '/tmp/yay', d.storage_configs.first.dummy_path
  end

  test 'creates instance of type specified by conf, or default_type if @type is missing in conf' do
    d = Dummy2.new
    d.configure(config_element())
    i = d.storage_create(conf: config_element('format', '', {'@type' => 'example'}), default_type: 'ex2')
    assert{ i.is_a?(Fluent::PluginHelper::Storage::SynchronizeWrapper) && i.instance_eval{ @storage }.is_a?(ExampleStorage) }

    d = Dummy2.new
    d.configure(config_element())
    i = d.storage_create(conf: nil, default_type: 'ex2')
    assert{ i.is_a?(Fluent::PluginHelper::Storage::SynchronizeWrapper) && i.instance_eval{ @storage }.is_a?(Example2Storage) }
  end

  test 'raises config error if config section is specified, but @type is not specified' do
    d = Dummy2.new
    d.configure(config_element())
    assert_raise Fluent::ConfigError.new("@type is required in <storage>") do
      d.storage_create(conf: config_element('storage', 'foo', {}), default_type: 'ex2')
    end
  end

  test 'raises config error if config argument has invalid characters' do
    d = Dummy.new
    assert_raise Fluent::ConfigError.new("Argument in <storage ARG> uses invalid characters: 'yaa y'") do
      d.configure(config_element('root', '', {}, [config_element('storage', 'yaa y', {'@type' => 'local'})]))
    end
    d.configure(config_element())
    assert_raise Fluent::ConfigError.new("Argument in <storage ARG> uses invalid characters: 'a,b'") do
      d.storage_create(usage: 'a,b', type: 'local')
    end
  end

  test 'can be configured without storage sections' do
    d = Dummy.new
    assert_nothing_raised do
      d.configure(config_element())
    end
    assert_equal 1, d._storages.size
  end

  test 'can be configured with a storage section' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', '', {'@type' => 'example'})
      ])
    assert_nothing_raised do
      d.configure(conf)
    end
    assert_equal 1, d._storages.size
    assert{ d._storages.values.all?{ |s| !s.running } }
  end

  test 'can be configured with 2 or more storage sections with different usages with each other' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'default', {'@type' => 'example'}),
        config_element('storage', 'extra', {'@type' => 'ex2', 'dummy_path' => 'v'}),
      ])
    assert_nothing_raised do
      d.configure(conf)
    end
    assert_equal 2, d._storages.size
    assert{ d._storages.values.all?{ |s| !s.running } }
  end

  test 'cannot be configured with 2 storage sections with same usage' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'default', {'@type' => 'example'}),
        config_element('storage', 'extra', {'@type' => 'ex2', 'dummy_path' => 'v'}),
        config_element('storage', 'extra', {'@type' => 'ex2', 'dummy_path' => 'v2'}),
      ])
    assert_raises Fluent::ConfigError do
      d.configure(conf)
    end
  end

  test 'creates a storage plugin instance which is already configured without usage' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', '', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create
    assert{ s.implementation.is_a? ExampleStorage }
  end

  test 'creates a storage plugin instance which is already configured with usage' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create(usage: 'mydata')
    assert{ s.implementation.is_a? ExampleStorage }
  end

  test 'creates a storage plugin without configurations' do
    @d = d = Dummy.new
    d.configure(config_element())
    d.start

    s = d.storage_create(usage: 'mydata', type: 'example', conf: config_element('storage', 'mydata'))
    assert{ s.implementation.is_a? ExampleStorage }
  end

  test 'creates 2 or more storage plugin instances' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example'}),
        config_element('storage', 'secret', {'@type' => 'ex2', 'dummy_path' => 'yay!'}),
      ])
    d.configure(conf)
    d.start

    s1 = d.storage_create(usage: 'mydata')
    s2 = d.storage_create(usage: 'secret')
    assert{ s1.implementation.is_a? ExampleStorage }
    assert{ s2.implementation.is_a? Example2Storage }
    assert_equal 'yay!', s2.implementation.dummy_path
  end

  test 'creates wrapped instances for non-synchronized plugin in default' do # and check operations
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create(usage: 'mydata')
    assert !s.implementation.synchronized?
    assert{ s.is_a? Fluent::PluginHelper::Storage::SynchronizeWrapper }
    assert s.synchronized?
    assert s.autosave
    assert s.save_at_shutdown

    assert_nil s.get('key')
    assert_equal 'value', s.put('key', 'value')
    assert_equal 'value', s.fetch('key', 'v1')
    assert_equal 'v2', s.update('key'){|v| v[0] + '2' }
    assert_equal 'v2', s.get('key')
    assert_equal 'v2', s.delete('key')
  end

  test 'creates wrapped instances for non-persistent plugins when configured as persistent' do # and check operations
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'persistent' => 'true'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create(usage: 'mydata')
    assert !s.implementation.persistent_always?
    assert{ s.is_a? Fluent::PluginHelper::Storage::PersistentWrapper }
    assert s.persistent
    assert s.persistent_always?
    assert s.synchronized?
    assert !s.autosave
    assert s.save_at_shutdown

    assert_nil s.get('key')
    assert_equal 'value', s.put('key', 'value')
    assert_equal 'value', s.fetch('key', 'v1')
    assert_equal 'v2', s.update('key'){|v| v[0] + '2' }
    assert_equal 'v2', s.get('key')
    assert_equal 'v2', s.delete('key')
  end

  test 'creates bare instances for synchronized plugin in default' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'ex3'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create(usage: 'mydata')
    assert s.implementation.synchronized?
    assert{ s.is_a? Example3Storage }
    assert s.synchronized?
  end

  test 'creates bare instances for persistent-always plugin when configured as persistent' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'ex4', 'persistent' => 'true'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create(usage: 'mydata')
    assert s.implementation.persistent_always?
    assert{ s.is_a? Example4Storage }
    assert s.persistent
    assert s.persistent_always?
    assert s.synchronized?
  end

  test 'does not execute timer if autosave is not specified' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'autosave' => 'false'})
      ])
    d.configure(conf)
    d.start

    d.storage_create(usage: 'mydata')
    assert_equal 0, d._timers.size
  end

  test 'executes timer if autosave is specified and plugin is not persistent' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'autosave_interval' => '1s', 'persistent' => 'false'})
      ])
    d.configure(conf)
    d.start

    d.storage_create(usage: 'mydata')
    assert_equal 1, d._timers.size
  end

  test 'executes timer for autosave, which calls #save periodically' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'autosave_interval' => '1s', 'persistent' => 'false'})
      ])
    d.configure(conf)
    d.start

    s = d.storage_create(usage: 'mydata')
    assert_equal 1, d._timers.size
    timeout = Time.now + 3
    while Time.now < timeout
      s.put('k', 'v')
      sleep 0.2
    end

    d.stop
    assert{ s.implementation.save_times > 0 }
  end

  test 'saves data for each operations if plugin storage is configured as persistent, and wrapped' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'persistent' => 'true'})
      ])
    d.configure(conf)
    d.start
    s = d.storage_create(usage: 'mydata')
    assert_equal 1, s.implementation.load_times
    assert_equal 0, s.implementation.save_times

    s.get('k1')
    assert_equal 2, s.implementation.load_times
    assert_equal 0, s.implementation.save_times

    s.put('k1', 'v1')
    assert_equal 3, s.implementation.load_times
    assert_equal 1, s.implementation.save_times

    s.fetch('k2', 'v2')
    assert_equal 4, s.implementation.load_times
    assert_equal 1, s.implementation.save_times

    s.delete('k1')
    assert_equal 5, s.implementation.load_times
    assert_equal 2, s.implementation.save_times
  end

  test 'stops timer for autosave by #stop, calls #save by #shutdown if save_at_shutdown is specified' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'save_at_shutdown' => 'true'})
      ])
    d.configure(conf)

    assert !d.timer_running?

    d.start

    assert d.timer_running?

    s = d.storage_create(usage: 'mydata')
    assert s.autosave
    assert_equal 1, d._timers.size

    d.stop

    assert !d.timer_running?

    assert_equal 1, s.implementation.load_times
    assert_equal 0, s.implementation.save_times

    d.before_shutdown

    d.shutdown

    assert_equal 1, s.implementation.load_times
    assert_equal 1, s.implementation.save_times
  end

  test 'calls #close and #terminate for all plugin instances by #close/#shutdown' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('storage', 'mydata', {'@type' => 'example', 'autosave' => 'false', 'save_at_shutdown' => 'true'})
      ])
    d.configure(conf)
    d.start
    s = d.storage_create(usage: 'mydata')

    s.put('k1', 'v1')
    s.put('k2', 2)
    s.put('k3', true)

    d.stop

    assert_equal 3, s.implementation.data.size
    assert_equal 0, s.implementation.saved.size

    d.shutdown

    assert_equal 3, s.implementation.data.size
    assert_equal 3, s.implementation.saved.size

    d.close

    assert_equal 0, s.implementation.data.size
    assert_equal 3, s.implementation.saved.size

    d.shutdown

    assert_equal 0, s.implementation.data.size
    assert_equal 0, s.implementation.saved.size
  end

  test 'calls lifecycle methods for all plugin instances via owner plugin' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [ config_element('storage', '', {'@type' => 'example'}), config_element('storage', 'e2', {'@type' => 'example'}) ])
    d.configure(conf)
    d.start

    i1 = d.storage_create(usage: '')
    i2 = d.storage_create(usage: 'e2')
    i3 = d.storage_create(usage: 'e3', type: 'ex2')

    assert i1.started?
    assert i2.started?
    assert i3.started?

    assert !i1.stopped?
    assert !i2.stopped?
    assert !i3.stopped?

    d.stop

    assert i1.stopped?
    assert i2.stopped?
    assert i3.stopped?

    assert !i1.before_shutdown?
    assert !i2.before_shutdown?
    assert !i3.before_shutdown?

    d.before_shutdown

    assert i1.before_shutdown?
    assert i2.before_shutdown?
    assert i3.before_shutdown?

    assert !i1.shutdown?
    assert !i2.shutdown?
    assert !i3.shutdown?

    d.shutdown

    assert i1.shutdown?
    assert i2.shutdown?
    assert i3.shutdown?

    assert !i1.after_shutdown?
    assert !i2.after_shutdown?
    assert !i3.after_shutdown?

    d.after_shutdown

    assert i1.after_shutdown?
    assert i2.after_shutdown?
    assert i3.after_shutdown?

    assert !i1.closed?
    assert !i2.closed?
    assert !i3.closed?

    d.close

    assert i1.closed?
    assert i2.closed?
    assert i3.closed?

    assert !i1.terminated?
    assert !i2.terminated?
    assert !i3.terminated?

    d.terminate

    assert i1.terminated?
    assert i2.terminated?
    assert i3.terminated?
  end
end
