require_relative '../helper'
require 'fluent/plugin/metrics'
require 'fluent/plugin/base'
require 'fluent/system_config'

class BareMetrics < Fluent::Plugin::Metrics
  Fluent::Plugin.register_metrics('bare', self)

  private

  # Just override for tests.
  def has_methods_for_counter?
    false
  end
end

class BasicCounterMetrics < Fluent::Plugin::Metrics
  Fluent::Plugin.register_metrics('example', self)

  attr_reader :data

  def initialize
    super
    @data = Hash.new(0)
  end
  def get(key)
    @data[key.to_s]
  end
  def inc(key)
    @data[key.to_s] +=1
  end
  def add(key, value)
    @data[key.to_s] += value
  end
  def set(key, value)
    @data[key.to_s] = value
  end
  def close
    @data = {}
    super
  end
end

class AliasedCounterMetrics < Fluent::Plugin::Metrics
  Fluent::Plugin.register_metrics('example', self)

  attr_reader :data

  def initialize
    super
    @data = Hash.new(0)
  end
  def configure(conf)
    super
    class << self
      alias_method :set, :set_counter
    end
  end
  def get(key)
    @data[key.to_s]
  end
  def inc(key)
    @data[key.to_s] +=1
  end
  def add(key, value)
    @data[key.to_s] += value
  end
  def set_counter(key, value)
    @data[key.to_s] = value
  end
  def close
    @data = {}
    super
  end
end

class BasicGaugeMetrics < Fluent::Plugin::Metrics
  Fluent::Plugin.register_metrics('example', self)

  attr_reader :data

  def initialize
    super
    @data = Hash.new(0)
  end
  def get(key)
    @data[key.to_s]
  end
  def inc(key)
    @data[key.to_s] +=1
  end
  def dec(key)
    @data[key.to_s] -=1
  end
  def add(key, value)
    @data[key.to_s] += value
  end
  def sub(key, value)
    @data[key.to_s] -= value
  end
  def set(key, value)
    @data[key.to_s] = value
  end
  def close
    @data = {}
    super
  end
end

class AliasedGaugeMetrics < Fluent::Plugin::Metrics
  Fluent::Plugin.register_metrics('example', self)

  attr_reader :data

  def initialize
    super
    @data = Hash.new(0)
  end
  def configure(conf)
    super
    class << self
      alias_method :dec, :dec_gauge
      alias_method :set, :set_gauge
      alias_method :sub, :sub_gauge
    end
  end
  def get(key)
    @data[key.to_s]
  end
  def inc(key)
    @data[key.to_s] +=1
  end
  def dec_gauge(key)
    @data[key.to_s] -=1
  end
  def add(key, value)
    @data[key.to_s] += value
  end
  def sub_gauge(key, value)
    @data[key.to_s] -= value
  end
  def set_gauge(key, value)
    @data[key.to_s] = value
  end
  def close
    @data = {}
    super
  end
end

class StorageTest < Test::Unit::TestCase
  sub_test_case 'BareMetrics' do
    setup do
      @m = BareMetrics.new
      @m.configure(config_element())
    end

    test 'is configured with plugin information and system config' do
      m = BareMetrics.new
      m.configure(config_element('metrics', '', {}))

      assert_false m.use_gauge_metric
      assert_false m.has_methods_for_counter
      assert_false m.has_methods_for_gauge
    end

    test 'all bare operations are not defined yet' do
      assert_raise NotImplementedError do
        @m.get('key')
      end
      assert_raise NotImplementedError do
        @m.inc('key')
      end
      assert_raise NotImplementedError do
        @m.dec('key')
      end
      assert_raise NotImplementedError do
        @m.add('key', 10)
      end
      assert_raise NotImplementedError do
        @m.sub('key', 11)
      end
      assert_raise NotImplementedError do
        @m.set('key', 123)
      end
    end
  end

  sub_test_case 'BasicCounterMetric' do
    setup do
      @m = BasicCounterMetrics.new
      @m.configure(config_element('metrics', '', {'@id' => '1'}))
    end

    test 'all basic counter operations work well' do
      assert_true @m.has_methods_for_counter
      assert_false @m.has_methods_for_gauge

      assert_equal 0, @m.get('key')
      assert_equal 1, @m.inc('key')

      @m.add('key', 20)
      assert_equal 21, @m.get('key')
      assert_raise NotImplementedError do
        @m.dec('key')
      end

      @m.set('key', 100)
      assert_equal 100, @m.get('key')
      assert_raise NotImplementedError do
        @m.sub('key', 11)
      end
    end
  end

  sub_test_case 'AliasedCounterMetric' do
    setup do
      @m = AliasedCounterMetrics.new
      @m.configure(config_element('metrics', '', {}))
    end

    test 'all aliased counter operations work well' do
      assert_true @m.has_methods_for_counter
      assert_false @m.has_methods_for_gauge

      assert_equal 0, @m.get('key')
      assert_equal 1, @m.inc('key')

      @m.add('key', 20)
      assert_equal 21, @m.get('key')
      assert_raise NotImplementedError do
        @m.dec('key')
      end

      @m.set('key', 100)
      assert_equal 100, @m.get('key')
      assert_raise NotImplementedError do
        @m.sub('key', 11)
      end
    end
  end

  sub_test_case 'BasicGaugeMetric' do
    setup do
      @m = BasicGaugeMetrics.new
      @m.use_gauge_metric = true
      @m.configure(config_element('metrics', '', {}))
    end

    test 'all basic gauge operations work well' do
      assert_false @m.has_methods_for_counter
      assert_true @m.has_methods_for_gauge

      assert_equal 0, @m.get('key')
      assert_equal 1, @m.inc('key')

      @m.add('key', 20)
      assert_equal 21, @m.get('key')
      @m.dec('key')
      assert_equal 20, @m.get('key')

      @m.set('key', 100)
      assert_equal 100, @m.get('key')
      @m.sub('key', 11)
      assert_equal 89, @m.get('key')
    end
  end

  sub_test_case 'AliasedGaugeMetric' do
    setup do
      @m = AliasedGaugeMetrics.new
      @m.use_gauge_metric = true
      @m.configure(config_element('metrics', '', {}))
    end

    test 'all aliased gauge operations work well' do
      assert_false @m.has_methods_for_counter
      assert_true @m.has_methods_for_gauge

      assert_equal 0, @m.get('key')
      assert_equal 1, @m.inc('key')

      @m.add('key', 20)
      assert_equal 21, @m.get('key')
      @m.dec('key')
      assert_equal 20, @m.get('key')

      @m.set('key', 100)
      assert_equal 100, @m.get('key')
      @m.sub('key', 11)
      assert_equal 89, @m.get('key')
    end
  end
end
