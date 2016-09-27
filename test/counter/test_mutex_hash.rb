require_relative '../helper'
require 'fluent/counter/mutex_hash'
require 'timecop'

class MutexHashTest < ::Test::Unit::TestCase
  setup do
    @store = {}
    @value = 'sample value'
    @counter_store_mutex = Fluent::Counter::MutexHash.new(@store)
  end

  sub_test_case 'synchronize' do
    test "create new mutex values if keys don't exist" do
      keys = ['key', 'key1']

      @counter_store_mutex.synchronize(*keys) do |store, k|
        store[k] = @value
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      keys.each do |key|
        assert_true mhash[key].is_a?(Mutex)
        assert_equal @value, @store[key]
      end
    end

    test 'nothing to do when an empty array passed' do
      @counter_store_mutex.synchronize(*[]) do |store, k|
        store[k] = @value
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      assert_true mhash.empty?
      assert_true @store.empty?
    end

    test 'use a one mutex value when the same key specified' do
      key = 'key'
      @counter_store_mutex.synchronize(key) do |store, k|
        store[k] = @value
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      m = mhash[key]
      assert_true m.is_a?(Mutex)
      assert_equal @value, @store[key]

      # access the same key once again
      value2 = 'test value2'
      @counter_store_mutex.synchronize(key) do |store, k|
        store[k] = value2
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      m2 = mhash[key]
      assert_true m2.is_a?(Mutex)
      assert_equal value2, @store[key]

      assert_equal m, m2
    end
  end

  sub_test_case 'synchronize_key' do
    test "create new mutex values if keys don't exist" do
      keys = ['key', 'key1']

      @counter_store_mutex.synchronize_keys(*keys) do |store, k|
        store[k] = @value
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      keys.each do |key|
        assert_true mhash[key].is_a?(Mutex)
        assert_equal @value, @store[key]
      end
    end

    test 'nothing to do when an empty array passed' do
      @counter_store_mutex.synchronize_keys(*[]) do |store, k|
        store[k] = @value
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      assert_true mhash.empty?
      assert_true @store.empty?
    end

    test 'use a one mutex value when the same key specified' do
      key = 'key'
      @counter_store_mutex.synchronize_keys(key) do |store, k|
        store[k] = @value
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      m = mhash[key]
      assert_true m.is_a?(Mutex)
      assert_equal @value, @store[key]

      # access the same key once again
      value2 = 'test value2'
      @counter_store_mutex.synchronize_keys(key) do |store, k|
        store[k] = value2
      end

      mhash = @counter_store_mutex.instance_variable_get(:@mutex_hash)
      m2 = mhash[key]
      assert_true m2.is_a?(Mutex)
      assert_equal value2, @store[key]

      assert_equal m, m2
    end
  end
end
