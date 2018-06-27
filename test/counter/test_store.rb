require_relative '../helper'
require 'fluent/counter/store'
require 'fluent/time'
require 'timecop'

class CounterStoreTest < ::Test::Unit::TestCase
  setup do
    @name = 'key_name'
    @scope = "server\tworker\tplugin"

    # timecop isn't compatible with EventTime
    t = Time.parse('2016-09-22 16:59:59 +0900')
    Timecop.freeze(t)
    @now = Fluent::EventTime.now
  end

  shutdown do
    Timecop.return
  end

  def extract_value_from_counter(counter, key)
    store = counter.instance_variable_get(:@storage).instance_variable_get(:@store)
    store[key]
  end

  def travel(sec)
    # Since Timecop.travel() causes test failures on Windows/AppVeyor by inducing
    # rounding errors to Time.now, we need to use Timecop.freeze() instead.
    Timecop.freeze(Time.now + sec)
  end

  sub_test_case 'init' do
    setup do
      @reset_interval = 10
      @store = Fluent::Counter::Store.new
      @data = { 'name' => @name, 'reset_interval' => @reset_interval }
      @key = Fluent::Counter::Store.gen_key(@scope, @name)
    end

    test 'create new value in the counter' do
      v = @store.init(@key, @data)

      assert_equal @name, v['name']
      assert_equal @reset_interval, v['reset_interval']

      v2 = extract_value_from_counter(@store, @key)
      v2 = @store.send(:build_response, v2)
      assert_equal v, v2
    end

    test 'raise an error when a passed key already exists' do
      @store.init(@key, @data)

      assert_raise Fluent::Counter::InvalidParams do
        @store.init(@key, @data)
      end
    end

    test 'return a value when passed key already exists and a ignore option is true' do
      v = @store.init(@key, @data)
      v1 = extract_value_from_counter(@store, @key)
      v1 = @store.send(:build_response, v1)
      v2 = @store.init(@key, @data, ignore: true)
      assert_equal v, v2
      assert_equal v1, v2
    end
  end

  sub_test_case 'get' do
    setup do
      @store = Fluent::Counter::Store.new
      data = { 'name' => @name, 'reset_interval' => 10 }
      @key = Fluent::Counter::Store.gen_key(@scope, @name)
      @store.init(@key, data)
    end

    test 'return a value from the counter' do
      v = extract_value_from_counter(@store, @key)
      expected = @store.send(:build_response, v)
      assert_equal expected, @store.get(@key)
    end

    test 'return a raw value from the counter when raw option is true' do
      v = extract_value_from_counter(@store, @key)
      assert_equal v, @store.get(@key, raw: true)
    end

    test "return nil when a passed key doesn't exist" do
      assert_equal nil, @store.get('unknown_key')
    end

    test "raise a error when when a passed key doesn't exist and raise_error option is true" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.get('unknown_key', raise_error: true)
      end
    end
  end

  sub_test_case 'key?' do
    setup do
      @store = Fluent::Counter::Store.new
      data = { 'name' => @name, 'reset_interval' => 10 }
      @key = Fluent::Counter::Store.gen_key(@scope, @name)
      @store.init(@key, data)
    end

    test 'return true when passed key exists' do
      assert_true @store.key?(@key)
    end

    test "return false when passed key doesn't exist" do
      assert_true !@store.key?('unknown_key')
    end
  end

  sub_test_case 'delete' do
    setup do
      @store = Fluent::Counter::Store.new
      data = { 'name' => @name, 'reset_interval' => 10 }
      @key = Fluent::Counter::Store.gen_key(@scope, @name)
      @init_value = @store.init(@key, data)
    end

    test 'delete a value from the counter' do
      v = @store.delete(@key)
      assert_equal @init_value, v
      assert_nil extract_value_from_counter(@store, @key)
    end

    test "raise an error when passed key doesn't exist" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.delete('unknown_key')
      end
    end
  end

  sub_test_case 'inc' do
    setup do
      @store = Fluent::Counter::Store.new
      @init_data = { 'name' => @name, 'reset_interval' => 10 }
      @travel_sec = 10
    end

    data(
      positive: 10,
      negative: -10
    )
    test 'increment or decrement a value in the counter' do |value|
      key = Fluent::Counter::Store.gen_key(@scope, @name)
      @store.init(key, @init_data)
      travel(@travel_sec)
      v = @store.inc(key, { 'value' => value })

      assert_equal value, v['total']
      assert_equal value, v['current']
      assert_equal @now, v['last_reset_at'] # last_reset_at doesn't change

      v1 = extract_value_from_counter(@store, key)
      v1 = @store.send(:build_response, v1)
      assert_equal v, v1
    end

    test "raise an error when passed key doesn't exist" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.inc('unknown_key', { 'value' => 1 })
      end
    end

    test 'raise an error when a type of passed value is incompatible with a stored value' do
      key1 = Fluent::Counter::Store.gen_key(@scope, @name)
      key2 = Fluent::Counter::Store.gen_key(@scope, 'name2')
      key3 = Fluent::Counter::Store.gen_key(@scope, 'name3')
      v1 = @store.init(key1, @init_data.merge('type' => 'integer'))
      v2 = @store.init(key2, @init_data.merge('type' => 'float'))
      v3 = @store.init(key3, @init_data.merge('type' => 'numeric'))
      assert_equal 'integer', v1['type']
      assert_equal 'float', v2['type']
      assert_equal 'numeric', v3['type']

      assert_raise Fluent::Counter::InvalidParams do
        @store.inc(key1, { 'value' => 1.1 })
      end

      assert_raise Fluent::Counter::InvalidParams do
        @store.inc(key2, { 'value' => 1 })
      end

      assert_nothing_raised do
        @store.inc(key3, { 'value' => 1 })
        @store.inc(key3, { 'value' => 1.0 })
      end
    end
  end

  sub_test_case 'reset' do
    setup do
      @store = Fluent::Counter::Store.new
      @travel_sec = 10

      @inc_value = 10
      @key = Fluent::Counter::Store.gen_key(@scope, @name)
      @store.init(@key, { 'name' => @name, 'reset_interval' => 10 })
      @store.inc(@key, { 'value' => 10 })
    end

    test 'reset a value in the counter' do
      travel(@travel_sec)

      v = @store.reset(@key)
      assert_equal @travel_sec, v['elapsed_time']
      assert_true v['success']
      counter = v['counter_data']

      assert_equal @name, counter['name']
      assert_equal @inc_value, counter['total']
      assert_equal @inc_value, counter['current']
      assert_equal 'numeric', counter['type']
      assert_equal @now, counter['last_reset_at']
      assert_equal 10, counter['reset_interval']

      v1 = extract_value_from_counter(@store, @key)
      assert_equal 0, v1['current']
      assert_true v1['current'].is_a?(Integer)
      assert_equal @inc_value, v1['total']
      assert_equal (@now + @travel_sec), Fluent::EventTime.new(*v1['last_reset_at'])
      assert_equal (@now + @travel_sec), Fluent::EventTime.new(*v1['last_modified_at'])
    end

    test 'reset a value after `reset_interval` passed' do
      first_travel_sec = 5
      travel(first_travel_sec) # jump time less than reset_interval
      v = @store.reset(@key)

      assert_equal false, v['success']
      assert_equal first_travel_sec, v['elapsed_time']
      store = extract_value_from_counter(@store, @key)
      assert_equal 10, store['current']
      assert_equal @now, Fluent::EventTime.new(*store['last_reset_at'])

      # time is passed greater than reset_interval
      travel(@travel_sec)
      v = @store.reset(@key)
      assert_true v['success']
      assert_equal @travel_sec + first_travel_sec, v['elapsed_time']

      v1 = extract_value_from_counter(@store, @key)
      assert_equal 0, v1['current']
      assert_equal (@now + @travel_sec + first_travel_sec), Fluent::EventTime.new(*v1['last_reset_at'])
      assert_equal (@now + @travel_sec + first_travel_sec), Fluent::EventTime.new(*v1['last_modified_at'])
    end

    test "raise an error when passed key doesn't exist" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.reset('unknown_key')
      end
    end
  end
end
