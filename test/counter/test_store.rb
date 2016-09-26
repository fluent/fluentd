require_relative '../helper'
require 'fluent/counter/store'
require 'timecop'

class CounterStoreValueTest < ::Test::Unit::TestCase
  setup do
    # timecop isn't compatible with EventTime
    t = Time.parse('2016-09-22 16:59:59 +0900')
    Timecop.freeze(t)
    @now = Fluent::EventTime.now
  end

  teardown do
    Timecop.return
  end

  sub_test_case 'init' do
    test 'default values' do
      v = Fluent::Counter::Store::Value.init(
        'name' => 'name',
        'reset_interval' => 10
      )
      assert_equal 'name', v.name
      assert_equal 10, v.reset_interval
      assert_equal 'numeric', v.type
      assert_equal @now, v.last_reset_at
      assert_equal @now, v.last_modified_at
      assert_equal 0, v.current
      assert_equal 0, v.total
    end

    data(
      integer: ['integer', 0, Integer],
      numeric: ['numeric', 0, Numeric],
      float: ['float', 0.0, Float]
    )
    test 'set a type' do |(type_name, value, type)|
      v = Fluent::Counter::Store::Value.init(
        'type' => type_name,
        'name' => 'name',
        'reset_interval' => 10
      )

      assert_true v.current.is_a?(type)
      assert_equal value, v.current
      assert_equal value, v.total
      assert_equal type_name, v.type
    end

    test 'raise an error when an invalid type has passed' do
      assert_raise do
        Fluent::Counter::Store::Value.init(
          'type' => 'invalid_type',
          'name' => 'name',
          'reset_interval' => 10
        )
      end
    end
  end

  sub_test_case 'initial_value' do
    data(
      integer: ['integer', Integer],
      numeric: ['numeric', Integer],
      float: ['float', Float],
    )
    test 'return a initial value' do |(type, type_class)|
      assert_true Fluent::Counter::Store::Value.initial_value(type).is_a?(type_class)
    end

    test 'raise an error when passed string is invalid' do
      assert_raise Fluent::Counter::InvalidParams do
        Fluent::Counter::Store::Value.initial_value('invalid value')
      end
    end
  end

  test 'to_response_hash' do
    v = Fluent::Counter::Store::Value.init(
      'name' => 'name',
      'reset_interval' => 10
    )
    expected = {
      'name' => 'name',
      'total' => 0,
      'current' => 0,
      'type' => 'numeric',
      'reset_interval' => 10,
      'last_reset_at' => @now,
    }
    assert_equal expected, v.to_response_hash
  end
end

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

  def extract_value_from_counter(counter, scope, name)
    store = counter.instance_variable_get(:@store)
    key = Fluent::Counter::Store.gen_key(scope, name)
    store[key]
  end

  sub_test_case 'init' do
    setup do
      @reset_interval = 10
      @store = Fluent::Counter::Store.new
      @data = { 'name' => @name, 'reset_interval' => @reset_interval }
    end

    test 'create new value in the counter' do
      v = @store.init(@name, @scope, @data)

      assert_equal @name, v.name
      assert_equal @reset_interval, v.reset_interval

      v2 = extract_value_from_counter(@store, @scope, @name)
      assert_equal v, v2
    end

    test 'raise an error when a passed key already exists' do
      @store.init(@name, @scope, @data)

      assert_raise Fluent::Counter::InvalidParams do
        @store.init(@name, @scope, @data)
      end
    end

    test 'return a value when passed key already exists and a ignore option is true' do
      v = @store.init(@name, @scope, @data)
      v1 = extract_value_from_counter(@store, @scope, @name)
      v2 = @store.init(@name, @scope, @data, ignore: true)
      assert_equal v, v2
      assert_equal v1, v2
    end
  end

  sub_test_case 'get' do
    setup do
      @store = Fluent::Counter::Store.new
      data = { 'name' => @name, 'reset_interval' => 10 }
      @store.init(@name, @scope, data)
    end

    test 'return a value from the counter' do
      v = extract_value_from_counter(@store, @scope, @name)
      assert_equal v, @store.get(@name, @scope)
    end

    test "return nil when a passed key doesn't exist" do
      assert_equal nil, @store.get('unknown_key', @scope)
      assert_equal nil, @store.get(@name, 'unknown_scope')
    end

    test "raise a error when when a passed key doesn't exist and raise_error option is true" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.get('unknown_key', @scope, raise_error: true)
      end

      assert_raise Fluent::Counter::UnknownKey do
        @store.get(@name, 'unknown_key', raise_error: true)
      end
    end
  end

  sub_test_case 'key?' do
    setup do
      @store = Fluent::Counter::Store.new
      data = { 'name' => @name, 'reset_interval' => 10 }
      @store.init(@name, @scope, data)
    end

    test 'return true when passed key exists' do
      assert_true @store.key?(@name, @scope)
    end

    test "return false when passed key doesn't exist" do
      assert_true !@store.key?('unknown_key', @scope)
      assert_true !@store.key?(@name, 'unknown_scope')
    end
  end

  sub_test_case 'delete' do
    setup do
      @store = Fluent::Counter::Store.new
      data = { 'name' => @name, 'reset_interval' => 10 }
      @init_value = @store.init(@name, @scope, data)
    end

    test 'delete a value from the counter' do
      v = @store.delete(@name, @scope)
      assert_equal @init_value, v
      assert_nil extract_value_from_counter(@store, @scope, @name)
    end

    test "raise an error when passed key doesn't exist" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.delete('unknown_key', @scope)
      end

      assert_raise Fluent::Counter::UnknownKey do
        @store.delete(@name, 'unknown_key')
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
      @store.init(@name, @scope, @init_data)
      Timecop.travel(@travel_sec)
      v = @store.inc(@name, @scope, { 'value' => value })

      assert_equal value, v.total
      assert_equal value, v.current
      assert_equal (@now + @travel_sec), v.last_modified_at
      assert_equal @now, v.last_reset_at # last_reset_at doesn't change

      v1 = extract_value_from_counter(@store, @scope, @name)
      assert_equal v, v1
    end

    test "raise an error when passed key doesn't exist" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.inc('unknown_key', @scope, { 'value' => 1 })
      end

      assert_raise Fluent::Counter::UnknownKey do
        @store.inc(@name, 'unknown_key', { 'value' => 1 })
      end
    end

    test 'raise an error when a type of passed value is incompatible with a stored value' do
      name2 = 'key_name2'
      name3 = 'key_name3'
      v = @store.init(@name, @scope, @init_data.merge('type' => 'integer'))
      v2 = @store.init(name2, @scope, @init_data.merge('type' => 'float'))
      v3 = @store.init(name3, @scope, @init_data.merge('type' => 'numeric'))
      assert_equal 'integer', v.type
      assert_equal 'float', v2.type
      assert_equal 'numeric', v3.type

      assert_raise Fluent::Counter::InvalidParams do
        @store.inc(@name, @scope, { 'value' => 1.1 })
      end

      assert_raise Fluent::Counter::InvalidParams do
        @store.inc(name2, @scope, { 'value' => 1 })
      end

      assert_nothing_raised do
        @store.inc(name3, @scope, { 'value' => 1 })
        @store.inc(name3, @scope, { 'value' => 1.0 })
      end
    end
  end

  sub_test_case 'reset' do
    setup do
      @store = Fluent::Counter::Store.new
      @travel_sec = 10

      @inc_value = 10
      @store.init(@name, @scope, { 'name' => @name, 'reset_interval' => 10 })
      @store.inc(@name, @scope, { 'value' => 10 })
    end

    test 'reset a value in the counter' do
      Timecop.travel(@travel_sec)

      v = @store.reset(@name, @scope)
      assert_equal @travel_sec, v['elapsed_time']
      assert_true v['success']
      counter = v['counter_data']

      assert_equal @name, counter['name']
      assert_equal @inc_value, counter['total']
      assert_equal @inc_value, counter['current']
      assert_equal 'numeric', counter['type']
      assert_equal @now, counter['last_reset_at']
      assert_equal 10, counter['reset_interval']

      v1 = extract_value_from_counter(@store, @scope, @name)
      assert_equal 0, v1.current
      assert_true v1.current.is_a?(Integer)
      assert_equal @inc_value, v1.total
      assert_equal (@now + @travel_sec), v1.last_reset_at
      assert_equal (@now + @travel_sec), v1.last_modified_at
    end

    test 'reset a value after `reset_interval` passed' do
      first_travel_sec = 5
      Timecop.travel(first_travel_sec) # jump time less than reset_interval
      v = @store.reset(@name, @scope)

      assert_equal false, v['success']
      assert_equal first_travel_sec, v['elapsed_time']
      store = extract_value_from_counter(@store, @scope, @name)
      assert_equal 10, store.current
      assert_equal @now, store.last_reset_at

      # time is passed greater than reset_interval
      Timecop.travel(@travel_sec)
      v = @store.reset(@name, @scope)
      assert_true v['success']
      assert_equal @travel_sec + first_travel_sec, v['elapsed_time']

      v1 = extract_value_from_counter(@store, @scope, @name)
      assert_equal 0, v1.current
      assert_equal (@now + @travel_sec + first_travel_sec), v1.last_reset_at
      assert_equal (@now + @travel_sec + first_travel_sec), v1.last_modified_at
    end

    test "raise an error when passed key doesn't exist" do
      assert_raise Fluent::Counter::UnknownKey do
        @store.reset('unknown_key', @scope)
      end

      assert_raise Fluent::Counter::UnknownKey do
        @store.reset(@name, 'unknown_key')
      end
    end
  end
end
