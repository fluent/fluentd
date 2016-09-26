require_relative '../helper'
require 'fluent/counter/client'
require 'fluent/counter/server'
require 'timecop'

class CounterClientTest < ::Test::Unit::TestCase
  TEST_HOST = '127.0.0.1'
  TEST_PORT = '8277'

  setup do
    # timecop isn't compatible with EventTime
    t = Time.parse('2016-09-22 16:59:59 +0900')
    Timecop.freeze(t)
    @now = Fluent::EventTime.now

    @options = {
      host: TEST_HOST,
      port: TEST_PORT,
    }

    @server_name = 'server1'
    @scope = "worker1\tplugin1"
    @loop = Coolio::Loop.new
    @server = Fluent::Counter::Server.new(@server_name, @options).start
    @client = Fluent::Counter::Client.new(@loop, @options).start
  end

  teardown do
    Timecop.return
    @server.stop
    @client.stop
  end

  test 'Callable API' do
    [:establish, :init, :delete, :inc, :reset, :get].each do |m|
      assert_true @client.respond_to?(m)
    end
  end

  def extract_value_from_server(server, scope, name)
    counter = server.instance_variable_get(:@counter)
    store = counter.instance_variable_get(:@store).instance_variable_get(:@store)
    key = Fluent::Counter::Store.gen_key(scope, name)
    store[key]
  end

  sub_test_case 'establish' do
    test 'establish a scope' do
      @client.establish(@scope)
      assert_equal "#{@server_name}\t#{@scope}", @client.instance_variable_get(:@scope)
    end

    data(
      empty: '',
      invalid_string: '_scope',
      invalid_string2: 'Scope'
    )
    test 'raise an error when passed scope is invalid' do |scope|
      assert_raise do
        @client.establish(scope)
      end
    end
  end

  sub_test_case 'init' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
    end

    data(
      numeric_type: [
        { 'name' => 'key', 'reset_interval' => 20, 'type' => 'numeric' }, 0
      ],
      float_type: [
        { 'name' => 'key', 'reset_interval' => 20, 'type' => 'float' }, 0.0
      ],
      integer_type: [
        { 'name' => 'key', 'reset_interval' => 20, 'type' => 'integer' }, 0
      ]
    )
    test 'create a value' do |(param, initial_value)|
      assert_nil extract_value_from_server(@server, @scope, 'key')

      response = @client.init(param)
      data = response['data'].first

      assert_nil response['errors']
      assert_equal 'key', data['name']
      assert_equal 20, data['reset_interval']
      assert_equal param['type'], data['type']
      assert_equal initial_value, data['current']
      assert_equal initial_value, data['total']

      v = extract_value_from_server(@server, @scope, 'key')
      assert_equal 'key', v.name
      assert_equal 20, v.reset_interval
      assert_equal param['type'], v.type
      assert_equal initial_value, v.total
      assert_equal initial_value, v.current
    end

    data(
      already_exist_key: [
        { 'name' => 'key1', 'reset_interval' => 10 },
        { 'code' => 'invalid_params', 'message' => 'key1 already exists in counter' }
      ],
      missing_name: [
        { 'reset_interval' => 10 },
        { 'code' => 'invalid_params', 'message' => '`name` is required' },
      ],
      missing_reset_interval: [
        { 'name' => 'key' },
        { 'code' => 'invalid_params', 'message' => '`reset_interval` is required' },
      ],
      invalid_key: [
        { 'name' => '\tkey' },
        { 'code' => 'invalid_params', 'message' => '`name` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      @client.init({ 'name' => 'key1', 'reset_interval' => 10 })
      resopnse = @client.init(param)

      errors = resopnse['errors'].first

      assert_empty resopnse['data']
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      param = { 'name' => 'key1', 'reset_interval' => 10 }
      param2 = { 'name' => 'key2', 'reset_interval' => 10 }
      @client.init(param)

      response = @client.init(param2, param)
      data = response['data'].first
      error = response['errors'].first

      assert_equal param2['name'], data['name']
      assert_equal param2['reset_interval'], data['reset_interval']

      assert_equal 'invalid_params', error['code']
      assert_equal "#{param['name']} already exists in counter", error['message']
    end

    test 'return a future object when asyn option is true' do
      param = { 'name' => 'key', 'reset_interval' => 10 }
      r = @client.init(param, options: { async: true })
      assert_true r.is_a?(Fluent::Counter::Future)
    end
  end

  sub_test_case 'delete' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'

      @init_obj = { 'name' => @name, 'reset_interval' => 20, 'type' => 'numeric' }
      @client.init(@init_obj)
    end

    test 'delete a value' do
      assert extract_value_from_server(@server, @scope, @name)

      response = @client.delete(@name)
      v = response['data'].first

      assert_nil response['errors']
      assert_equal @name, v['name']
      assert_equal 20, v['reset_interval']
      assert_equal 'numeric', v['type']

      assert_nil extract_value_from_server(@server, @scope, @name)
    end

    data(
      key_not_found: [
        'key2',
        { 'code' => 'unknown_key', 'message' => "`key2` doesn't exist in counter" }
      ],
      invalid_key: [
        '\tkey',
        { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      resopnse = @client.delete(param)

      errors = resopnse['errors'].first

      assert_empty resopnse['data']
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      unknow_name = 'key2'

      response = @client.delete(@name, unknow_name)
      data = response['data'].first
      error = response['errors'].first

      assert_equal @name, data['name']
      assert_equal @init_obj['reset_interval'], data['reset_interval']

      assert_equal 'unknown_key', error['code']
      assert_equal "`key2` doesn't exist in counter", error['message']

      assert_nil extract_value_from_server(@server, @scope, @name)
    end

    test 'return a future object when asyn option is true' do
      r = @client.delete(@name, options: { async: true })
      assert_true r.is_a?(Fluent::Counter::Future)
    end
  end

  sub_test_case 'inc' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'

      @init_obj = { 'name' => @name, 'reset_interval' => 20, 'type' => 'numeric' }
      @client.init(@init_obj)
    end

    test 'increment a value' do
      v = extract_value_from_server(@server, @scope, @name)
      assert_equal 0, v.total
      assert_equal 0, v.current

      @client.inc({ 'name' => @name, 'value' => 10 })

      v = extract_value_from_server(@server, @scope, @name)
      assert_equal 10, v.total
      assert_equal 10, v.current
      assert_equal @now, v.last_modified_at
    end

    test 'create and increment a value when force option is true' do
      name = 'new_key'
      param = { 'name' => name, 'value' => 11, 'reset_interval' => 1 }

      assert_nil extract_value_from_server(@server, @scope, name)

      @client.inc(param, options: { force: true })

      v = extract_value_from_server(@server, @scope, name)
      assert_equal name, v.name
      assert_equal 1, v.reset_interval
      assert_equal 11, v.total
      assert_equal 11, v.total
    end

    data(
      not_exist_key: [
        { 'name' => 'key2', 'value' => 10 },
        { 'code' => 'unknown_key', 'message' => "`key2` doesn't exist in counter" }
      ],
      missing_name: [
        { 'value' => 10 },
        { 'code' => 'invalid_params', 'message' => '`name` is required' },
      ],
      missing_value: [
        { 'name' => 'key' },
        { 'code' => 'invalid_params', 'message' => '`value` is required' },
      ],
      invalid_key: [
        { 'name' => '\tkey' },
        { 'code' => 'invalid_params', 'message' => '`name` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      resopnse = @client.inc(param)

      errors = resopnse['errors'].first
      assert_empty resopnse['data']
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      parmas = [
        { 'name' => @name, 'value' => 10 },
        { 'name' => 'unknown_key', 'value' => 9 }
      ]
      response = @client.inc(*parmas)

      data = response['data'].first
      error = response['errors'].first

      assert_equal @name, data['name']
      assert_equal 10, data['current']
      assert_equal 10, data['total']

      assert_equal 'unknown_key', error['code']
      assert_equal "`unknown_key` doesn't exist in counter", error['message']
    end

    test 'return a future object when asyn option is true' do
      param = { 'name' => 'key', 'value' => 10 }
      r = @client.inc(param, options: { async: true })
      assert_true r.is_a?(Fluent::Counter::Future)
    end
  end

  sub_test_case 'get' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'

      @init_obj = { 'name' => @name, 'reset_interval' => 20, 'type' => 'numeric' }
      @client.init(@init_obj)
    end

    test 'get a value' do
      v1 = extract_value_from_server(@server, @scope, @name)
      v2 = @client.get(@name)['data'].first

      assert_equal v1.name, v2['name']
      assert_equal v1.current, v2['current']
      assert_equal v1.total, v2['total']
      assert_equal v1.type, v2['type']
    end

    data(
      key_not_found: [
        'key2',
        { 'code' => 'unknown_key', 'message' => "`key2` doesn't exist in counter" }
      ],
      invalid_key: [
        '\tkey',
        { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      resopnse = @client.get(param)

      errors = resopnse['errors'].first

      assert_empty resopnse['data']
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      unknow_name = 'key2'

      response = @client.get(@name, unknow_name)
      data = response['data'].first
      error = response['errors'].first

      assert_equal @name, data['name']
      assert_equal @init_obj['reset_interval'], data['reset_interval']

      assert_equal 'unknown_key', error['code']
      assert_equal "`key2` doesn't exist in counter", error['message']
    end

    test 'return a future object when asyn option is true' do
      r = @client.get(@name, options: { async: true })
      assert_true r.is_a?(Fluent::Counter::Future)
    end
  end

  sub_test_case 'rseet' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'

      @init_obj = { 'name' => @name, 'reset_interval' => 5, 'type' => 'numeric' }
      @client.init(@init_obj)
      @client.inc({ 'name' => @name, 'value' => 10 })
    end

    test 'reset a value after `reset_interval` passed' do
      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 10, v1.total
      assert_equal 10, v1.current
      assert_equal @now, v1.last_reset_at

      Timecop.travel(6)         # greater than reset_interval

      v2 = @client.reset(@name)
      data = v2['data'].first

      c = data['counter_data']

      assert_equal 6, data['elapsed_time']
      assert_true data['success']

      assert_equal 10, c['current']
      assert_equal 10, c['total']
      assert_equal @now, c['last_reset_at']

      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 0, v1.current
      assert_equal 10, v1.total
      assert_equal (@now + 6), v1.last_reset_at
      assert_equal (@now + 6), v1.last_modified_at
    end

    test 'return a value object before `reset_interval` passed' do
      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 10, v1.total
      assert_equal 10, v1.current
      assert_equal @now, v1.last_reset_at

      Timecop.travel(4)         # less than reset_interval

      v2 = @client.reset(@name)
      data = v2['data'].first

      c = data['counter_data']

      assert_equal 4, data['elapsed_time']
      assert_equal false, data['success']

      assert_equal 10, c['current']
      assert_equal 10, c['total']
      assert_equal @now, c['last_reset_at']

      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 10, v1.current
      assert_equal 10, v1.total
      assert_equal @now, v1.last_reset_at
      assert_equal @now, v1.last_reset_at
    end

    data(
      key_not_found: [
        'key2',
        { 'code' => 'unknown_key', 'message' => "`key2` doesn't exist in counter" }
      ],
      invalid_key: [
        '\tkey',
        { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      resopnse = @client.reset(param)

      errors = resopnse['errors'].first

      assert_empty resopnse['data']
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      unknow_name = 'key2'

      Timecop.travel(6)         # greater than reset_interval

      response = @client.reset(@name, unknow_name)
      data = response['data'].first
      error = response['errors'].first
      counter = data['counter_data']

      assert_true data['success']
      assert_equal 6, data['elapsed_time']
      assert_equal @name, counter['name']
      assert_equal @init_obj['reset_interval'], counter['reset_interval']
      assert_equal 10, counter['total']
      assert_equal 10, counter['current']

      assert_equal 'unknown_key', error['code']
      assert_equal "`key2` doesn't exist in counter", error['message']

      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 0, v1.current
      assert_equal 10, v1.total
      assert_equal (@now + 6), v1.last_reset_at
      assert_equal (@now + 6), v1.last_modified_at
    end

    test 'return a future object when asyn option is true' do
      r = @client.reset(@name, options: { async: true })
      assert_true r.is_a?(Fluent::Counter::Future)
    end
  end
end
