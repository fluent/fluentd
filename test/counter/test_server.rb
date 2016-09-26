require_relative '../helper'
require 'fluent/counter/server'
require 'fluent/time'
require 'flexmock/test_unit'
require 'timecop'

class CounterCounterTest < ::Test::Unit::TestCase
  setup do
    # timecop isn't compatible with EventTime
    t = Time.parse('2016-09-22 16:59:59 +0900')
    Timecop.freeze(t)
    @now = Fluent::EventTime.now

    @scope = "server\tworker\tplugin"
    @server_name = 'server1'
    @counter = Fluent::Counter::Counter.new(@server_name, $log)
  end

  teardown do
    Timecop.return
  end

  def extract_value_from_counter(counter, scope, name)
    store = counter.instance_variable_get(:@store).instance_variable_get(:@store)
    key = "#{scope}\t#{name}"
    store[key]
  end

  test 'raise an error when server name is invalid' do
    assert_raise do
      Fluent::Counter::Counter.new("\tinvalid_name")
    end
  end

  sub_test_case 'on_message' do
    data(
      establish: 'establish',
      init: 'init',
      delete: 'delete',
      inc: 'inc',
      get: 'get',
      reset: 'reset',
    )
    test 'call valid methods' do |method|
      stub(@counter).send do |_m, params, scope, options|
        { 'data' => [params, scope, options] }
      end

      request = { 'id' => 0, 'method' => method }
      expected = { 'id' => 0, 'data' => [nil, nil, nil] }
      assert_equal expected, @counter.on_message(request)
    end

    data(
      missing_id: [
        { 'method' => 'init' },
        { 'code' => 'invalid_request', 'message' => 'Request should include `id`' }
      ],
      missing_mehtod: [
        { 'id' => 0 },
        { 'code' => 'invalid_request', 'message' => 'Request should include `method`' }
      ],
      invalid_mehtod: [
        { 'id' => 0, 'method' => 'invalid_method'},
        { 'code' => 'method_not_found', 'message' => 'Unknown Method name passed: invalid_method'}
      ]
    )
    test 'invalid request' do |(request, error)|
      expected = {
        'id' => request['id'],
        'data' => [],
        'errors' => [error],
      }

      assert_equal expected, @counter.on_message(request)
    end

    test 'return an `internal_server_error` error object when an error raises in safe_run' do
      stub(@counter).send do |_m, _params, _scope, _options|
        raise 'Error in safe_run'
      end

      request = { 'id' => 0, 'method' => 'init' }
      expected = {
        'id' => request['id'],
        'data' => [],
        'errors' => [
          { 'code' => 'internal_server_error', 'message' => 'Error in safe_run' }
        ]
      }
      assert_equal expected, @counter.on_message(request)
    end

    test 'output an error log when passed data is not Hash' do
      data = 'this is not a hash'
      mock($log).error("Recieved data is not Hash: #{data}")
      @counter.on_message(data)
    end
  end

  sub_test_case 'establish' do
    test 'establish a scope in a counter' do
      result = @counter.send('establish', ['key'], nil, nil)
      expected = { 'data' => ["#{@server_name}\tkey"] }
      assert_equal expected, result
    end

    data(
      empty: [[], 'One or more `params` are required'],
      empty_key: [[''], '`scope` is the invalid format'],
      invalid_key: [['_key'], '`scope` is the invalid format'],
    )
    test 'raise an error: invalid_params' do |(params, msg)|
      result = @counter.send('establish', params, nil, nil)
      expected = {
        'data' => [],
        'errors' => [{ 'code' => 'invalid_params', 'message' => msg }]
      }
      assert_equal expected, result
    end
  end

  sub_test_case 'init' do
    setup do
      @name = 'key1'
    end

    test 'create new value in a counter' do
      assert_nil extract_value_from_counter(@counter, @scope, @name)
      result = @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, {})
      assert_nil result['errors']

      counter = result['data'].first
      assert_equal 'numeric', counter['type']
      assert_equal @name, counter['name']
      assert_equal 0, counter['current']
      assert_equal 0, counter['total']
      assert_equal @now, counter['last_reset_at']
      assert extract_value_from_counter(@counter, @scope, @name)
    end

    data(
      numeric: 'numeric',
      integer: 'integer',
      float: 'float'
    )
    test 'set the type of a counter value' do |type|
      result = @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1, 'type' => type }], @scope, {})
      counter = result['data'].first
      assert_equal type, counter['type']

      v = extract_value_from_counter(@counter, @scope, @name)
      assert_equal type, v.type
    end

    data(
      empty: [[], 'One or more `params` are required'],
      missing_name: [
        [{ 'rest_interval' => 20 }],
        '`name` is required'
      ],
      invalid_name: [
        [{ 'name' => '_test', 'reset_interval' => 20 }],
        '`name` is the invalid format'
      ],
      missing_interval: [
        [{ 'name' => 'test' }],
        '`reset_interval` is required'
      ],
      minus_interval: [
        [{ 'name' => 'test', 'reset_interval' => -1 }],
        '`reset_interval` should be a positive number'
      ],
      invalid_type: [
        [{ 'name' => 'test', 'reset_interval' => 1, 'type' => 'invalid_type' }],
        '`type` should be integer, float, or numeric'
      ]
    )
    test 'return an error object: invalid_params' do |(params, msg)|
      result = @counter.send('init', params, @scope, {})
      assert_empty result['data']
      error = result['errors'].first
      assert_equal 'invalid_params', error['code']
      assert_equal msg, error['message']
    end

    test 'return error objects when passed key already exists' do
      @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, {})

      # call `init` to same key twice
      result = @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, {})
      assert_empty result['data']
      error = result['errors'].first

      expected = { 'code' => 'invalid_params', 'message' => "#{@name} already exists in counter" }
      assert_equal expected, error
    end

    test 'return existing value when passed key already exists and ignore option is true' do
      v1 = @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, {})

      # call `init` to same key twice
      v2 = @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, { 'ignore' => true })
      assert_nil v2['errors']
      assert_equal v1['data'], v2['data']
    end

    test 'call `synchronize_keys` when random option is true' do
      mhash = @counter.instance_variable_get(:@mutex_hash)
      mock(mhash).synchronize(@name).once
      @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, {})

      mhash = @counter.instance_variable_get(:@mutex_hash)
      mock(mhash).synchronize_keys(@name).once
      @counter.send('init', [{ 'name' => @name, 'reset_interval' => 1 }], @scope, { 'random' => true })
    end
  end

  sub_test_case 'delete' do
    setup do
      @name = 'key1'
      @counter.send('init', [{ 'name' => @name, 'reset_interval' => 20 }], @scope, {})
    end

    test 'delete a value from a counter' do
      assert extract_value_from_counter(@counter, @scope, @name)

      result = @counter.send('delete', [@name], @scope, {})
      assert_nil result['errors']

      counter = result['data'].first
      assert_equal 0, counter['current']
      assert_equal 0, counter['total']
      assert_equal 'numeric', counter['type']
      assert_equal @name, counter['name']
      assert_equal @now, counter['last_reset_at']

      assert_nil extract_value_from_counter(@counter, @scope, @name)
    end

    data(
      empty: [[], 'One or more `params` are required'],
      empty_key: [[''], '`key` is the invalid format'],
      invalid_key: [['_key'], '`key` is the invalid format'],
    )
    test 'return an error object: invalid_params' do |(params, msg)|
      result = @counter.send('delete', params, @scope, {})

      assert_empty result['data']
      error = result['errors'].first
      assert_equal 'invalid_params', error['code']
      assert_equal msg, error['message']
    end

    test 'return an error object: unknown_key' do
      unknown_key = 'unknown_key'
      result = @counter.send('delete', [unknown_key], @scope, {})

      assert_empty result['data']
      error = result['errors'].first
      assert_equal unknown_key, error['code']
      assert_equal "`#{unknown_key}` doesn't exist in counter", error['message']
    end

    test 'call `synchronize_keys` when random option is true' do
      mhash = @counter.instance_variable_get(:@mutex_hash)
      mock(mhash).synchronize(@name).once
      @counter.send('delete', [@name], @scope, {})

      mhash = @counter.instance_variable_get(:@mutex_hash)
      mock(mhash).synchronize_keys(@name).once
      @counter.send('delete', [@name], @scope, { 'random' => true })
    end
  end

  sub_test_case 'inc' do
    setup do
      @name1 = 'key1'
      @name2 = 'key2'
      inc_objects = [
        { 'name' => @name1, 'reset_interval' => 20 },
        { 'name' => @name2, 'type' => 'integer', 'reset_interval' => 20 }
      ]
      @counter.send('init', inc_objects, @scope, {})
    end

    test 'increment or decrement a value in counter' do
      result = @counter.send('inc', [{ 'name' => @name1, 'value' => 10 }], @scope, {})
      assert_nil result['errors']

      counter = result['data'].first
      assert_equal 10, counter['current']
      assert_equal 10, counter['total']
      assert_equal 'numeric', counter['type']
      assert_equal @name1, counter['name']
      assert_equal @now, counter['last_reset_at']

      c = extract_value_from_counter(@counter, @scope, @name1)
      assert_equal 10, c.current
      assert_equal 10, c.total
      assert_equal @now, c.last_reset_at
      assert_equal @now, c.last_modified_at
    end

    test 'create new value and increment/decrement its value when `force` option is true' do
      new_name = 'new_key'
      assert_nil extract_value_from_counter(@counter, @scope, new_name)

      v1 = @counter.send('inc', [{ 'name' => new_name, 'value' => 10 }], @scope, {})
      assert_empty v1['data']
      error = v1['errors'].first
      assert_equal 'unknown_key', error['code']

      assert_nil extract_value_from_counter(@counter, @scope, new_name)

      v2 = @counter.send(
        'inc',
        [{ 'name' => new_name, 'value' => 10, 'reset_interval' => 20 }],
        @scope,
        { 'force' => true }
      )

      assert_nil v2['errors']

      counter = v2['data'].first
      assert_equal 10, counter['current']
      assert_equal 10, counter['total']
      assert_equal 'numeric', counter['type']
      assert_equal new_name, counter['name']
      assert_equal @now, counter['last_reset_at']

      assert extract_value_from_counter(@counter, @scope, new_name)
    end

    data(
      empty: [[], 'One or more `params` are required', {}],
      missing_name: [
        [{ 'value' => 10 }],
        '`name` is required', {}
      ],
      missing_value: [
        [{ 'name' => 'key1' }],
        '`value` is required', {}
      ],
      invalid_type: [
        [{ 'name' => 'key2', 'value' => 10.0 }],
        '`type` is integer. You should pass integer value as a `value`', {}
      ],
      missing_reset_interval: [
        [{ 'name' => 'key1', 'value' => 1 }],
        '`reset_interval` is required',
        { 'force' => true }
      ]
    )
    test 'return an error object: invalid_params' do |(params, msg, opt)|
      result = @counter.send('inc', params, @scope, opt)
      assert_empty result['data']

      error = result['errors'].first
      assert_equal 'invalid_params', error['code']
      assert_equal msg, error['message']
    end

    test 'call `synchronize_keys` when random option is true' do
      mhash = @counter.instance_variable_get(:@mutex_hash)
      mock(mhash).synchronize(@name1).once
      params = [{ 'name' => @name1, 'value' => 1 }]
      @counter.send('inc', params, @scope, {})

      mhash = @counter.instance_variable_get(:@mutex_hash)
      mock(mhash).synchronize_keys(@name1).once
      @counter.send('inc', params, @scope, { 'random' => true })
    end
  end

  sub_test_case 'reset' do
    setup do
      @name = 'key'
      @travel_sec = 10
      @counter.send('init', [{ 'name' => @name, 'reset_interval' => 10 }], @scope, {})
      @counter.send('inc', [{ 'name' => @name, 'value' => 10 }], @scope, {})
    end

    test 'reset a value in the counter' do
      Timecop.travel(@travel_sec)

      result = @counter.send('reset', [@name], @scope, {})
      assert_nil result['errors']

      data = result['data'].first
      assert_true data['success']
      assert_equal @travel_sec, data['elapsed_time']

      counter = data['counter_data']
      assert_equal 10, counter['current']
      assert_equal 10, counter['total']
      assert_equal 'numeric', counter['type']
      assert_equal @name, counter['name']
      assert_equal @now, counter['last_reset_at']

      v = extract_value_from_counter(@counter, @scope, @name)
      assert_equal 0, v.current
      assert_equal 10, v.total
      assert_equal (@now + @travel_sec), v.last_modified_at
      assert_equal (@now + @travel_sec) , v.last_modified_at
    end

    test 'reset a value after `reset_interval` passed' do
      first_travel_sec = 5
      Timecop.travel(first_travel_sec) # jump time less than reset_interval
      result = @counter.send('reset', [@name], @scope, {})
      v = result['data'].first

      assert_equal false, v['success']
      assert_equal first_travel_sec, v['elapsed_time']

      store = extract_value_from_counter(@counter, @scope, @name)
      assert_equal 10, store.current
      assert_equal @now, store.last_reset_at

      # time is passed greater than reset_interval
      Timecop.travel(@travel_sec)
      result = @counter.send('reset', [@name], @scope, {})
      v = result['data'].first

      assert_true v['success']
      assert_equal @travel_sec + first_travel_sec, v['elapsed_time']

      v1 = extract_value_from_counter(@counter, @scope, @name)
      assert_equal 0, v1.current
      assert_equal (@now + @travel_sec + first_travel_sec), v1.last_reset_at
      assert_equal (@now + @travel_sec + first_travel_sec), v1.last_modified_at
    end

    data(
      empty: [[], 'One or more `params` are required'],
      empty_key: [[''], '`key` is the invalid format'],
      invalid_key: [['_key'], '`key` is the invalid format'],
    )
    test 'return an error object: invalid_params' do |(params, msg)|
      result = @counter.send('reset', params, @scope, {})
      assert_empty result['data']
      assert_equal 'invalid_params', result['errors'].first['code']
      assert_equal msg, result['errors'].first['message']
    end

    test 'return an error object: unknown_key' do
      unknown_key = 'unknown_key'
      result = @counter.send('reset', [unknown_key], @scope, {})

      assert_empty result['data']
      error = result['errors'].first
      assert_equal unknown_key, error['code']
      assert_equal "`#{unknown_key}` doesn't exist in counter", error['message']
    end
  end

  sub_test_case 'get' do
    setup do
      @name1 = 'key1'
      @name2 = 'key2'
      init_objects = [
        { 'name' => @name1, 'reset_interval' => 0 },
        { 'name' => @name2, 'reset_interval' => 0 },
      ]
      @counter.send('init', init_objects, @scope, {})
    end

    test 'get a counter value' do
      key = @name1
      result = @counter.send('get', [key], @scope, {})
      assert_nil result['errors']

      counter = result['data'].first
      assert_equal 0, counter['current']
      assert_equal 0, counter['total']
      assert_equal 'numeric', counter['type']
      assert_equal key, counter['name']
    end

    test 'get counter values' do
      result = @counter.send('get', [@name1, @name2], @scope, {})
      assert_nil result['errors']

      counter1 = result['data'][0]
      assert_equal 0, counter1['current']
      assert_equal 0, counter1['total']
      assert_equal 'numeric', counter1['type']
      assert_equal @name1, counter1['name']

      counter2 = result['data'][1]
      assert_equal 0, counter2['current']
      assert_equal 0, counter2['total']
      assert_equal 'numeric', counter2['type']
      assert_equal @name2, counter2['name']
    end

    data(
      empty: [[], 'One or more `params` are required'],
      empty_key: [[''], '`key` is the invalid format'],
      invalid_key: [['_key'], '`key` is the invalid format'],
    )
    test 'return an error object: invalid_params' do |(params, msg)|
      result = @counter.send('get', params, @scope, {})
      assert_empty result['data']
      assert_equal 'invalid_params', result['errors'].first['code']
      assert_equal msg, result['errors'].first['message']
    end

    test 'return an error object: unknown_key' do
      unknown_key = 'unknown_key'
      result = @counter.send('get', [unknown_key], @scope, {})

      assert_empty result['data']
      error = result['errors'].first
      assert_equal unknown_key, error['code']
      assert_equal "`#{unknown_key}` doesn't exist in counter", error['message']
    end
  end
end

class CounterCounterResponseTest < ::Test::Unit::TestCase
  setup do
    @response = Fluent::Counter::Counter::Response.new
    @errors = [
      StandardError.new('standard error'),
      Fluent::Counter::InternalServerError.new('internal server error')
    ]
    @now = Fluent::EventTime.now
    value = Fluent::Counter::Store::Value.new('name', 100, 11, 'numeric', 10, @now, @now)
    @values = [value, 'test']
  end

  test 'push_error' do
    @errors.each do |e|
      @response.push_error(e)
    end

    v = @response.instance_variable_get(:@errors)
    assert_equal @errors, v
  end

  test 'push_data' do
    @values.each do |v|
      @response.push_data v
    end

    data = @response.instance_variable_get(:@data)
    assert_equal @values, data
  end

  test 'to_hash' do
    @errors.each do |e|
      @response.push_error(e)
    end

    @values.each do |v|
      @response.push_data v
    end

    expected_errors = [
      { 'code' => 'internal_server_error', 'message' => 'standard error' },
      { 'code' => 'internal_server_error', 'message' => 'internal server error' }
    ]
    expected_data = [
      {
        'name' => 'name',
        'total' => 100,
        'current' => 11,
        'type' => 'numeric',
        'reset_interval' => 10,
        'last_reset_at' => @now,
      },
      'test'
    ]

    hash = @response.to_hash
    assert_equal expected_errors, hash['errors']
    assert_equal expected_data, hash['data']
  end
end
