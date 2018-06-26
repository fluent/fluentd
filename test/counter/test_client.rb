require_relative '../helper'
require 'fluent/counter/client'
require 'fluent/counter/store'
require 'fluent/counter/server'
require 'flexmock/test_unit'
require 'timecop'

class CounterClientTest < ::Test::Unit::TestCase
  TEST_ADDR = '127.0.0.1'
  TEST_PORT = '8277'

  setup do
    # timecop isn't compatible with EventTime
    t = Time.parse('2016-09-22 16:59:59 +0900')
    Timecop.freeze(t)
    @now = Fluent::EventTime.now

    @options = {
      addr: TEST_ADDR,
      port: TEST_PORT,
      log: $log,
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

  sub_test_case 'on_message' do
    setup do
      @future = flexmock('future')
      @client.instance_variable_set(:@responses, { 1 => @future })
    end

    test 'call a set method to a corresponding object' do
      @future.should_receive(:set).once.with(Hash)
      @client.send(:on_message, { 'id' => 1 })
    end

    test "output a warning log when passed id doesn't exist" do
      data = { 'id' => 2 }
      mock($log).warn("Receiving missing id data: #{data}")
      @client.send(:on_message, data)
    end
  end

  def extract_value_from_server(server, scope, name)
    store = server.instance_variable_get(:@store).instance_variable_get(:@storage).instance_variable_get(:@store)
    key = Fluent::Counter::Store.gen_key(scope, name)
    store[key]
  end

  def travel(sec)
    # Since Timecop.travel() causes test failures on Windows/AppVeyor by inducing
    # rounding errors to Time.now, we need to use Timecop.freeze() instead.
    Timecop.freeze(Time.now + sec)
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
        { name: 'key', reset_interval: 20, type: 'numeric' }, 0
      ],
      float_type: [
        { name: 'key', reset_interval: 20, type: 'float' }, 0.0
      ],
      integer_type: [
        { name: 'key', reset_interval: 20, type: 'integer' }, 0
      ]
    )
    test 'create a value' do |(param, initial_value)|
      assert_nil extract_value_from_server(@server, @scope, param[:name])

      response = @client.init(param).get
      data = response.data.first

      assert_nil response.errors
      assert_equal param[:name], data['name']
      assert_equal param[:reset_interval], data['reset_interval']
      assert_equal param[:type], data['type']
      assert_equal initial_value, data['current']
      assert_equal initial_value, data['total']

      v = extract_value_from_server(@server, @scope, param[:name])
      assert_equal param[:name], v['name']
      assert_equal param[:reset_interval], v['reset_interval']
      assert_equal param[:type], v['type']
      assert_equal initial_value, v['total']
      assert_equal initial_value, v['current']
    end

    test 'raise an error when @scope is nil' do
      @client.instance_variable_set(:@scope, nil)
      assert_raise 'Call `establish` method to get a `scope` before calling this method' do
        @client.init(name: 'key1', reset_interval: 10).get
      end
    end

    data(
      already_exist_key: [
        { name: 'key1', reset_interval: 10 },
        { 'code' => 'invalid_params', 'message' => "worker1\tplugin1\tkey1 already exists in counter" }
      ],
      missing_name: [
        { reset_interval: 10 },
        { 'code' => 'invalid_params', 'message' => '`name` is required' },
      ],
      missing_reset_interval: [
        { name: 'key' },
        { 'code' => 'invalid_params', 'message' => '`reset_interval` is required' },
      ],
      invalid_name: [
        { name: '\tkey' },
        { 'code' => 'invalid_params', 'message' => '`name` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      @client.init(:name => 'key1', :reset_interval => 10).get
      response = @client.init(param).get
      errors = response.errors.first

      assert_empty response.data
      assert_equal expected_error, errors

      assert_raise {
        @client.init(param).wait
      }
    end

    test 'return an existing value when passed key already exists and ignore option is true' do
      res1 = @client.init(name: 'key1', reset_interval: 10).get
      res2 = nil
      assert_nothing_raised do
        res2 = @client.init({ name: 'key1', reset_interval: 10 }, options: { ignore: true }).get
      end
      assert_equal res1.data, res2.data
    end

    test 'return an error object and data object' do
      param = { name: 'key1', reset_interval: 10 }
      param2 = { name: 'key2', reset_interval: 10 }
      @client.init(param).get

      response = @client.init([param2, param]).get
      data = response.data.first
      error = response.errors.first

      assert_equal param2[:name], data['name']
      assert_equal param2[:reset_interval], data['reset_interval']

      assert_equal 'invalid_params', error['code']
      assert_equal "#{@scope}\t#{param[:name]} already exists in counter", error['message']
    end

    test 'return a future object when async call' do
      param = { name: 'key', reset_interval: 10 }
      r = @client.init(param)
      assert_true r.is_a?(Fluent::Counter::Future)
      assert_nil r.errors
    end
  end

  sub_test_case 'delete' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'
      @key = Fluent::Counter::Store.gen_key(@scope, @name)

      @init_obj = { name: @name, reset_interval: 20, type: 'numeric' }
      @client.init(@init_obj).get
    end

    test 'delete a value' do
      assert extract_value_from_server(@server, @scope, @name)

      response = @client.delete(@name).get
      v = response.data.first

      assert_nil response.errors
      assert_equal @init_obj[:name], v['name']
      assert_equal @init_obj[:type], v['type']
      assert_equal @init_obj[:reset_interval], v['reset_interval']

      assert_nil extract_value_from_server(@server, @scope, @name)
    end

    test 'raise an error when @scope is nil' do
      @client.instance_variable_set(:@scope, nil)
      assert_raise 'Call `establish` method to get a `scope` before calling this method' do
        @client.delete(@name).get
      end
    end

    data(
      key_not_found: [
        'key2',
        { 'code' => 'unknown_key', 'message' => "`worker1\tplugin1\tkey2` doesn't exist in counter" }
      ],
      invalid_key: [
        '\tkey',
        { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      response = @client.delete(param).get
      errors = response.errors.first

      assert_empty response.data
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      unknown_name = 'key2'

      response = @client.delete(@name, unknown_name).get
      data = response.data.first
      error = response.errors.first

      assert_equal @name, data['name']
      assert_equal @init_obj[:reset_interval], data['reset_interval']

      assert_equal 'unknown_key', error['code']
      assert_equal "`#{@scope}\t#{unknown_name}` doesn't exist in counter", error['message']

      assert_nil extract_value_from_server(@server, @scope, @name)
    end

    test 'return a future object when async call' do
      r = @client.delete(@name)
      assert_true r.is_a?(Fluent::Counter::Future)
      assert_nil r.errors
    end
  end

  sub_test_case 'inc' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'
      @key = Fluent::Counter::Store.gen_key(@scope, @name)

      @init_obj = { name: @name, reset_interval: 20, type: 'numeric' }
      @client.init(@init_obj).get
    end

    test 'increment a value' do
      v = extract_value_from_server(@server, @scope, @name)
      assert_equal 0, v['total']
      assert_equal 0, v['current']

      travel(1)
      inc_obj = { name: @name, value: 10 }
      @client.inc(inc_obj).get

      v = extract_value_from_server(@server, @scope, @name)
      assert_equal inc_obj[:value], v['total']
      assert_equal inc_obj[:value], v['current']
      assert_equal (@now + 1), Fluent::EventTime.new(*v['last_modified_at'])
    end

    test 'create and increment a value when force option is true' do
      name = 'new_key'
      param = { name: name, value: 11, reset_interval: 1 }

      assert_nil extract_value_from_server(@server, @scope, name)

      @client.inc(param, options: { force: true }).get

      v = extract_value_from_server(@server, @scope, name)
      assert v
      assert_equal param[:name], v['name']
      assert_equal 1, v['reset_interval']
      assert_equal param[:value], v['current']
      assert_equal param[:value], v['total']
    end

    test 'raise an error when @scope is nil' do
      @client.instance_variable_set(:@scope, nil)
      assert_raise 'Call `establish` method to get a `scope` before calling this method' do
        @client.inc(name: 'name', value: 1).get
      end
    end

    data(
      not_exist_key: [
        { name: 'key2', value: 10 },
        { 'code' => 'unknown_key', 'message' => "`worker1\tplugin1\tkey2` doesn't exist in counter" }
      ],
      missing_name: [
        { value: 10 },
        { 'code' => 'invalid_params', 'message' => '`name` is required' },
      ],
      missing_value: [
        { name: 'key' },
        { 'code' => 'invalid_params', 'message' => '`value` is required' },
      ],
      invalid_name: [
        { name: '\tkey' },
        { 'code' => 'invalid_params', 'message' => '`name` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      response = @client.inc(param).get
      errors = response.errors.first
      assert_empty response.data
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      parmas = [
        { name: @name, value: 10 },
        { name: 'unknown_key', value: 9 },
      ]
      response = @client.inc(parmas).get

      data = response.data.first
      error = response.errors.first

      assert_equal @name, data['name']
      assert_equal 10, data['current']
      assert_equal 10, data['total']

      assert_equal 'unknown_key', error['code']
      assert_equal "`#{@scope}\tunknown_key` doesn't exist in counter", error['message']
    end

    test 'return a future object when async call' do
      param = { name: 'key', value: 10 }
      r = @client.inc(param)
      assert_true r.is_a?(Fluent::Counter::Future)
      assert_nil r.errors
    end
  end

  sub_test_case 'get' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'

      @init_obj = { name: @name, reset_interval: 20, type: 'numeric' }
      @client.init(@init_obj).get
    end

    test 'get a value' do
      v1 = extract_value_from_server(@server, @scope, @name)
      v2 = @client.get(@name).data.first

      assert_equal v1['name'], v2['name']
      assert_equal v1['current'], v2['current']
      assert_equal v1['total'], v2['total']
      assert_equal v1['type'], v2['type']
    end

    test 'raise an error when @scope is nil' do
      @client.instance_variable_set(:@scope, nil)
      assert_raise 'Call `establish` method to get a `scope` before calling this method' do
        @client.get(@name).get
      end
    end

    data(
      key_not_found: [
        'key2',
        { 'code' => 'unknown_key', 'message' => "`worker1\tplugin1\tkey2` doesn't exist in counter" }
      ],
      invalid_key: [
        '\tkey',
        { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      response = @client.get(param).get
      errors = response.errors.first
      assert_empty response.data
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      unknown_name = 'key2'

      response = @client.get(@name, unknown_name).get
      data = response.data.first
      error = response.errors.first

      assert_equal @name, data['name']
      assert_equal @init_obj[:reset_interval], data['reset_interval']

      assert_equal 'unknown_key', error['code']
      assert_equal "`#{@scope}\t#{unknown_name}` doesn't exist in counter", error['message']
    end

    test 'return a future object when async call' do
      r = @client.get(@name)
      assert_true r.is_a?(Fluent::Counter::Future)
      assert_nil r.errors
    end
  end

  sub_test_case 'reset' do
    setup do
      @client.instance_variable_set(:@scope, @scope)
      @name = 'key'
      @key = Fluent::Counter::Store.gen_key(@scope, @name)

      @init_obj = { name: @name, reset_interval: 5, type: 'numeric' }
      @client.init(@init_obj).get
      @inc_obj = { name: @name, value: 10 }
      @client.inc(@inc_obj).get
    end

    test 'reset a value after `reset_interval` passed' do
      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal @inc_obj[:value], v1['total']
      assert_equal @inc_obj[:value], v1['current']
      assert_equal @now, Fluent::EventTime.new(*v1['last_reset_at'])

      travel_sec = 6            # greater than reset_interval
      travel(travel_sec)

      v2 = @client.reset(@name).get
      data = v2.data.first

      c = data['counter_data']

      assert_equal travel_sec, data['elapsed_time']
      assert_true data['success']

      assert_equal @inc_obj[:value], c['current']
      assert_equal @inc_obj[:value], c['total']
      assert_equal @now, c['last_reset_at']

      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 0, v1['current']
      assert_equal @inc_obj[:value], v1['total']
      assert_equal (@now + travel_sec), Fluent::EventTime.new(*v1['last_reset_at'])
      assert_equal (@now + travel_sec), Fluent::EventTime.new(*v1['last_modified_at'])
    end

    test 'areturn a value object before `reset_interval` passed' do
      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal @inc_obj[:value], v1['total']
      assert_equal @inc_obj[:value], v1['current']
      assert_equal @now, Fluent::EventTime.new(*v1['last_reset_at'])

      travel_sec = 4            # less than reset_interval
      travel(travel_sec)

      v2 = @client.reset(@name).get
      data = v2.data.first

      c = data['counter_data']

      assert_equal travel_sec, data['elapsed_time']
      assert_equal false, data['success']

      assert_equal @inc_obj[:value], c['current']
      assert_equal @inc_obj[:value], c['total']
      assert_equal @now, c['last_reset_at']

      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal @inc_obj[:value], v1['current']
      assert_equal @inc_obj[:value], v1['total']
      assert_equal @now, Fluent::EventTime.new(*v1['last_reset_at'])
    end

    test 'raise an error when @scope is nil' do
      @client.instance_variable_set(:@scope, nil)
      assert_raise 'Call `establish` method to get a `scope` before calling this method' do
        @client.reset(@name).get
      end
    end

    data(
      key_not_found: [
        'key2',
        { 'code' => 'unknown_key', 'message' => "`worker1\tplugin1\tkey2` doesn't exist in counter" }
      ],
      invalid_key: [
        '\tkey',
        { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
      ]
    )
    test 'return an error object' do |(param, expected_error)|
      response = @client.reset(param).get
      errors = response.errors.first
      assert_empty response.data
      assert_equal expected_error, errors
    end

    test 'return an error object and data object' do
      unknown_name = 'key2'

      travel_sec = 6            # greater than reset_interval
      travel(travel_sec)

      response = @client.reset(@name, unknown_name).get
      data = response.data.first
      error = response.errors.first
      counter = data['counter_data']

      assert_true data['success']
      assert_equal travel_sec, data['elapsed_time']
      assert_equal @name, counter['name']
      assert_equal @init_obj[:reset_interval], counter['reset_interval']
      assert_equal @inc_obj[:value], counter['total']
      assert_equal @inc_obj[:value], counter['current']

      assert_equal 'unknown_key', error['code']
      assert_equal "`#{@scope}\t#{unknown_name}` doesn't exist in counter", error['message']

      v1 = extract_value_from_server(@server, @scope, @name)
      assert_equal 0, v1['current']
      assert_equal @inc_obj[:value], v1['total']
      assert_equal (@now + travel_sec), Fluent::EventTime.new(*v1['last_reset_at'])
      assert_equal (@now + travel_sec), Fluent::EventTime.new(*v1['last_modified_at'])
    end

    test 'return a future object when async call' do
      r = @client.reset(@name)
      assert_true r.is_a?(Fluent::Counter::Future)
      assert_nil r.errors
    end
  end
end
