require_relative '../helper'
require 'fluent/counter/validator'

class CounterValidatorTest < ::Test::Unit::TestCase
  data(
    invalid_name1: '',
    invalid_name3: '_',
    invalid_name4: 'A',
    invalid_name5: 'a*',
    invalid_name6: "a\t",
    invalid_name7: "\n",
  )
  test 'invalid name' do |invalid_name|
    assert_nil(Fluent::Counter::Validator::VALID_NAME =~ invalid_name)
  end

  sub_test_case 'request' do
    test 'return an empty array' do
      data = { 'id' => 0, 'method' => 'init' }
      errors = Fluent::Counter::Validator.request(data)
      assert_empty errors
    end

    data(
      missing_id: [
        { 'method' => 'init' },
        { 'code' => 'invalid_request', 'message' => 'Request should include `id`' }
      ],
      missing_method: [
        { 'id' => 0 },
        { 'code' => 'invalid_request', 'message' => 'Request should include `method`' }
      ],
      invalid_method: [
        { 'id' => 0, 'method' => "A\t" },
        { 'code' => 'invalid_request', 'message' => '`method` is the invalid format' }
      ],
      unknown_method: [
        { 'id' => 0, 'method' => 'unknown_method' },
        { 'code' => 'method_not_found', 'message' => 'Unknown method name passed: unknown_method' }
      ]
    )
    test 'return an error array' do |(data, expected_error)|
      errors = Fluent::Counter::Validator.request(data)
      assert_equal [expected_error], errors
    end
  end

  sub_test_case 'call' do
    test "return an error hash when passed method doesn't exist" do
      v = Fluent::Counter::Validator.new(:unknown)
      success, errors = v.call(['key1'])
      assert_empty success
      assert_equal 'internal_server_error', errors.first.to_hash['code']
    end
  end

  test 'validate_empty!' do
    v = Fluent::Counter::Validator.new(:empty)
    success, errors = v.call([])
    assert_empty success
    assert_equal [Fluent::Counter::InvalidParams.new('One or more `params` are required')], errors
  end
end

class CounterArrayValidatorTest < ::Test::Unit::TestCase
  test 'validate_key!' do
    ary = ['key', 100, '_']
    error_expected = [
      { 'code' => 'invalid_params', 'message' => 'The type of `key` should be String' },
      { 'code' => 'invalid_params', 'message' => '`key` is the invalid format' }
    ]
    v = Fluent::Counter::ArrayValidator.new(:key)
    valid_params, errors = v.call(ary)

    assert_equal ['key'], valid_params
    assert_equal error_expected, errors.map(&:to_hash)
  end
end

class CounterHashValidatorTest < ::Test::Unit::TestCase
  test 'validate_name!' do
    hash = [
      { 'name' => 'key' },
      {},
      { 'name' => 10 },
      { 'name' => '_' }
    ]
    error_expected = [
      { 'code' => 'invalid_params', 'message' => '`name` is required' },
      { 'code' => 'invalid_params', 'message' => 'The type of `name` should be String' },
      { 'code' => 'invalid_params', 'message' => '`name` is the invalid format' },
    ]
    v = Fluent::Counter::HashValidator.new(:name)
    success, errors = v.call(hash)

    assert_equal [{ 'name' => 'key' }], success
    assert_equal error_expected, errors.map(&:to_hash)
  end

  test 'validate_value!' do
    hash = [
      { 'value' => 1 },
      { 'value' => -1 },
      {},
      { 'value' => 'str' }
    ]
    error_expected = [
      { 'code' => 'invalid_params', 'message' => '`value` is required' },
      { 'code' => 'invalid_params', 'message' => 'The type of `value` type should be Numeric' },
    ]
    v = Fluent::Counter::HashValidator.new(:value)
    valid_params, errors = v.call(hash)

    assert_equal [{ 'value' => 1 }, { 'value' => -1 }], valid_params
    assert_equal error_expected, errors.map(&:to_hash)
  end

  test 'validate_reset_interval!' do
    hash = [
      { 'reset_interval' => 1 },
      { 'reset_interval' => 1.0 },
      {},
      { 'reset_interval' => -1 },
      { 'reset_interval' => 'str' }
    ]
    error_expected = [
      { 'code' => 'invalid_params', 'message' => '`reset_interval` is required' },
      { 'code' => 'invalid_params', 'message' => '`reset_interval` should be a positive number' },
      { 'code' => 'invalid_params', 'message' => 'The type of `reset_interval` should be Numeric' },
    ]
    v = Fluent::Counter::HashValidator.new(:reset_interval)
    valid_params, errors = v.call(hash)

    assert_equal [{ 'reset_interval' => 1 }, { 'reset_interval' => 1.0 }], valid_params
    assert_equal error_expected.map(&:to_hash), errors.map(&:to_hash)
  end
end
