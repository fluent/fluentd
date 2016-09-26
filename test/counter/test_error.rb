require_relative '../helper'
require 'fluent/counter/error'

class CounterErrorTest < ::Test::Unit::TestCase
  setup do
    @message = 'error message'
  end

  test 'invalid_params' do
    error = Fluent::Counter::InvalidParams.new(@message)
    expected = { 'code' => 'invalid_params', 'message' => @message }
    assert_equal expected, error.to_hash
  end

  test 'unknown_key' do
    error = Fluent::Counter::UnknownKey.new(@message)
    expected = { 'code' => 'unknown_key', 'message' => @message }
    assert_equal expected, error.to_hash
  end

  test 'parse_error' do
    error = Fluent::Counter::ParseError.new(@message)
    expected = { 'code' => 'parse_error', 'message' => @message }
    assert_equal expected, error.to_hash
  end

  test 'method_not_found' do
    error = Fluent::Counter::MethodNotFound.new(@message)
    expected = { 'code' => 'method_not_found', 'message' => @message }
    assert_equal expected, error.to_hash
  end

  test 'invalid_request' do
    error = Fluent::Counter::InvalidRequest.new(@message)
    expected = { 'code' => 'invalid_request', 'message' => @message }
    assert_equal expected, error.to_hash
  end

  test 'internal_server_error' do
    error = Fluent::Counter::InternalServerError.new(@message)
    expected = { 'code' => 'internal_server_error', 'message' => @message }
    assert_equal expected, error.to_hash
  end
end
