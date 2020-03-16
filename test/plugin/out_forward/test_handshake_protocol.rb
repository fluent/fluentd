require_relative '../../helper'
require 'flexmock/test_unit'

require 'fluent/plugin/out_forward'
require 'fluent/plugin/out_forward/handshake_protocol'
require 'fluent/plugin/out_forward/connection_manager'

class HandshakeProtocolTest < Test::Unit::TestCase
  sub_test_case '#invok when helo state' do
    test 'sends PING message and change state to pingpong' do
      hostname = 'hostname'
      handshake = Fluent::Plugin::ForwardOutput::HandshakeProtocol.new(log: $log, hostname: hostname, shared_key: 'shared_key', password: nil, username: nil)
      ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:helo)

      sock = StringIO.new('')
      handshake.invoke(sock, ri, ['HELO', {}])

      assert_equal(ri.state, :pingpong)
      Fluent::MessagePackFactory.msgpack_unpacker.feed_each(sock.string) do |ping|
        assert_equal(ping.size, 6)
        assert_equal(ping[0], 'PING')
        assert_equal(ping[1], hostname)
        assert(ping[2].is_a?(String)) # content is hashed value
        assert(ping[3].is_a?(String)) # content is hashed value
        assert_equal(ping[4], '')
        assert_equal(ping[5], '')
      end
    end

    test 'returns PING message with username if auth exists' do
      hostname = 'hostname'
      username = 'username'
      pass = 'pass'
      handshake = Fluent::Plugin::ForwardOutput::HandshakeProtocol.new(log: $log, hostname: hostname, shared_key: 'shared_key', password: pass, username: username)
      ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:helo)

      sock = StringIO.new('')
      handshake.invoke(sock, ri, ['HELO', { 'auth' => 'auth' }])

      assert_equal(ri.state, :pingpong)
      Fluent::MessagePackFactory.msgpack_unpacker.feed_each(sock.string) do |ping|
        assert_equal(ping.size, 6)
        assert_equal(ping[0], 'PING')
        assert_equal(ping[1], hostname)
        assert(ping[2].is_a?(String)) # content is hashed value
        assert(ping[3].is_a?(String)) # content is hashed value
        assert_equal(ping[4], username)
        assert_not_equal(ping[5], pass) # should be hashed
      end
    end

    data(
      lack_of_elem: ['HELO'],
      wrong_message: ['HELLO!', {}],
    )
    test 'raises an error when message is' do |msg|
      handshake = Fluent::Plugin::ForwardOutput::HandshakeProtocol.new(log: $log, hostname: 'hostname', shared_key: 'shared_key', password: nil, username: nil)
      ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:helo)

      sock = StringIO.new('')
      assert_raise(Fluent::Plugin::ForwardOutput::HeloError) do
        handshake.invoke(sock, ri, msg)
      end

      assert_equal(ri.state, :helo)
    end
  end

  sub_test_case '#invok when pingpong state' do
    test 'sends PING message and change state to pingpong' do
      handshake = Fluent::Plugin::ForwardOutput::HandshakeProtocol.new(log: $log, hostname: 'hostname', shared_key: 'shared_key', password: nil, username: nil)
      handshake.instance_variable_set(:@shared_key_salt, 'ce1897b0d3dbd76b90d7fb96010dcac3') # to fix salt

      ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:pingpong, '', '')
      handshake.invoke(
        '',
        ri,
        # 40a3.... = Digest::SHA512.new.update('ce1897b0d3dbd76b90d7fb96010dcac3').update('client_hostname').update('').update('shared_key').hexdigest
        ['PONG', true, '', 'client_hostname', '40a3c5943cc6256e0c5dcf176e97db3826b0909698c330dc8e53d15af63efb47e030d113130255dd6e7ced5176d2999cc2e02a44852d45152503af317b73b33f']
      )
      assert_equal(ri.state, :established)
    end

    test 'raises an error when password and username are nil if auth exists' do
      handshake = Fluent::Plugin::ForwardOutput::HandshakeProtocol.new(log: $log, hostname: 'hostname', shared_key: 'shared_key', password: nil, username: nil)
      ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:helo)

      assert_raise(Fluent::Plugin::ForwardOutput::PingpongError.new('username and password are required')) do
        handshake.invoke('', ri, ['HELO', { 'auth' => 'auth' }])
      end
    end

    data(
      lack_of_elem: ['PONG', true, '', 'client_hostname'],
      wrong_message: ['WRONG_PONG', true, '', 'client_hostname', '40a3c5943cc6256e0c5dcf176e97db3826b0909698c330dc8e53d15af63efb47e030d113130255dd6e7ced5176d2999cc2e02a44852d45152503af317b73b33f'],
      error_by_server: ['PONG', false, 'error', 'client_hostname', '40a3c5943cc6256e0c5dcf176e97db3826b0909698c330dc8e53d15af63efb47e030d113130255dd6e7ced5176d2999cc2e02a44852d45152503af317b73b33f'],
      same_hostname_as_server: ['PONG', true, '', 'hostname', '40a3c5943cc6256e0c5dcf176e97db3826b0909698c330dc8e53d15af63efb47e030d113130255dd6e7ced5176d2999cc2e02a44852d45152503af317b73b33f'],
      wrong_key: ['PONG', true, '', 'hostname', 'wrong_key'],
    )
    test 'raises an error when message is' do |msg|
      handshake = Fluent::Plugin::ForwardOutput::HandshakeProtocol.new(log: $log, hostname: 'hostname', shared_key: 'shared_key', password: '', username: '')
      handshake.instance_variable_set(:@shared_key_salt, 'ce1897b0d3dbd76b90d7fb96010dcac3') # to fix salt

      ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:pingpong, '', '')
      assert_raise(Fluent::Plugin::ForwardOutput::PingpongError) do
        handshake.invoke('', ri, msg)
      end

      assert_equal(ri.state, :pingpong)
    end
  end
end
