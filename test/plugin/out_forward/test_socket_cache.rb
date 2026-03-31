require_relative '../../helper'

require 'fluent/plugin/out_forward/socket_cache'
require 'timecop'
require 'socket'

class SocketCacheTest < Test::Unit::TestCase
  sub_test_case 'checkout_or' do
    test 'when given key does not exist' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      sock = mock!.open { 'socket' }.subject
      assert_equal('socket', c.checkout_or('key') { sock.open })
    end

    test 'when given key exists' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      socket = 'socket'
      assert_equal(socket, c.checkout_or('key') { socket })
      c.checkin(socket)

      sock = mock!.open.never.subject
      assert_equal(socket, c.checkout_or('key') { sock.open })
    end

    test 'when given key exists but used by other' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      assert_equal('sock', c.checkout_or('key') { 'sock' })

      new_sock = 'new sock'
      sock = mock!.open { new_sock }.subject
      assert_equal(new_sock, c.checkout_or('key') { sock.open })
    end

    test "when given key's value was expired" do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(0, $log)
      assert_equal('sock', c.checkout_or('key') { 'sock' })

      new_sock = 'new sock'
      sock = mock!.open { new_sock }.subject
      assert_equal(new_sock, c.checkout_or('key') { sock.open })
    end

    test 'reuse same hash object after calling purge_obsolete_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      c.checkout_or('key') { 'socket' }
      c.purge_obsolete_socks

      assert_nothing_raised(NoMethodError) do
        c.checkout_or('key') { 'new socket' }
      end
    end

    test 'discards cached socket after remote close' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      server = TCPServer.open('127.0.0.1', unused_port(protocol: :tcp))
      first_sock = TCPSocket.new('127.0.0.1', server.addr[1])
      first_peer = server.accept

      c.checkout_or('key') { first_sock }
      c.checkin(first_sock)

      first_peer.close
      waiting(5) do
        until IO.select([first_sock], nil, nil, 0)
          sleep 0.01
        end
      end

      second_peer = nil
      second_sock = c.checkout_or('key') do
        sock = TCPSocket.new('127.0.0.1', server.addr[1])
        second_peer = server.accept
        sock
      end

      assert_not_same(first_sock, second_sock)
      assert_true(first_sock.closed?)
    ensure
      c&.clear
      first_peer&.close rescue nil
      second_peer&.close rescue nil
      server&.close rescue nil
    end
  end

  sub_test_case 'checkin' do
    test 'when value exists' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      socket = 'socket'
      c.checkout_or('key') { socket }
      c.checkin(socket)

      assert_equal(socket, c.instance_variable_get(:@available_sockets)['key'].first.sock)
      assert_equal(1, c.instance_variable_get(:@available_sockets)['key'].size)
      assert_equal(0, c.instance_variable_get(:@inflight_sockets).size)
    end

    test 'when value does not exist' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      c.checkout_or('key') { 'sock' }
      c.checkin('other sock')

      assert_equal(0, c.instance_variable_get(:@available_sockets)['key'].size)
      assert_equal(1, c.instance_variable_get(:@inflight_sockets).size)
    end
  end

  test 'revoke' do
    c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
    socket = 'socket'
    c.checkout_or('key') { socket }
    c.revoke(socket)

    assert_equal(1, c.instance_variable_get(:@inactive_sockets).size)
    assert_equal(0, c.instance_variable_get(:@inflight_sockets).size)
    assert_equal(0, c.instance_variable_get(:@available_sockets)['key'].size)

    sock = mock!.open { 1 }.subject
    assert_equal(1, c.checkout_or('key') { sock.open })
  end

  sub_test_case 'clear' do
    test 'when value is in available_sockets' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      m = mock!.close { 'closed' }.subject
      m2 = mock!.close { 'closed' }.subject
      m3 = mock!.close { 'closed' }.subject
      c.checkout_or('key') { m }
      c.revoke(m)
      c.checkout_or('key') { m2 }
      c.checkin(m2)
      c.checkout_or('key2') { m3 }

      assert_equal(1, c.instance_variable_get(:@inflight_sockets).size)
      assert_equal(1, c.instance_variable_get(:@available_sockets)['key'].size)
      assert_equal(1, c.instance_variable_get(:@inactive_sockets).size)

      c.clear
      assert_equal(0, c.instance_variable_get(:@inflight_sockets).size)
      assert_equal(0, c.instance_variable_get(:@available_sockets)['key'].size)
      assert_equal(0, c.instance_variable_get(:@inactive_sockets).size)
    end
  end

  sub_test_case 'purge_obsolete_socks' do
    def teardown
      Timecop.return
    end

    test 'delete key in inactive_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      sock = mock!.close { 'closed' }.subject
      c.checkout_or('key') { sock }
      c.revoke(sock)
      assert_false(c.instance_variable_get(:@inactive_sockets).empty?)

      c.purge_obsolete_socks
      assert_true(c.instance_variable_get(:@inactive_sockets).empty?)
    end

    test 'move key from available_sockets to inactive_sockets' do
      Timecop.freeze(Time.parse('2016-04-13 14:00:00 +0900'))

      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      sock = mock!.close { 'closed' }.subject
      sock2 = mock!.close.never.subject
      stub(sock).inspect
      stub(sock2).inspect

      c.checkout_or('key') { sock }
      c.checkin(sock)

      # wait timeout
      Timecop.freeze(Time.parse('2016-04-13 14:00:11 +0900'))
      c.checkout_or('key') { sock2 }

      assert_equal(1, c.instance_variable_get(:@inflight_sockets).size)
      assert_equal(sock2, c.instance_variable_get(:@inflight_sockets).values.first.sock)

      c.purge_obsolete_socks

      assert_equal(1, c.instance_variable_get(:@inflight_sockets).size)
      assert_equal(sock2, c.instance_variable_get(:@inflight_sockets).values.first.sock)
    end

    test 'should not purge just after checkin and purge after timeout' do
      Timecop.freeze(Time.parse('2016-04-13 14:00:00 +0900'))

      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      sock = mock!.close.never.subject
      stub(sock).inspect
      c.checkout_or('key') { sock }

      Timecop.freeze(Time.parse('2016-04-13 14:00:11 +0900'))
      c.checkin(sock)

      assert_equal(1, c.instance_variable_get(:@available_sockets).size)
      c.purge_obsolete_socks
      assert_equal(1, c.instance_variable_get(:@available_sockets).size)

      Timecop.freeze(Time.parse('2016-04-13 14:00:22 +0900'))
      assert_equal(1, c.instance_variable_get(:@available_sockets).size)
      c.purge_obsolete_socks
      assert_equal(0, c.instance_variable_get(:@available_sockets).size)
    end
  end
end
