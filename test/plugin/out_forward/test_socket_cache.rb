require_relative '../../helper'

require 'fluent/plugin/out_forward/socket_cache'

class SocketCacheTest < Test::Unit::TestCase
  sub_test_case 'fetch_or' do
    test 'when gived key does not exist' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      sock = mock!.open { 1 }.subject
      assert_equal(1, c.fetch_or { sock.open })
    end

    test 'when given key exists' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      assert_equal(1, c.fetch_or { 1 })

      sock = dont_allow(mock!).open
      assert_equal(1, c.fetch_or { sock.open })
    end

    test "when given key's value was expired" do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(0, $log)
      assert_equal(1, c.fetch_or { 1 })

      sock = mock!.open { 1 }.subject
      assert_equal(1, c.fetch_or { sock.open })
    end
  end

  test 'revoke' do
    c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
    c.fetch_or { 1 }
    c.revoke

    sock = mock!.open { 1 }.subject
    assert_equal(1, c.fetch_or { sock.open })
  end

  test 'revoke_by_value' do
    c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
    c.fetch_or { 1 }
    c.revoke_by_value(1)

    sock = mock!.open { 1 }.subject
    assert_equal(1, c.fetch_or { sock.open })
  end

  sub_test_case 'dec_ref' do
    test 'when value exists in active_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      c.fetch_or { 1 }
      c.dec_ref

      assert_equal(0, c.instance_variable_get(:@active_socks)[Thread.current.object_id].ref)
    end

    test 'when value exists in inactive_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      c.fetch_or { 1 }
      c.revoke
      c.dec_ref
      assert_equal(-1, c.instance_variable_get(:@inactive_socks)[Thread.current.object_id].ref)
    end
  end

  sub_test_case 'dec_ref_by_value' do
    test 'when value exists in active_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      c.fetch_or { 1 }
      c.dec_ref_by_value(1)

      assert_equal(0, c.instance_variable_get(:@active_socks)[Thread.current.object_id].ref)
    end

    test 'when value exists in inactive_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      c.fetch_or { 1 }
      c.revoke
      c.dec_ref_by_value(1)
      assert_equal(-1, c.instance_variable_get(:@inactive_socks)[Thread.current.object_id].ref)
    end
  end

  sub_test_case 'clear' do
    test 'when value is in active_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      m = mock!.close { 'closed' }.subject
      c.fetch_or { m }
      assert_true(!c.instance_variable_get(:@active_socks).empty?)

      c.clear
      assert_true(c.instance_variable_get(:@active_socks).empty?)
    end

    test 'when value is in inactive_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      m = mock!.close { 'closed' }.subject
      c.fetch_or { m }
      c.revoke
      assert_true(!c.instance_variable_get(:@inactive_socks).empty?)

      c.clear
      assert_true(c.instance_variable_get(:@active_socks).empty?)
    end
  end

  sub_test_case 'purge_obsolete_socks' do
    test 'delete key in inactive_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      m = mock!.close { 'closed' }.subject
      c.fetch_or { m }
      c.revoke
      assert_true(!c.instance_variable_get(:@inactive_socks).empty?)

      c.purge_obsolete_socks
      assert_true(c.instance_variable_get(:@active_socks).empty?)
    end

    test 'move key from active_socks to inactive_socks' do
      c = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
      m = dont_allow(mock!).close
      stub(m).inspect         # for log
      c.fetch_or { m }
      assert_true(!c.instance_variable_get(:@active_socks).empty?)
      assert_true(c.instance_variable_get(:@inactive_socks).empty?)

      c.purge_obsolete_socks
      assert_true(!c.instance_variable_get(:@active_socks).empty?)
      assert_true(c.instance_variable_get(:@inactive_socks).empty?)
    end
  end
end
