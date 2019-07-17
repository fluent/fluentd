require_relative '../../helper'
require 'fluent/test/driver/output'
require 'flexmock/test_unit'

require 'fluent/plugin/out_forward'
require 'fluent/plugin/out_forward/connection_manager'
require 'fluent/plugin/out_forward/socket_cache'

class ConnectionManager < Test::Unit::TestCase
  sub_test_case '#connect' do
    sub_test_case 'when socket_cache is nil' do
      test 'creates socket and does not close when block is not given' do
        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: false,
          connection_factory: -> (_, _, _) { sock = 'sock'; mock(sock).close.never; sock },
          socket_cache: nil,
        )

        mock.proxy(cm).connect_keepalive(anything).never
        sock, ri = cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: false)
        assert_equal(sock, 'sock')
        assert_equal(ri.state, :established)
      end

      test 'creates socket and calls close when block is given' do
        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: false,
          connection_factory: -> (_, _, _) {
            sock = 'sock'
            mock(sock).close.once
            mock(sock).close_write.once
            sock
          },
          socket_cache: nil,
        )

        mock.proxy(cm).connect_keepalive(anything).never
        cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: false) do |sock, ri|
          assert_equal(sock, 'sock')
          assert_equal(ri.state, :established)
        end
      end

      test 'when secure is true, state is helo' do
        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: true,
          connection_factory: -> (_, _, _) { sock = 'sock'; mock(sock).close.never; sock },
          socket_cache: nil,
        )

        mock.proxy(cm).connect_keepalive(anything).never
        sock, ri = cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: false)
        assert_equal(sock, 'sock')
        assert_equal(ri.state, :helo)
      end

      test 'when require_ack is true' do
        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: false,
          connection_factory: -> (_, _, _) {
            sock = 'sock'
            mock(sock).close.never
            mock(sock).close_write.never
            sock
          },
          socket_cache: nil,
        )

        mock.proxy(cm).connect_keepalive(anything).never

        cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: true) do |sock, ri|
          assert_equal(sock, 'sock')
          assert_equal(ri.state, :established)
        end
      end
    end

    sub_test_case 'when socket_cache exists' do
      test 'calls connect_keepalive' do
        cache = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
        mock(cache).dec_ref.never

        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: false,
          connection_factory: -> (_, _, _) { sock = 'sock'; mock(sock).close.never; sock },
          socket_cache: cache,
        )

        mock.proxy(cm).connect_keepalive(host: 'host', port: 1234, hostname: 'hostname', require_ack: false).once

        sock, ri = cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: false)
        assert_equal(sock, 'sock')
        assert_equal(ri.state, :established)
      end

      test 'calls connect_keepalive and closes socket with block' do
        cache = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
        mock(cache).dec_ref.once

        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: false,
          connection_factory: -> (_, _, _) { sock = 'sock'; mock(sock); sock },
          socket_cache: cache,
        )

        mock.proxy(cm).connect_keepalive(host: 'host', port: 1234, hostname: 'hostname', require_ack: false).once

        cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: false) do |sock, ri|
          assert_equal(sock, 'sock')
          assert_equal(ri.state, :established)
        end
      end

      test 'does not call dec_ref when require_ack is true' do
        cache = Fluent::Plugin::ForwardOutput::SocketCache.new(10, $log)
        mock(cache).dec_ref.never

        cm = Fluent::Plugin::ForwardOutput::ConnectionManager.new(
          log: $log,
          secure: false,
          connection_factory: -> (_, _, _) {
            sock = 'sock'
            mock(sock).close.never
            mock(sock).close_write.never
            sock
          },
          socket_cache: cache,
        )

        mock.proxy(cm).connect_keepalive(host: 'host', port: 1234, hostname: 'hostname', require_ack: true).once
        cm.connect(host: 'host', port: 1234, hostname: 'hostname', require_ack: true) do |sock, ri|
          assert_equal(sock, 'sock')
          assert_equal(ri.state, :established)
        end
      end
    end
  end
end
