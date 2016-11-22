#!/usr/bin/env ruby
require_relative 'helper'
require 'fluent/plugin_helper/server'
require 'fluent/plugin/base'
require 'timeout'

require 'serverengine'
require 'fileutils'

class AToFailAtFirstTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :server
  end

  PORT = unused_port

  setup do
    @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
    if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
    @socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
    ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s

    @d = Dummy.new
    @d.start
    @d.after_start
  end

  teardown do
    @d.stopped? || @d.stop
    @d.before_shutdown? || @d.before_shutdown
    @d.shutdown? || @d.shutdown
    @d.after_shutdown? || @d.after_shutdown
    @d.closed? || @d.close
    @d.terminated? || @d.terminate

    @socket_manager_server.close
    if @socket_manager_server.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
  end

  sub_test_case '#server_create and #server_create_connection' do
    data(
      'server_create tcp' => [:server_create, :tcp, {}],
      'server_create udp' => [:server_create, :udp, {max_bytes: 128}],
      # 'server_create tls' => [:server_create, :tls, {}],
      # 'server_create unix' => [:server_create, :unix, {}],
      'server_create_connection tcp' => [:server_create, :tcp, {}],
      # 'server_create_connection tls' => [:server_create, :tls, {}],
      # 'server_create_connection unix' => [:server_create, :unix, {}],
    )
    test 'cannot create 2 or more servers using same bind address and port if shared option is false' do |(m, proto, kwargs)|
      STDERR.puts "#{__LINE__}: running..."
      begin
        d2 = Dummy.new; d2.start; d2.after_start
      STDERR.puts "#{__LINE__}: running..."

      STDERR.puts "#{__LINE__}: running..."
        assert_nothing_raised do
      STDERR.puts "#{__LINE__}: running..."
          @d.__send__(m, :myserver, PORT, proto: proto, shared: false, **kwargs){|x| x }
      STDERR.puts "#{__LINE__}: running..."
        end
      STDERR.puts "#{__LINE__}: running..."
          d2.__send__(m, :myserver, PORT, proto: proto, **kwargs){|x| x }
      STDERR.puts "#{__LINE__}: running..."
        require 'fiddle'
      STDERR.puts "#{__LINE__}: running..."
        Fiddle::Function.new(0,[],Fiddle::TYPE_VOID).call
      STDERR.puts "#{__LINE__}: running..."
      ensure
      STDERR.puts "#{__LINE__}: running..."
        d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
      STDERR.puts "#{__LINE__}: running..."
      end
      STDERR.puts "#{__LINE__}: running..."
    end
  end
end
