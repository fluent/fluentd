#!/usr/bin/env ruby
$: << File.expand_path('../../lib', __FILE__)
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

  def test_foo
    @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
    if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
    @socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
    ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s

    @d = Dummy.new
    @d.start
    @d.after_start
    m, proto, kwargs = *[:server_create, :tcp, {}]
    begin
      d2 = Dummy.new; d2.start; d2.after_start
      assert_nothing_raised do
        @d.__send__(m, :myserver, PORT, proto: proto, shared: false, **kwargs){|x| x }
      end
      STDERR.puts "#{__LINE__}: #{d2} #{m} #{proto} #{kwargs} running..."
      d2.server_create(:myserver, PORT, proto: proto, **kwargs){|x| x }
      STDERR.puts "#{__LINE__}: running..."
    ensure
      STDERR.puts "#{__LINE__}: running..."
      d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
    end
    STDERR.puts "#{__LINE__}: running..."
  end
end
