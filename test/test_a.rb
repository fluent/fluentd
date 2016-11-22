#!/usr/bin/env ruby
require 'serverengine'
$: << File.expand_path('../../lib', __FILE__)
require 'fluent/plugin_id'
require 'fluent/log'
require 'fluent/env'
require 'fluent/engine'
require 'fluent/plugin_helper'
require 'fluent/plugin_helper/server'
require 'fluent/plugin/base'


class Dummy < Fluent::Plugin::Base
  include Fluent::PluginId
  include Fluent::PluginLoggerMixin
  include Fluent::PluginHelper::Mixin
  helpers :server
end

def unused_port(num = 1)
  ports = []
  sockets = []
  num.times do
    s = TCPServer.open(0)
    sockets << s
    ports << s.addr[1]
  end
  sockets.each{|s| s.close }
  if num == 1
    return ports.first
  else
    return *ports
  end
end
logger = ServerEngine::DaemonLogger.new(STDERR)
$log ||= Fluent::Log.new(logger)

PORT = unused_port

@socket_manager_path = ServerEngine::SocketManager::Server.generate_path
if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
  FileUtils.rm_f @socket_manager_path
end
@socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s

d = Dummy.new
d.start
d.after_start
begin
  d2 = Dummy.new; d2.start; d2.after_start
  d.server_create(:myserver, PORT, proto: :tcp, shared: false){|x| x }
  STDERR.puts "#{__LINE__}: #{d2} running..."
  meth = d2.method(:server_create)
  p meth
  p meth.source_location
  d2.server_create(:myserver, PORT, proto: :tcp){|x| x }
  STDERR.puts "#{__LINE__}: running..."
ensure
  STDERR.puts "#{__LINE__}: running..."
  d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
end
STDERR.puts "#{__LINE__}: running..."
