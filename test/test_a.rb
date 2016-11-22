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

module ServerEngine
  module SocketManager
    def self.recv_peer(peer)
      STDERR.puts "#{__LINE__}: running..."
      len = peer.read(4).unpack('N').first
      STDERR.puts "#{__FILE__}:#{__LINE__}: #{len} running..."
      data = peer.read(len)
      STDERR.puts "#{__FILE__}:#{__LINE__}: #{data} running..."
      ret = Marshal.load(data)
      STDERR.puts "#{__FILE__}:#{__LINE__}: #{ret} running..."
      ret
    ensure
      STDERR.puts "#{__FILE__}:#{__LINE__}: #{ret} running..."
    end

    class Client
      def initialize(path)
        @path = path
      end

      def listen(proto, bind, port)
        STDERR.puts "#{__LINE__}: running..."
        bind_ip = IPAddr.new(IPSocket.getaddress(bind))
        STDERR.puts "#{__LINE__}: running..."
        family = bind_ip.ipv6? ? Socket::AF_INET6 : Socket::AF_INET
        STDERR.puts "#{__LINE__}: running..."

        listen_method = case proto
                        when :tcp then :listen_tcp
                        when :udp then :listen_udp
                        else
                          raise ArgumentError, "unknown protocol: #{proto}"
                        end
        STDERR.puts "#{__LINE__}: running..."
        peer = connect_peer(@path)
        STDERR.puts "#{__LINE__}: running..."
        begin
          SocketManager.send_peer(peer, [Process.pid, listen_method, bind, port])
          meth = SocketManager.method(:recv_peer)
          STDERR.puts "#{__LINE__}: #{meth} #{meth.source_location} running..."
          trace = TracePoint.new do |tp|
            if tp.event == :raise
              p [tp.path, tp.lineno, tp.defined_class, tp.method_id, tp.event, tp.raised_exception]
            else
              p [tp.path, tp.lineno, tp.defined_class, tp.method_id, tp.event]
            end
          end
          trace.enable
          res = SocketManager.recv_peer(peer)
        STDERR.puts "#{__LINE__}: #{res} running..."
        trace.disable
        STDERR.puts "#{__LINE__}: running..."
        STDERR.puts "#{__LINE__}: running..."
        STDERR.puts "#{__LINE__}: running..."
        STDERR.puts "#{__LINE__}: running..."
          if res.is_a?(Exception)
        STDERR.puts "#{__LINE__}: running..."
            raise res
          else
        STDERR.puts "#{__LINE__}: running..."
            return send(:recv, family, proto, peer, res)
          end
        ensure
        STDERR.puts "#{__LINE__}: running..."
          peer.close
        end
      end
    end
  end
  module RbWinSock
    extern "void rb_sys_fail(const char *)"
    def self.raise_last_error(name)
      rb_sys_fail(name)
    end
  end
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
