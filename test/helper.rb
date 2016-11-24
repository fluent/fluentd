# simplecov must be loaded before any of target code
if ENV['SIMPLE_COV']
  require 'simplecov'
  if defined?(SimpleCov::SourceFile)
    mod = SimpleCov::SourceFile
    def mod.new(*args, &block)
      m = allocate
      m.instance_eval do
        begin
          initialize(*args, &block)
        rescue Encoding::UndefinedConversionError
          @src = "".force_encoding('UTF-8')
        end
      end
      m
    end
  end
  unless SimpleCov.running
    SimpleCov.start do
      add_filter '/test/'
      add_filter '/gems/'
    end
  end
end

# Some tests use Hash instead of Element for configure.
# We should rewrite these tests in the future and remove this ad-hoc code
class Hash
  def corresponding_proxies
    @corresponding_proxies ||= []
  end

  def to_masked_element
    self
  end
end

require 'rr'
require 'test/unit'
require 'test/unit/rr'
require 'fileutils'
require 'fluent/config/element'
require 'fluent/log'
require 'fluent/test'
require 'fluent/test/helpers'
require 'fluent/plugin/base'
require 'fluent/plugin_id'
require 'fluent/plugin_helper'
require 'fluent/msgpack_factory'
require 'fluent/time'
require 'serverengine'

module Fluent
  module Plugin
    class TestBase < Base
      # a base plugin class, but not input nor output
      # mainly for helpers and owned plugins
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin
    end
  end
end

unless defined?(Test::Unit::AssertionFailedError)
  class Test::Unit::AssertionFailedError < StandardError
  end
end

include Fluent::Test::Helpers

def unused_port(num = 1, protocol: :tcp, bind: "0.0.0.0")
  case protocol
  when :tcp
    unused_port_tcp(num)
  when :udp
    unused_port_udp(num, bind: bind)
  else
    raise ArgumentError, "unknown protocol: #{protocol}"
  end
end

def unused_port_tcp(num = 1)
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

PORT_RANGE_AVAILABLE = (1024...65535)

def unused_port_udp(num = 1, bind: "0.0.0.0")
  family = IPAddr.new(IPSocket.getaddress(bind)).ipv4? ? ::Socket::AF_INET : ::Socket::AF_INET6
  ports = []
  sockets = []
  while ports.size < num
    port = rand(PORT_RANGE_AVAILABLE)
    u = UDPSocket.new(family)
    if (u.bind(bind, port) rescue nil)
      ports << port
      sockets << u
    else
      u.close
    end
  end
  sockets.each{|s| s.close }
  if num == 1
    return ports.first
  else
    return *ports
  end
end

def waiting(seconds, logs: nil, plugin: nil)
  begin
    Timeout.timeout(seconds) do
      yield
    end
  rescue Timeout::Error
    if logs
      STDERR.print(*logs)
    elsif plugin
      STDERR.print(*plugin.log.out.logs)
    end
    raise
  end
end

def ipv6_enabled?
  require 'socket'

  begin
    TCPServer.open("::1", 0)
    true
  rescue
    false
  end
end

dl_opts = {}
dl_opts[:log_level] = ServerEngine::DaemonLogger::WARN
logdev = Fluent::Test::DummyLogDevice.new
logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
$log ||= Fluent::Log.new(logger)
