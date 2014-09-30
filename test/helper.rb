# simplecov must be loaded before any of target code
if ENV['SIMPLE_COV']
  require 'simplecov'
  unless SimpleCov.running
    SimpleCov.start do
      add_filter '/test/'
      add_filter '/gems/'
    end
  end
end

require 'test/unit'
require 'fileutils'
require 'fluent/log'
require 'fluent/test'
require 'rr'

unless defined?(Test::Unit::AssertionFailedError)
  class Test::Unit::AssertionFailedError < StandardError
  end
end

def unused_port
  s = TCPServer.open(0)
  port = s.addr[1]
  s.close
  port
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

$log = Fluent::Log.new(Fluent::Test::DummyLogDevice.new, Fluent::Log::LEVEL_WARN)
