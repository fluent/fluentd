require 'test/unit'
require 'fileutils'
require 'fluent/log'
require 'rr'

unless defined?(Test::Unit::AssertionFailedError)
  class Test::Unit::AssertionFailedError < StandardError
  end
end

class Test::Unit::TestCase
  include RR::Adapters::TestUnit
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

$log = Fluent::Log.new(STDOUT, Fluent::Log::LEVEL_WARN)
