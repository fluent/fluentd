require 'test/unit'
require 'fileutils'
require 'fluent/log'
require 'rr'

class Test::Unit::TestCase
  include RR::Adapters::TestUnit
end

$log = Fluent::Log.new(STDOUT, Fluent::Log::LEVEL_WARN)

