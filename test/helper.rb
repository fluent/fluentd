require 'test/unit'
require 'fileutils'
require 'fluent/log'

class Test::Unit::TestCase
end

$log = Fluent::Log.new(STDOUT, Fluent::Log::LEVEL_WARN)

