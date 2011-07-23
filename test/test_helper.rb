require 'test/unit'
$LOAD_PATH << File.dirname(__FILE__)+"/../lib"
require 'fileutils'

class Test::Unit::TestCase
  #class << self
  #  alias_method :it, :test
  #end
	def self.it(name, &block)
		define_method("test_#{name}", &block)
	end
end

