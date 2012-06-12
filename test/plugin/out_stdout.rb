require 'fluent/test'

class StdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::StdoutOutput).configure(conf)
  end

  def test_configure
    d = create_driver
  end

  # def test_emit
  # end
end

