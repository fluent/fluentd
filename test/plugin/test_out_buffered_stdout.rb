require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_buffered_stdout'

class BufferedStdoutOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::BufferedStdoutOutput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'json', d.instance.output_type
  end

  def test_configure_output_type
    d = create_driver(CONFIG + "\noutput_type json")
    assert_equal 'json', d.instance.output_type

    d = create_driver(CONFIG + "\noutput_type hash")
    assert_equal 'hash', d.instance.output_type

    assert_raise(Fluent::ConfigError) do
      d = create_driver(CONFIG + "\noutput_type foo")
    end
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    d.expect_format %[{\"a\":1}\n{\"a\":2}\n]

    d.run
  end
end
