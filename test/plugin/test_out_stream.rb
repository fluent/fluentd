require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_stream'

module StreamOutputTest
  def setup
    Fluent::Test.setup
  end

  def test_write
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    expect = ["test",
        [time,{"a"=>1}].to_msgpack +
        [time,{"a"=>2}].to_msgpack
      ].to_msgpack

    result = d.run
    assert_equal(expect, result)
  end

  def create_driver(klass, conf)
    Fluent::Test::BufferedOutputTestDriver.new(klass) do
      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end
end

class TcpOutputTest < Test::Unit::TestCase
  include StreamOutputTest

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    host 127.0.0.1
    send_timeout 51
  ]

  def create_driver(conf=CONFIG)
    super(Fluent::TcpOutput, conf)
  end

  def test_configure
    d = create_driver
    assert_equal PORT, d.instance.port
    assert_equal '127.0.0.1', d.instance.host
    assert_equal 51, d.instance.send_timeout
  end
end

class UnixOutputTest < Test::Unit::TestCase
  include StreamOutputTest

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/out_unix#{ENV['TEST_ENV_NUMBER']}"
  CONFIG = %[
    path #{TMP_DIR}/unix
    send_timeout 52
  ]

  def create_driver(conf=CONFIG)
    super(Fluent::UnixOutput, conf)
  end

  def test_configure
    d = create_driver
    assert_equal "#{TMP_DIR}/unix", d.instance.path
    assert_equal 52, d.instance.send_timeout
  end
end

