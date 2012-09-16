require 'fluent/test'
require 'net/http'

class SyslogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    port 9912
    bind 127.0.0.1
    tag t1
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::SyslogInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 9912, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 't1', d.instance.tag
  end

  def test_emit
    d = create_driver

    time = Time.parse("Jun 22 00:00:01").to_i
    Fluent::Engine.now = time

    d.expect_emit "t1.kern.warn", time, {
      "host"    => "some-host",
      "ident"   => "kernel",
      "pid"     => "0",
      "message" => "AirPort: Link Up on en0"
    }


    d.run do
      socket = UDPSocket.new
      socket.connect("127.0.0.1", 9912)
      socket.send("<4>Jun 22 00:00:01 some-host kernel[0] <Debug>: AirPort: Link Up on en0", 0)
      socket.close

      sleep 0.5
    end

  end
end
