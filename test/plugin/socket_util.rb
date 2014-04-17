require 'fluent/test'
require 'helper'
require 'fluent/plugin/socket_util'

class SocketUtilTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  HOST = '127.0.0.1'
  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind #{HOST}
  ]
  TIME = Time.parse("2011-01-02 13:14:15 UTC").to_i

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ForwardInput).configure(conf)
  end

  def create_and_send(data, opts = {})
    io = Fluent::SocketUtil.create_tcp_socket(HOST, PORT, opts)
    begin
      io.write data
    ensure
      io.close
    end
  end

  def open_and_send(data, opts = {})
    Fluent::SocketUtil.open_tcp_socket(HOST, PORT, opts) {|io| io.write data }
  end

  def test_create_tcp_socket
    d = create_driver

    d.expect_emit "tag1", TIME, {"a"=>1}
    d.expect_emit "tag2", TIME, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        create_and_send [tag, time, record].to_msgpack
      }
      sleep 0.5
    end
  end

  def test_open_tcp_socket
    d = create_driver

    d.expect_emit "tag1", TIME, {"a"=>1}
    d.expect_emit "tag2", TIME, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        open_and_send [tag, time, record].to_msgpack
      }
      sleep 0.5
    end
  end

  def test_tcp_socket_timeout
    d = create_driver

    samples = []
    samples << ["tag1", TIME, {"a"=>1}]
    samples << ["tag2", TIME, {"a"=>2}]

    stub(IO).select { nil } # IO.select returns nil if timeout

    d.run do
      samples.each {|tag,time,record|
        assert_raise(Timeout::Error) {
          create_and_send [tag, time, record].to_msgpack, connect_timeout: 0.5
        }
      }
    end
  end
end
