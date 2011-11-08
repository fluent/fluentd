require 'fluent/test'

module StreamInputTest
  def setup
    Fluent::Test.setup
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, 0, record].to_msgpack
      }
      sleep 0.5
    end
  end

  def test_message
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, time, record].to_msgpack
      }
      sleep 0.5
    end
  end

  def test_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = []
      d.expected_emits.each {|tag,time,record|
        entries << [time, record]
      }
      send_data ["tag1", entries].to_msgpack
      sleep 0.5
    end
  end

  def test_packed_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data ["tag1", entries].to_msgpack
      sleep 0.5
    end
  end

  def test_message_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, time, record].to_json
      }
      sleep 0.5
    end
  end

  def create_driver(klass, conf)
    Fluent::Test::InputTestDriver.new(klass).configure(conf)
  end

  def send_data(data)
    io = connect
    begin
      io.write data
    ensure
      io.close
    end
  end
end

class TcpInputTest < Test::Unit::TestCase
  include StreamInputTest

  CONFIG = %[
    port 13998
    bind 127.0.0.1
  ]

  def create_driver(conf=CONFIG)
    super(Fluent::TcpInput, conf)
  end

  def test_configure
    d = create_driver
    assert_equal 13998, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
  end

  def connect
    TCPSocket.new('127.0.0.1', 13998)
  end
end

class UnixInputTest < Test::Unit::TestCase
  include StreamInputTest

  TMP_DIR = File.dirname(__FILE__) + "/../tmp"

  CONFIG = %[
    path #{TMP_DIR}/unix
  ]

  def create_driver(conf=CONFIG)
    super(Fluent::UnixInput, conf)
  end

  def test_configure
    d = create_driver
    assert_equal "#{TMP_DIR}/unix", d.instance.path
  end

  def connect
    UNIXSocket.new("#{TMP_DIR}/unix")
  end
end

