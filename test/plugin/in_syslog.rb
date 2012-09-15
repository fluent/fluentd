require 'fluent/test'

class SyslogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    port 9911
    bind 127.0.0.1
    tag syslog
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::SyslogInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 9911, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
  end

  def test_time_format
    d = create_driver

    tests = [
      {'msg' => '<6>Sep 11 00:00:00 localhost logger: foo', 'expected' => Time.strptime('Sep 11 00:00:00', '%b %d %H:%M:%S').to_i},
      {'msg' => '<6>Sep  1 00:00:00 localhost logger: foo', 'expected' => Time.strptime('Sep  1 00:00:00', '%b  %d %H:%M:%S').to_i},
    ]

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', 9911)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    emits = d.emits
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][1])
    }
  end

  def test_msg_size
    d = create_driver

    tests = [
      {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 100, 'expected' => 'x' * 100},
      {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 1024, 'expected' => 'x' * 1024},
    ]

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', 9911)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    emits = d.emits
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][2]['message'])
    }
  end
end

