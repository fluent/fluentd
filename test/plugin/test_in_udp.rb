require 'fluent/test'
require 'helper'

class UdpInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  PORT = unused_port
  CONFIG = %!
    port #{PORT}
    bind 127.0.0.1
    tag udp
    format /^\\[(?<time>[^\\]]*)\\] (?<message>.*)/
  !

  IPv6_CONFIG = %!
    port #{PORT}
    bind ::1
    tag udp
    format /^\\[(?<time>[^\\]]*)\\] (?<message>.*)/
  !

  def create_driver(conf = CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::UdpInput).configure(conf)
  end

  def test_configure
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      d = create_driver(v)
      assert_equal PORT, d.instance.port
      assert_equal k, d.instance.bind
      assert_equal 4096, d.instance.body_size_limit
    }
  end

  def test_time_format
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      d = create_driver(v)

      tests = [
        {'msg' => '[Sep 11 00:00:00] localhost logger: foo', 'expected' => Time.strptime('Sep 11 00:00:00', '%b %d %H:%M:%S').to_i},
        {'msg' => '[Sep  1 00:00:00] localhost logger: foo', 'expected' => Time.strptime('Sep  1 00:00:00', '%b  %d %H:%M:%S').to_i},
      ]

      d.run do
        u = Fluent::SocketUtil.create_udp_socket(k)
        u.connect(k, PORT)
        tests.each {|test|
          u.send(test['msg'], 0)
        }
        sleep 1
      end

      emits = d.emits
      emits.each_index {|i|
        assert_equal(tests[i]['expected'], emits[i][1])
      }
    }
  end

  def test_msg_size
    d = create_driver
    tests = create_test_case

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each { |test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    compare_test_result(d.emits, tests)
  end

  def create_test_case
    [
      {'msg' => '[Sep 10 00:00:00] localhost: ' + 'x' * 100 + "\n", 'expected' => 'localhost: ' + 'x' * 100},
      {'msg' => '[Sep 10 00:00:00] localhost: ' + 'x' * 1024 + "\n", 'expected' => 'localhost: ' + 'x' * 1024},
    ]
  end

  def compare_test_result(emits, tests)
    assert_equal(2, emits.size)
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][2]['message'])
    }
  end
end
