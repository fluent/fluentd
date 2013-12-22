require 'fluent/test'
require 'helper'

class SyslogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/socket_util'
  end

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
    tag syslog
  ]

  IPv6_CONFIG = %[
    port #{PORT}
    bind ::1
    tag syslog
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::SyslogInput).configure(conf)
  end

  def test_configure
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      d = create_driver(v)
      assert_equal PORT, d.instance.port
      assert_equal k, d.instance.bind
    }
  end

  def test_time_format
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      d = create_driver(v)

      tests = [
        {'msg' => '<6>Sep 11 00:00:00 localhost logger: foo', 'expected' => Time.strptime('Sep 11 00:00:00', '%b %d %H:%M:%S').to_i},
        {'msg' => '<6>Sep  1 00:00:00 localhost logger: foo', 'expected' => Time.strptime('Sep  1 00:00:00', '%b  %d %H:%M:%S').to_i},
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
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    compare_test_result(d.emits, tests)
  end

  def test_msg_size_with_tcp
    d = create_driver([CONFIG, 'protocol_type tcp'].join("\n"))
    tests = create_test_case

    d.run do
      tests.each {|test|
        TCPSocket.open('127.0.0.1', PORT) do |s|
          s.send(test['msg'], 0)
        end
      }
      sleep 1
    end

    compare_test_result(d.emits, tests)
  end

  def test_msg_size_with_same_tcp_connection
    d = create_driver([CONFIG, 'protocol_type tcp'].join("\n"))
    tests = create_test_case

    d.run do
      TCPSocket.open('127.0.0.1', PORT) do |s|
        tests.each {|test|
          s.send(test['msg'], 0)
        }
      end
      sleep 1
    end

    compare_test_result(d.emits, tests)
  end

  def create_test_case
    # actual syslog message has "\n"
    [
      {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 100 + "\n", 'expected' => 'x' * 100},
      {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 1024 + "\n", 'expected' => 'x' * 1024},
    ]
  end

  def compare_test_result(emits, tests)
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][2]['message'])
    }
  end
end
