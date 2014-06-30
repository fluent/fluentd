require 'fluent/test'
require 'helper'

class TcpInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
    tag tcp
    format none
  ]

  IPv6_CONFIG = %[
    port #{PORT}
    bind ::1
    tag tcp
    format none
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::TcpInput).configure(conf)
  end

  def test_configure
    configs = {'127.0.0.1' => CONFIG}
    configs.merge!('::1' => IPv6_CONFIG) if ipv6_enabled?

    configs.each_pair { |k, v|
      d = create_driver(v)
      assert_equal PORT, d.instance.port
      assert_equal k, d.instance.bind
      assert_equal "\n", d.instance.delimiter
    }
  end

  def test_msg_size
    d = create_driver
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

  def test_msg_size_with_same_connection
    d = create_driver
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
    [
      {'msg' => "tcptest1\n", 'expected' => 'tcptest1'},
      {'msg' => "tcptest2\n", 'expected' => 'tcptest2'},
    ]
  end

  def compare_test_result(emits, tests)
    assert_equal(2, emits.size)
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][2]['message'])
    }
  end
end
