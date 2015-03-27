require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_tcp'

class TcpInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  PORT = unused_port
  BASE_CONFIG = %[
    port #{PORT}
    tag tcp
  ]
  CONFIG = BASE_CONFIG + %[
    bind 127.0.0.1
    format none
  ]
  IPv6_CONFIG = BASE_CONFIG + %[
    bind ::1
    format none
  ]

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::TcpInput).configure(conf)
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

  {
    'none' => [
      {'msg' => "tcptest1\n", 'expected' => 'tcptest1'},
      {'msg' => "tcptest2\n", 'expected' => 'tcptest2'},
    ],
    'json' => [
      {'msg' => {'k' => 123, 'message' => 'tcptest1'}.to_json + "\n", 'expected' => 'tcptest1'},
      {'msg' => {'k' => 'tcptest2', 'message' => 456}.to_json + "\n", 'expected' => 456},
    ]
  }.each { |format, test_cases|
    define_method("test_msg_size_#{format}") do
      d = create_driver(BASE_CONFIG + "format #{format}")
      tests = test_cases

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

    define_method("test_msg_size_with_same_connection_#{format}") do
      d = create_driver(BASE_CONFIG + "format #{format}")
      tests = test_cases

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
  }

  def compare_test_result(emits, tests)
    assert_equal(2, emits.size)
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][2]['message'])
    }
  end
end
