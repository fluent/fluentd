require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_udp'

class UdpInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  PORT = unused_port
  BASE_CONFIG = %[
    port #{PORT}
    tag udp
  ]
  CONFIG = BASE_CONFIG + %!
    bind 127.0.0.1
    format /^\\[(?<time>[^\\]]*)\\] (?<message>.*)/
  !
  IPv6_CONFIG = BASE_CONFIG + %!
    bind ::1
    format /^\\[(?<time>[^\\]]*)\\] (?<message>.*)/
  !

  def create_driver(conf)
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
        {'msg' => '[Sep 11 00:00:00] localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Sep 11 00:00:00', '%b %d %H:%M:%S'))},
        {'msg' => '[Sep  1 00:00:00] localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Sep  1 00:00:00', '%b  %d %H:%M:%S'))},
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
        assert_equal_event_time(tests[i]['expected'], emits[i][1])
      }
    }
  end

  data('none' => {'format' => 'none',
                  'tests'  => [{'msg' => "tcptest1\n", 'expected' => 'tcptest1'},
                               {'msg' => "tcptest2\n", 'expected' => 'tcptest2'}]},
       'json' => {'format' => 'json',
                  'tests'  => [{'msg' => {'k' => 123, 'message' => 'tcptest1'}.to_json + "\n", 'expected' => 'tcptest1'},
                               {'msg' => {'k' => 'tcptest2', 'message' => 456}.to_json + "\n", 'expected' => 456}]},
       'regexp' => {'format' => '/^\\[(?<time>[^\\]]*)\\] (?<message>.*)/',
                    'tests' => [{'msg' => '[Sep 10 00:00:00] localhost: ' + 'x' * 100 + "\n", 'expected' => 'localhost: ' + 'x' * 100},
                                {'msg' => '[Sep 10 00:00:00] localhost: ' + 'x' * 1024 + "\n", 'expected' => 'localhost: ' + 'x' * 1024},
                               ]})
  test('test_msg_size') do |data|
    format = data['format']
    d = create_driver(BASE_CONFIG + "format #{format}")
    tests = data['tests']

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each { |test|
        u.send(test['msg'], 0)
      }
      sleep 0.1
    end

    compare_test_result(d.emits, tests)
  end

  def compare_test_result(emits, tests)
    assert_equal(2, emits.size)
    emits.each_index {|i|
      assert_equal(tests[i]['expected'], emits[i][2]['message'])
      assert(emits[i][1].is_a?(Fluent::EventTime))
    }
  end

  data('none' => {'format'    => 'none',
                  'delimiter' => "@",
                  'msg'       => "tcptest3@tcptest4\n",
                  'expected'  => ['tcptest3', 'tcptest4']},
       'json' => {'format'    => 'json',
                  'delimiter' => "@",
                  'msg'       =>  {'k' => 10, 'message' => 100}.to_json + "@" + {'k' => 20, 'message' => 200}.to_json + "\n",
                  'expected'  =>  [100, 200]},
       'regexp' => {'format'    => '/^\\[(?<time>[^\\]]*)\\] (?<message>.*)/',
                    'delimiter' => "@",
                    'msg'       => "[Sep 10 00:00:00] message1@[Sep 10 00:00:00] message2\n",
                    'expected'  => ['message1', 'message2']})
  test('test_delimiter') do |data|
    format = data['format']
    delimiter = data['delimiter']
    msg = data['msg']
    expected = data['expected']

    d = create_driver([BASE_CONFIG, "format #{format}", "delimiter #{delimiter}"].join("\n"))
    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      u.send(msg, 0)
      sleep 0.1
    end

    assert_equal(2, d.emits.size)
    d.emits.each_index {|i|
      assert_equal(expected[i], d.emits[i][2]['message'])
      assert(d.emits[i][1].is_a?(Fluent::EventTime))
    }
  end
end
