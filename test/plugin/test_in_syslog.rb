require_relative '../helper'
require 'fluent/test'

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
      assert_equal 2, emits.size
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

    assert_equal 2, d.emits.size
    compare_test_result(d.emits, tests)
  end

  def test_msg_size_udp_for_large_msg
    d = create_driver(CONFIG + %[
      message_length_limit 5k
    ])
    tests = create_test_case(true)

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    assert_equal 3, d.emits.size
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

    assert_equal 2, d.emits.size
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

    assert_equal 2, d.emits.size
    compare_test_result(d.emits, tests)
  end

  def test_msg_size_with_json_format
    d = create_driver([CONFIG, 'format json'].join("\n"))
    time = Time.parse('2013-09-18 12:00:00 +0900').to_i
    tests = ['Hello!', 'Syslog!'].map { |msg|
      event = {'time' => time, 'message' => msg}
      {'msg' => '<6>' + event.to_json + "\n", 'expected' => msg}
    }

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    assert_equal 2, d.emits.size
    compare_test_result(d.emits, tests)
  end

  LOCALHOST_HOSTNAME_GETTER = ->(){sock = UDPSocket.new(::Socket::AF_INET); sock.do_not_reverse_lookup = false; sock.connect("127.0.0.1", 2048); sock.peeraddr[2] }
  LOCALHOST_HOSTNAME = LOCALHOST_HOSTNAME_GETTER.call

  data('old parameter' => 'include_source_host',
       'new parameter' => 'source_hostname_key source_host')
  def test_msg_size_with_include_source_host(param)
    d = create_driver([CONFIG, param].join("\n"))
    tests = create_test_case

    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    assert_equal 2, d.emits.size
    compare_test_result(d.emits, tests, {host: LOCALHOST_HOSTNAME})
  end

  def test_msg_size_with_include_priority
    d = create_driver([CONFIG, 'priority_key priority'].join("\n"))
    tests = create_test_case

    priority = 'info'
    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    assert_equal 2, d.emits.size
    compare_test_result(d.emits, tests, {priority: priority})
  end

  def test_msg_size_with_include_facility
    d = create_driver([CONFIG, 'facility_key facility'].join("\n"))
    tests = create_test_case

    facility = 'kern'
    d.run do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
      sleep 1
    end

    assert_equal 2, d.emits.size
    compare_test_result(d.emits, tests, {facility: facility})
  end

  def create_test_case(large_message = false)
    # actual syslog message has "\n"
    if large_message
      [
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 100 + "\n", 'expected' => 'x' * 100},
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 1024 + "\n", 'expected' => 'x' * 1024},
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 4096 + "\n", 'expected' => 'x' * 4096},
      ]
    else
      [
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 100 + "\n", 'expected' => 'x' * 100},
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 1024 + "\n", 'expected' => 'x' * 1024},
      ]
    end
  end

  def compare_test_result(events, tests, options = {})
    events.each_index { |i|
      assert_equal('syslog.kern.info', events[i][0]) # <6> means kern.info
      assert_equal(tests[i]['expected'], events[i][2]['message'])
      assert_equal(options[:host], events[i][2]['source_host']) if options[:host]
      assert_equal(options[:priority], events[i][2]['priority']) if options[:priority]
      assert_equal(options[:facility], events[i][2]['facility']) if options[:facility]
    }
  end

  class SyslogMessageFormatTest < self
    def test_syslog_rfc5424_format
      d = create_driver(CONFIG + 'message_format rfc5424')
      tests = [
        '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!',
        '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      ]

      run_tests(d, tests)
      compare_test_result(d)
    end

    def test_syslog_auto_format
      d = create_driver(CONFIG + 'message_format auto')
      tests = [
        '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!',
        '<6>Sep 11 00:00:00 localhost fluentd: Hi, from Fluentd!'
      ]

      run_tests(d, tests)
      compare_test_result(d)
    end

    def run_tests(d, tests)
      d.run do
        u = Fluent::SocketUtil.create_udp_socket('127.0.0.1')
        u.connect('127.0.0.1', PORT)
        tests.each {|test|
          u.send(test, 0)
        }
        sleep 1
      end
    end

    def compare_test_result(d)
      emits = d.emits
      assert_equal 2, emits.size
      emits.each_index {|i|
        record =  emits[i][2]
        assert_equal('fluentd', record['ident'])
        assert_equal('Hi, from Fluentd!', record['message'])
      }
    end
  end
end
