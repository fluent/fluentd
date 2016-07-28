require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_syslog'

class SyslogInputTest < Test::Unit::TestCase
  class << self
    def startup
      socket_manager_path = ServerEngine::SocketManager::Server.generate_path
      @server = ServerEngine::SocketManager::Server.open(socket_manager_path)
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
    end

    def shutdown
      @server.close
    end
  end

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
    Fluent::Test::Driver::Input.new(Fluent::Plugin::SyslogInput).configure(conf)
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
        {'msg' => '<6>Dec 11 00:00:00 localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Dec 11 00:00:00', '%b %d %H:%M:%S'))},
        {'msg' => '<6>Dec  1 00:00:00 localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Dec  1 00:00:00', '%b  %d %H:%M:%S'))},
      ]
      d.run(expect_emits: 2) do
        u = Fluent::SocketUtil.create_udp_socket(k)
        u.connect(k, PORT)
        tests.each {|test|
          u.send(test['msg'], 0)
        }
      end

      events = d.events
      assert(events.size > 0)
      events.each_index {|i|
        assert_equal_event_time(tests[i]['expected'], events[i][1])
      }
    }
  end

  def test_msg_size
    d = create_driver
    tests = create_test_case

    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_udp_for_large_msg
    d = create_driver(CONFIG + %[
      message_length_limit 5k
    ])
    tests = create_test_case(large_message: true)

    d.run(expect_emits: 3) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_tcp
    d = create_driver([CONFIG, 'protocol_type tcp'].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      tests.each {|test|
        TCPSocket.open('127.0.0.1', PORT) do |s|
          s.send(test['msg'], 0)
        end
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_same_tcp_connection
    d = create_driver([CONFIG, 'protocol_type tcp'].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      TCPSocket.open('127.0.0.1', PORT) do |s|
        tests.each {|test|
          s.send(test['msg'], 0)
        }
      end
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_json_format
    d = create_driver([CONFIG, 'format json'].join("\n"))
    time = Time.parse('2013-09-18 12:00:00 +0900').to_i
    tests = ['Hello!', 'Syslog!'].map { |msg|
      event = {'time' => time, 'message' => msg}
      {'msg' => '<6>' + event.to_json + "\n", 'expected' => msg}
    }

    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_include_source_host
    d = create_driver([CONFIG, 'include_source_host true'].join("\n"))
    tests = create_test_case

    host = nil
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      host = u.peeraddr[2]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, host)
  end

  def create_test_case(large_message: false)
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

  def compare_test_result(events, tests, host = nil)
    events.each_index { |i|
      assert_equal('syslog.kern.info', events[i][0]) # <6> means kern.info
      assert_equal(tests[i]['expected'], events[i][2]['message'])
      assert_equal(host, events[i][2]['source_host']) if host
    }
  end
end
