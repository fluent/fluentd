require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_syslog'

class SyslogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
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

  data(
    ipv4: ['127.0.0.1', CONFIG, ::Socket::AF_INET],
    ipv6: ['::1', IPv6_CONFIG, ::Socket::AF_INET6],
  )
  def test_configure(data)
    bind_addr, config, family = data
    omit "IPv6 unavailable" if family == ::Socket::AF_INET6 && !ipv6_enabled?

    d = create_driver(config)
    assert_equal PORT, d.instance.port
    assert_equal bind_addr, d.instance.bind
  end

  sub_test_case 'source_hostname_key and source_address_key features' do
    test 'resolve_hostname must be true with source_hostname_key' do
      assert_raise(Fluent::ConfigError) {
        create_driver(CONFIG + <<EOS)
resolve_hostname false
source_hostname_key hostname
EOS
      }
    end

    data('resolve_hostname' => 'resolve_hostname true',
         'source_hostname_key' => 'source_hostname_key source_host')
    def test_configure_resolve_hostname(param)
      d = create_driver([CONFIG, param].join("\n"))
      assert_true d.instance.resolve_hostname
    end
  end

  data(
    ipv4: ['127.0.0.1', CONFIG, ::Socket::AF_INET],
    ipv6: ['::1', IPv6_CONFIG, ::Socket::AF_INET6],
  )
  def test_time_format(data)
    bind_addr, config, family = data
    omit "IPv6 unavailable" if family == ::Socket::AF_INET6 && !ipv6_enabled?

    d = create_driver(config)

    tests = [
      {'msg' => '<6>Dec 11 00:00:00 localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Dec 11 00:00:00', '%b %d %H:%M:%S'))},
      {'msg' => '<6>Dec  1 00:00:00 localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Dec  1 00:00:00', '%b  %d %H:%M:%S'))},
    ]
    d.run(expect_emits: 2) do
      u = UDPSocket.new(family)
      u.connect(bind_addr, PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    events = d.events
    assert(events.size > 0)
    events.each_index {|i|
      assert_equal_event_time(tests[i]['expected'], events[i][1])
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
    compare_test_result(d.events, tests, {host: host})
  end

  def test_msg_size_with_priority_key
    d = create_driver([CONFIG, 'priority_key priority'].join("\n"))
    tests = create_test_case

    priority = 'info'
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {priority: priority})
  end

  def test_msg_size_with_facility_key
    d = create_driver([CONFIG, 'facility_key facility'].join("\n"))
    tests = create_test_case

    facility = 'kern'
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {facility: facility})
  end

  def test_msg_size_with_source_address_key
    d = create_driver([CONFIG, 'source_address_key source_address'].join("\n"))
    tests = create_test_case

    address = nil
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', PORT)
      address = u.peeraddr[3]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {address: address})
  end

  def test_msg_size_with_source_hostname_key
    d = create_driver([CONFIG, 'source_hostname_key source_hostname'].join("\n"))
    tests = create_test_case

    hostname = nil
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.do_not_reverse_lookup = false
      u.connect('127.0.0.1', PORT)
      hostname = u.peeraddr[2]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {hostname: hostname})
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

  def compare_test_result(events, tests, options = {})
    events.each_index { |i|
      assert_equal('syslog.kern.info', events[i][0]) # <6> means kern.info
      assert_equal(tests[i]['expected'], events[i][2]['message'])
      assert_equal(options[:host], events[i][2]['source_host']) if options[:host]
      assert_equal(options[:address], events[i][2]['source_address']) if options[:address]
      assert_equal(options[:hostname], events[i][2]['source_hostname']) if options[:hostname]
      assert_equal(options[:priority], events[i][2]['priority']) if options[:priority]
      assert_equal(options[:facility], events[i][2]['facility']) if options[:facility]
    }
  end

  sub_test_case 'octet counting frame' do
    def test_msg_size_with_tcp
      d = create_driver([CONFIG, 'protocol_type tcp', 'frame_type octet_count'].join("\n"))
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
      d = create_driver([CONFIG, 'protocol_type tcp', 'frame_type octet_count'].join("\n"))
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

    def create_test_case(large_message: false)
      msgs = [
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 100, 'expected' => 'x' * 100},
        {'msg' => '<6>Sep 10 00:00:00 localhost logger: ' + 'x' * 1024, 'expected' => 'x' * 1024},
      ]
      msgs.each { |msg|
        m = msg['msg']
        msg['msg'] = "#{m.size + 1} #{m}"
      }
      msgs
    end
  end
end
