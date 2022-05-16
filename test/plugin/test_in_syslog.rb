require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_syslog'

class SyslogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @port = unused_port
  end

  def teardown
    @port = nil
  end

  def ipv4_config
    %[
      port #{@port}
      bind 127.0.0.1
      tag syslog
    ]
  end

  def ipv6_config
    %[
      port #{@port}
      bind ::1
      tag syslog
    ]
  end

  def create_driver(conf=ipv4_config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::SyslogInput).configure(conf)
  end

  data(
    ipv4: ['127.0.0.1', :ipv4, ::Socket::AF_INET],
    ipv6: ['::1', :ipv6, ::Socket::AF_INET6],
  )
  def test_configure(data)
    bind_addr, protocol, family = data
    config = send("#{protocol}_config")
    omit "IPv6 unavailable" if family == ::Socket::AF_INET6 && !ipv6_enabled?

    d = create_driver(config)
    assert_equal @port, d.instance.port
    assert_equal bind_addr, d.instance.bind
  end

  sub_test_case 'source_hostname_key and source_address_key features' do
    test 'resolve_hostname must be true with source_hostname_key' do
      assert_raise(Fluent::ConfigError) {
        create_driver(ipv4_config + <<EOS)
resolve_hostname false
source_hostname_key hostname
EOS
      }
    end

    data('resolve_hostname' => 'resolve_hostname true',
         'source_hostname_key' => 'source_hostname_key source_host')
    def test_configure_resolve_hostname(param)
      d = create_driver([ipv4_config, param].join("\n"))
      assert_true d.instance.resolve_hostname
    end
  end

  data('Use protocol_type' => ['protocol_type tcp', :tcp, :udp],
       'Use transport' => ["<transport tcp>\n </transport>", nil, :tcp],
       'Use transport and protocol' => ["protocol_type udp\n<transport tcp>\n </transport>", :udp, :tcp])
  def test_configure_protocol(param)
    conf, proto_type, transport_proto_type = *param
    d = create_driver([ipv4_config, conf].join("\n"))

    assert_equal(d.instance.protocol_type, proto_type)
    assert_equal(d.instance.transport_config.protocol, transport_proto_type)
  end

  # For backward compat
  def test_respect_protocol_type_than_transport
    d = create_driver([ipv4_config, "<transport tcp> \n</transport>", "protocol_type udp"].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end


  data(
    ipv4: ['127.0.0.1', :ipv4, ::Socket::AF_INET],
    ipv6: ['::1', :ipv6, ::Socket::AF_INET6],
  )
  def test_time_format(data)
    bind_addr, protocol, family = data
    config = send("#{protocol}_config")
    omit "IPv6 unavailable" if family == ::Socket::AF_INET6 && !ipv6_enabled?

    d = create_driver(config)

    tests = [
      {'msg' => '<6>Dec 11 00:00:00 localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Dec 11 00:00:00', '%b %d %H:%M:%S'))},
      {'msg' => '<6>Dec  1 00:00:00 localhost logger: foo', 'expected' => Fluent::EventTime.from_time(Time.strptime('Dec  1 00:00:00', '%b  %d %H:%M:%S'))},
    ]
    d.run(expect_emits: 2) do
      u = UDPSocket.new(family)
      u.connect(bind_addr, @port)
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
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_udp_for_large_msg
    d = create_driver(ipv4_config + %[
      message_length_limit 5k
    ])
    tests = create_test_case(large_message: true)

    d.run(expect_emits: 3) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_tcp
    d = create_driver([ipv4_config, "<transport tcp> \n</transport>"].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      tests.each {|test|
        TCPSocket.open('127.0.0.1', @port) do |s|
          s.send(test['msg'], 0)
        end
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_emit_rfc5452
    d = create_driver([ipv4_config, "facility_key pri\n<parse>\n message_format rfc5424\nwith_priority true\n</parse>"].join("\n"))
    msg = '<1>1 2017-02-06T13:14:15.003Z myhostname 02abaf0687f5 10339 02abaf0687f5 - method=POST db=0.00'

    d.run(expect_emits: 1, timeout: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      u.send(msg, 0)
    end

    tag, _, event = d.events[0]
    assert_equal('syslog.kern.alert', tag)
    assert_equal('kern', event['pri'])
  end

  def test_msg_size_with_same_tcp_connection
    d = create_driver([ipv4_config, "<transport tcp> \n</transport>"].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      TCPSocket.open('127.0.0.1', @port) do |s|
        tests.each {|test|
          s.send(test['msg'], 0)
        }
      end
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_json_format
    d = create_driver([ipv4_config, 'format json'].join("\n"))
    time = Time.parse('2013-09-18 12:00:00 +0900').to_i
    tests = ['Hello!', 'Syslog!'].map { |msg|
      event = {'time' => time, 'message' => msg}
      {'msg' => '<6>' + event.to_json + "\n", 'expected' => msg}
    }

    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests)
  end

  def test_msg_size_with_include_source_host
    d = create_driver([ipv4_config, 'include_source_host true'].join("\n"))
    tests = create_test_case

    host = nil
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      host = u.peeraddr[2]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {host: host})
  end

  data(
    severity_key: 'severity_key',
    priority_key: 'priority_key',
  )
  def test_msg_size_with_severity_key(param_name)
    d = create_driver([ipv4_config, "#{param_name} severity"].join("\n"))
    tests = create_test_case

    severity = 'info'
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {severity: severity})
  end

  def test_msg_size_with_facility_key
    d = create_driver([ipv4_config, 'facility_key facility'].join("\n"))
    tests = create_test_case

    facility = 'kern'
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {facility: facility})
  end

  def test_msg_size_with_source_address_key
    d = create_driver([ipv4_config, 'source_address_key source_address'].join("\n"))
    tests = create_test_case

    address = nil
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.connect('127.0.0.1', @port)
      address = u.peeraddr[3]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert(d.events.size > 0)
    compare_test_result(d.events, tests, {address: address})
  end

  def test_msg_size_with_source_hostname_key
    d = create_driver([ipv4_config, 'source_hostname_key source_hostname'].join("\n"))
    tests = create_test_case

    hostname = nil
    d.run(expect_emits: 2) do
      u = UDPSocket.new
      u.do_not_reverse_lookup = false
      u.connect('127.0.0.1', @port)
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
      assert_equal(options[:severity], events[i][2]['severity']) if options[:severity]
      assert_equal(options[:facility], events[i][2]['facility']) if options[:facility]
    }
  end

  sub_test_case 'octet counting frame' do
    def test_msg_size_with_tcp
      d = create_driver([ipv4_config, "<transport tcp> \n</transport>", 'frame_type octet_count'].join("\n"))
      tests = create_test_case

      d.run(expect_emits: 2) do
        tests.each {|test|
          TCPSocket.open('127.0.0.1', @port) do |s|
            s.send(test['msg'], 0)
          end
        }
      end

      assert(d.events.size > 0)
      compare_test_result(d.events, tests)
    end

    def test_msg_size_with_same_tcp_connection
      d = create_driver([ipv4_config, "<transport tcp> \n</transport>", 'frame_type octet_count'].join("\n"))
      tests = create_test_case

      d.run(expect_emits: 2) do
        TCPSocket.open('127.0.0.1', @port) do |s|
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
        msg['msg'] = "#{m.size} #{m}"
      }
      msgs
    end
  end

  def create_unmatched_lines_test_case
    [
      # valid message
      {'msg' => '<6>Sep 10 00:00:00 localhost logger: xxx', 'expected' => {'host'=>'localhost', 'ident'=>'logger', 'message'=>'xxx'}},
      # missing priority
      {'msg' => 'hello world', 'expected' => {'unmatched_line' => 'hello world'}},
      # timestamp parsing failure
      {'msg' => '<6>ZZZ 99 99:99:99 localhost logger: xxx', 'expected' => {'unmatched_line' => '<6>ZZZ 99 99:99:99 localhost logger: xxx'}},
    ]
  end

  def compare_unmatched_lines_test_result(events, tests, options = {})
    events.each_index { |i|
      tests[i]['expected'].each { |k,v|
        assert_equal v, events[i][2][k], "No key <#{k}> in response or value mismatch"
      }
      assert_equal('syslog.unmatched', events[i][0], 'tag does not match syslog.unmatched') unless i==0
      assert_equal(options[:address], events[i][2]['source_address'], 'response has no source_address or mismatch') if options[:address]
      assert_equal(options[:hostname], events[i][2]['source_hostname'], 'response has no source_hostname or mismatch') if options[:hostname]
    }
  end

  def test_emit_unmatched_lines
    d = create_driver([ipv4_config, 'emit_unmatched_lines true'].join("\n"))
    tests = create_unmatched_lines_test_case

    d.run(expect_emits: 3) do
      u = UDPSocket.new
      u.do_not_reverse_lookup = false
      u.connect('127.0.0.1', @port)
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert_equal tests.size, d.events.size
    compare_unmatched_lines_test_result(d.events, tests)
  end

  def test_emit_unmatched_lines_with_hostname
    d = create_driver([ipv4_config, 'emit_unmatched_lines true', 'source_hostname_key source_hostname'].join("\n"))
    tests = create_unmatched_lines_test_case

    hostname = nil
    d.run(expect_emits: 3) do
      u = UDPSocket.new
      u.do_not_reverse_lookup = false
      u.connect('127.0.0.1', @port)
      hostname = u.peeraddr[2]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert_equal tests.size, d.events.size
    compare_unmatched_lines_test_result(d.events, tests, {hostname: hostname})
  end

  def test_emit_unmatched_lines_with_address
    d = create_driver([ipv4_config, 'emit_unmatched_lines true', 'source_address_key source_address'].join("\n"))
    tests = create_unmatched_lines_test_case

    address = nil
    d.run(expect_emits: 3) do
      u = UDPSocket.new
      u.do_not_reverse_lookup = false
      u.connect('127.0.0.1', @port)
      address = u.peeraddr[3]
      tests.each {|test|
        u.send(test['msg'], 0)
      }
    end

    assert_equal tests.size, d.events.size
    compare_unmatched_lines_test_result(d.events, tests, {address: address})
  end

  def test_send_keepalive_packet_is_disabled_by_default
    d = create_driver(ipv4_config + %[
      <transport tcp>
      </transport>
      protocol tcp
    ])
    assert_false d.instance.send_keepalive_packet
  end

  def test_send_keepalive_packet_can_be_enabled
    addr = "127.0.0.1"
    d = create_driver(ipv4_config + %[
      <transport tcp>
      </transport>
      send_keepalive_packet true
    ])
    assert_true d.instance.send_keepalive_packet
    mock.proxy(d.instance).server_create_connection(
      :in_syslog_tcp_server, @port,
      bind: addr,
      resolve_name: nil,
      send_keepalive_packet: true)
    d.run do
      TCPSocket.open(addr, @port)
    end
  end

  def test_send_keepalive_packet_can_not_be_enabled_for_udp
    assert_raise(Fluent::ConfigError) do
      create_driver(ipv4_config + %[
        send_keepalive_packet true
      ])
    end
  end
end
