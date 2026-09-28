require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_syslog'

class SyslogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @port = unused_port(protocol: :udp)
  end

  def teardown
    @port = nil
  end

  def ipv4_config(port = @port)
    %[
      port #{port}
      bind 127.0.0.1
      tag syslog
    ]
  end

  def ipv6_config(port = @port)
    %[
      port #{port}
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
    port = unused_port(protocol: proto_type ? proto_type : transport_proto_type)
    d = create_driver([ipv4_config(port), conf].join("\n"))

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
    port = unused_port(protocol: :tcp)
    d = create_driver([ipv4_config(port), "<transport tcp> \n</transport>"].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      tests.each {|test|
        TCPSocket.open('127.0.0.1', port) do |s|
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
    port = unused_port(protocol: :tcp)
    d = create_driver([ipv4_config(port), "<transport tcp> \n</transport>"].join("\n"))
    tests = create_test_case

    d.run(expect_emits: 2) do
      TCPSocket.open('127.0.0.1', port) do |s|
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
      port = unused_port(protocol: :tcp)
      d = create_driver([ipv4_config(port), "<transport tcp> \n</transport>", 'frame_type octet_count'].join("\n"))
      tests = create_test_case

      d.run(expect_emits: 2) do
        tests.each {|test|
          TCPSocket.open('127.0.0.1', port) do |s|
            s.send(test['msg'], 0)
          end
        }
      end

      assert(d.events.size > 0)
      compare_test_result(d.events, tests)
    end

    def test_msg_size_with_same_tcp_connection
      port = unused_port(protocol: :tcp)
      d = create_driver([ipv4_config(port), "<transport tcp> \n</transport>", 'frame_type octet_count'].join("\n"))
      tests = create_test_case

      d.run(expect_emits: 2) do
        TCPSocket.open('127.0.0.1', port) do |s|
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
    port = unused_port(protocol: :tcp)
    d = create_driver(ipv4_config(port) + %[
      <transport tcp>
      </transport>
      protocol tcp
    ])
    assert_false d.instance.send_keepalive_packet
  end

  def test_send_keepalive_packet_can_be_enabled
    addr = "127.0.0.1"
    port = unused_port(protocol: :tcp)
    d = create_driver(ipv4_config(port) + %[
      <transport tcp>
      </transport>
      send_keepalive_packet true
    ])
    assert_true d.instance.send_keepalive_packet
    mock.proxy(d.instance).server_create_connection(
      :in_syslog_tcp_server, port,
      bind: addr,
      resolve_name: nil,
      send_keepalive_packet: true)
    d.run do
      TCPSocket.open(addr, port)
    end
  end

  def test_send_keepalive_packet_can_not_be_enabled_for_udp
    assert_raise(Fluent::ConfigError) do
      create_driver(ipv4_config + %[
        send_keepalive_packet true
      ])
    end
  end

  sub_test_case 'message_length_limit for tcp' do
    LIMIT = 1024
    NORMAL_MESSAGE = '<6>Sep 10 00:00:00 localhost logger: hello'

    def create_tcp_driver(port, frame_type: :traditional)
      create_driver([
        ipv4_config(port),
        "<transport tcp>\n</transport>",
        "frame_type #{frame_type}",
        "message_length_limit #{LIMIT}",
      ].join("\n"))
    end

    def max_buffer_size(d)
      conns = d.instance.instance_variable_get(:@_server_connections)
      conns.filter_map { |conn|
        conn.instance_variable_get(:@callback_connection)&.buffer&.bytesize
      }.max || 0
    end

    # The plugin closes the connection by sending RST (SO_LINGER 0), so both EOF and
    # ECONNRESET mean that the connection was closed by the plugin.
    def wait_until_closed(sock, timeout: 10)
      waiting(timeout) do
        loop do
          begin
            return true if sock.read_nonblock(1024).nil?
          rescue EOFError, Errno::ECONNRESET
            return true
          rescue IO::WaitReadable
            IO.select([sock], nil, nil, 0.1)
          end
        end
      end
    rescue Timeout::Error
      false
    end

    def test_default_message_length_limit
      assert_equal 8192, create_driver(ipv4_config).instance.message_length_limit
    end

    test 'traditional: the buffer does not grow unboundedly without delimiter' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port)
      observed_max = 0

      d.run(expect_emits: 1, timeout: 30) do
        TCPSocket.open('127.0.0.1', port) do |s|
          # Send 200KB without any delimiter.
          100.times do
            s.write('x' * 2048)
            s.flush
            sleep 0.01
            size = max_buffer_size(d)
            observed_max = size if size > observed_max
          end

          # The tail of the oversized data is discarded up to the next delimiter,
          # and the subsequent message must be still handled.
          s.write("\n#{NORMAL_MESSAGE}\n")
          s.flush
          waiting(10) { sleep 0.1 until d.events.size >= 1 }
        end
      end

      assert do
        observed_max <= 32 * 1024 # much smaller than the 200KB we sent
      end
      assert_equal 1, d.events.size
      assert_equal 'hello', d.events[0][2]['message']
    end

    test 'traditional: a message larger than the limit is dropped and the next one is emitted' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port)

      d.run(expect_emits: 1, timeout: 20) do
        TCPSocket.open('127.0.0.1', port) do |s|
          s.write("<6>Sep 10 00:00:00 localhost logger: #{'x' * (LIMIT * 2)}\n")
          s.write("#{NORMAL_MESSAGE}\n")
          s.flush
          waiting(10) { sleep 0.1 until d.events.size >= 1 }
        end
      end

      assert_equal 1, d.events.size
      assert_equal 'hello', d.events[0][2]['message']
    end

    test 'traditional: a message split into multiple chunks is reassembled' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port)

      d.run(expect_emits: 1, timeout: 20) do
        TCPSocket.open('127.0.0.1', port) do |s|
          NORMAL_MESSAGE.each_char do |c|
            s.write(c)
            s.flush
          end
          s.write("\n")
          s.flush
        end
      end

      assert_equal 1, d.events.size
      assert_equal 'hello', d.events[0][2]['message']
    end

    test 'octet_count: a declared length larger than the limit closes the connection' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port, frame_type: :octet_count)
      closed = false

      d.run(timeout: 20) do
        TCPSocket.open('127.0.0.1', port) do |s|
          s.write("#{LIMIT + 1} #{NORMAL_MESSAGE}")
          s.flush
          closed = wait_until_closed(s)
        end
      end
      assert_true closed
      assert_equal 0, d.events.size
    end

    test 'octet_count: data without delimiter beyond the limit closes the connection' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port, frame_type: :octet_count)
      closed = false

      d.run(timeout: 20) do
        TCPSocket.open('127.0.0.1', port) do |s|
          s.write('x' * (LIMIT * 2)) # no space delimiter at all
          s.flush
          closed = wait_until_closed(s)
        end
      end
      assert_true closed
      assert_equal 0, d.events.size
    end

    test 'octet_count: a message split into multiple chunks is reassembled' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port, frame_type: :octet_count)

      d.run(expect_emits: 1, timeout: 20) do
        TCPSocket.open('127.0.0.1', port) do |s|
          s.write("#{NORMAL_MESSAGE.size} ")
          s.flush
          sleep 0.1
          s.write(NORMAL_MESSAGE[0...10])
          s.flush
          sleep 0.1
          s.write(NORMAL_MESSAGE[10..-1])
          s.flush
        end
      end

      assert_equal 1, d.events.size
      assert_equal 'hello', d.events[0][2]['message']
    end

    test 'octet_count: a near-limit message is not treated as an attack' do
      port = unused_port(protocol: :tcp)
      d = create_tcp_driver(port, frame_type: :octet_count)
      message = "<6>Sep 10 00:00:00 localhost logger: #{'x' * (LIMIT - 37)}"
      assert_equal LIMIT, message.size

      d.run(expect_emits: 1, timeout: 20) do
        TCPSocket.open('127.0.0.1', port) do |s|
          # The length header + this partial frame (1025 bytes) exceeds the limit,
          # but it must not be treated as an attack because the frame is legitimate.
          s.write("#{message.size} #{message[0...1020]}")
          s.flush
          sleep 0.1
          s.write(message[1020..-1])
          s.flush
        end
      end

      assert_equal 1, d.events.size
      assert_equal 'x' * (LIMIT - 37), d.events[0][2]['message']
    end
  end
end
