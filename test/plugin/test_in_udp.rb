require_relative '../helper'
require 'fluent/test/driver/input'
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
    Fluent::Test::Driver::Input.new(Fluent::Plugin::UdpInput).configure(conf)
  end

  def create_udp_socket(host, port)
    u = if IPAddr.new(IPSocket.getaddress(host)).ipv4?
          UDPSocket.new(Socket::AF_INET)
        else
          UDPSocket.new(Socket::AF_INET6)
        end
    u.do_not_reverse_lookup = false
    u.connect(host, port)
    if block_given?
      begin
        yield u
      ensure
        u.close rescue nil
      end
    else
      u
    end
  end

  data(
    'ipv4' => [CONFIG, '127.0.0.1', :ipv4],
    'ipv6' => [IPv6_CONFIG, '::1', :ipv6],
  )
  test 'configure' do |data|
    conf, bind, protocol = data
    omit "IPv6 is not supported on this environment" if protocol == :ipv6 && !ipv6_enabled?

    d = create_driver(conf)
    assert_equal PORT, d.instance.port
    assert_equal bind, d.instance.bind
    assert_equal 4096, d.instance.message_length_limit
    assert_equal nil, d.instance.receive_buffer_size
  end

  data(
    'ipv4' => [CONFIG, '127.0.0.1', :ipv4],
    'ipv6' => [IPv6_CONFIG, '::1', :ipv6],
  )
  test 'time_format' do |data|
    conf, bind, protocol = data
    omit "IPv6 is not supported on this environment" if protocol == :ipv6 && !ipv6_enabled?

    d = create_driver(conf)

    tests = [
      {'msg' => '[Sep 11 00:00:00] localhost logger: foo', 'expected' => event_time('Sep 11 00:00:00', format: '%b %d %H:%M:%S')},
      {'msg' => '[Sep  1 00:00:00] localhost logger: foo', 'expected' => event_time('Sep  1 00:00:00', format: '%b  %d %H:%M:%S')},
    ]

    d.run(expect_records: 2) do
      create_udp_socket(bind, PORT) do |u|
        tests.each do |test|
          u.send(test['msg'], 0)
        end
      end
    end

    events = d.events
    tests.each_with_index do |t, i|
      assert_equal_event_time(t['expected'], events[i][1])
    end
  end

  data(
    'message_length_limit' => 'message_length_limit 2048',
    'body_size_limit' => 'body_size_limit 2048'
  )
  test 'message_length_limit/body_size_limit compatibility' do |param|

    d = create_driver(CONFIG + param)
    assert_equal 2048, d.instance.message_length_limit
  end

  data(
    'none' => {
      'format' => 'none',
      'payloads' => ["tcptest1\n", "tcptest2\n"],
      'expecteds' => [
        {"message" => "tcptest1"},
        {"message" => "tcptest2"},
      ],
    },
    'json' => {
      'format' => 'json',
      'payloads' => [
        {'k' => 123, 'message' => 'tcptest1'}.to_json + "\n",
        {'k' => 'tcptest2', 'message' => 456}.to_json + "\n",
      ],
      'expecteds' => [
        {'k' => 123, 'message' => 'tcptest1'},
        {'k' => 'tcptest2', 'message' => 456},
      ],
    },
    'regexp' => {
      'format' => '/^\\[(?<time>[^\\]]*)\\] (?<message>.*)/',
      'payloads' => [
        '[Sep 10 00:00:00] localhost: ' + 'x' * 100 + "\n",
        '[Sep 10 00:00:00] localhost: ' + 'x' * 1024 + "\n"
      ],
      'expecteds' => [
        {"message" => 'localhost: ' + 'x' * 100},
        {"message" => 'localhost: ' + 'x' * 1024},
      ],
    },
  )
  test 'message size with format' do |data|
    format = data['format']
    payloads = data['payloads']
    expecteds = data['expecteds']

    d = create_driver(BASE_CONFIG + "format #{format}")
    d.run(expect_records: 2) do
      create_udp_socket('127.0.0.1', PORT) do |u|
        payloads.each do |payload|
          u.send(payload, 0)
        end
      end
    end

    assert_equal 2, d.events.size
    expecteds.each_with_index do |expected_record, i|
      assert_equal "udp", d.events[i][0]
      assert d.events[i][1].is_a?(Fluent::EventTime)
      assert_equal expected_record, d.events[i][2]
    end
  end

  test 'remove_newline' do
    d = create_driver(BASE_CONFIG + %!
      format none
      remove_newline false
    !)
    payloads = ["test1\n", "test2\n"]
    d.run(expect_records: 2) do
      create_udp_socket('127.0.0.1', PORT) do |u|
        payloads.each do |payload|
          u.send(payload, 0)
        end
      end
    end

    expecteds = payloads.map { |payload| {'message' => payload} }
    assert_equal 2, d.events.size
    expecteds.each_with_index do |expected_record, i|
      assert_equal "udp", d.events[i][0]
      assert d.events[i][1].is_a?(Fluent::EventTime)
      assert_equal expected_record, d.events[i][2]
    end
  end

  test 'source_hostname_key' do
    d = create_driver(BASE_CONFIG + %!
      format none
      source_hostname_key host
    !)
    hostname = nil
    d.run(expect_records: 1) do
      create_udp_socket('127.0.0.1', PORT) do |u|
        u.send("test", 0)
        hostname = u.peeraddr[2]
      end
    end

    expected = {'message' => 'test'}
    assert_equal 1, d.events.size
    assert_equal "udp", d.events[0][0]
    assert d.events[0][1].is_a?(Fluent::EventTime)
    assert_equal hostname, d.events[0][2]['host']
  end

  test 'receive_buffer_size' do
    # doesn't check exact value because it depends on platform and condition

    # check if default socket and in_udp's one without receive_buffer_size have same size buffer
    d0 = create_driver(BASE_CONFIG + %!
      format none
    !)
    d0.run do
      sock = d0.instance.instance_variable_get(:@_servers)[0].server.instance_variable_get(:@sock)
      begin
        default_sock = UDPSocket.new
        assert_equal(default_sock.getsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF).int, sock.getsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF).int)
      ensure
        default_sock.close
      end
    end

    # check if default socket and in_udp's one with receive_buffer_size have different size buffer
    d1 = create_driver(BASE_CONFIG + %!
      format none
      receive_buffer_size 1001
    !)
    d1.run do
      sock = d1.instance.instance_variable_get(:@_servers)[0].server.instance_variable_get(:@sock)
      begin
        default_sock = UDPSocket.new
        assert_not_equal(default_sock.getsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF).int, sock.getsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF).int)
      ensure
        default_sock.close
      end
    end
  end
end
