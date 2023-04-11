require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_udp'

class UdpInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @port = unused_port
  end

  def teardown
    @port = nil
  end

  def base_config
    %[
      port #{@port}
      tag udp
    ]
  end

  def ipv4_config
    base_config + %!
      bind 127.0.0.1
      format /^\\[(?<time>[^\\]]*)\\] (?<message>.*)/
    !
  end

  def ipv6_config
    base_config + %!
      bind ::1
      format /^\\[(?<time>[^\\]]*)\\] (?<message>.*)/
    !
  end

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
    'ipv4' => ['127.0.0.1', :ipv4],
    'ipv6' => ['::1', :ipv6],
  )
  test 'configure' do |data|
    bind, protocol = data
    conf = send("#{protocol}_config")
    omit "IPv6 is not supported on this environment" if protocol == :ipv6 && !ipv6_enabled?

    d = create_driver(conf)
    assert_equal @port, d.instance.port
    assert_equal bind, d.instance.bind
    assert_equal 4096, d.instance.message_length_limit
    assert_equal nil, d.instance.receive_buffer_size
  end

  test ' configure w/o parse section' do
    assert_raise(Fluent::ConfigError.new("<parse> section is required.")) {
      create_driver(base_config)
    }
  end

  data(
    'ipv4' => ['127.0.0.1', :ipv4],
    'ipv6' => ['::1', :ipv6],
  )
  test 'time_format' do |data|
    bind, protocol = data
    conf = send("#{protocol}_config")
    omit "IPv6 is not supported on this environment" if protocol == :ipv6 && !ipv6_enabled?

    d = create_driver(conf)

    tests = [
      {'msg' => '[Sep 11 00:00:00] localhost logger: foo', 'expected' => event_time('Sep 11 00:00:00', format: '%b %d %H:%M:%S')},
      {'msg' => '[Sep  1 00:00:00] localhost logger: foo', 'expected' => event_time('Sep  1 00:00:00', format: '%b  %d %H:%M:%S')},
    ]

    d.run(expect_records: 2) do
      create_udp_socket(bind, @port) do |u|
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

    d = create_driver(ipv4_config + param)
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

    d = create_driver(base_config + "format #{format}")
    d.run(expect_records: 2) do
      create_udp_socket('127.0.0.1', @port) do |u|
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
    d = create_driver(base_config + %!
      format none
      remove_newline false
    !)
    payloads = ["test1\n", "test2\n"]
    d.run(expect_records: 2) do
      create_udp_socket('127.0.0.1', @port) do |u|
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
    d = create_driver(base_config + %!
      format none
      source_hostname_key host
    !)
    hostname = nil
    d.run(expect_records: 1) do
      create_udp_socket('127.0.0.1', @port) do |u|
        u.send("test", 0)
        hostname = u.peeraddr[2]
      end
    end

    assert_equal 1, d.events.size
    assert_equal "udp", d.events[0][0]
    assert d.events[0][1].is_a?(Fluent::EventTime)
    assert_equal hostname, d.events[0][2]['host']
  end

  test 'source_address_key' do
    d = create_driver(base_config + %!
      format none
      source_address_key addr
    !)
    address = nil
    d.run(expect_records: 1) do
      create_udp_socket('127.0.0.1', @port) do |u|
        u.send("test", 0)
        address = u.peeraddr[3]
      end
    end

    assert_equal 1, d.events.size
    assert_equal "udp", d.events[0][0]
    assert d.events[0][1].is_a?(Fluent::EventTime)
    assert_equal address, d.events[0][2]['addr']
  end

  test 'receive_buffer_size' do
    # doesn't check exact value because it depends on platform and condition

    # check if default socket and in_udp's one without receive_buffer_size have same size buffer
    d0 = create_driver(base_config + %!
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
    d1 = create_driver(base_config + %!
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

  test 'message_length_limit' do
    message_length_limit = 32
    d = create_driver(base_config + %!
      format none
      message_length_limit #{message_length_limit}
    !)
    d.run(expect_records: 3) do
      create_udp_socket('127.0.0.1', @port) do |u|
        3.times do |i|
          u.send("#{i}" * 40 + "\n", 0)
        end
      end
    end

    if Fluent.windows?
      expected_records = []
    else
      expected_records = 3.times.collect do |i|
        "#{i}" * message_length_limit
      end
    end
    actual_records = d.events.collect do |event|
      event[2]["message"]
    end

    assert_equal expected_records, actual_records
  end
end
