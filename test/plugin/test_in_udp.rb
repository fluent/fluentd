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
    assert_equal 4096, d.instance.body_size_limit
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
end
