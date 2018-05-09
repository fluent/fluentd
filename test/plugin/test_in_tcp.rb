require_relative '../helper'
require 'fluent/test/driver/input'
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

  def create_tcp_socket(host, port, &block)
    if block_given?
      TCPSocket.open(host, port, &block)
    else
      TCPSocket.open(host, port)
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
    assert_equal "\n", d.instance.delimiter
  end

  test_case_data = {
    'none' => {
      'format' => 'none',
      'payloads' => [ "tcptest1\n", "tcptest2\n" ],
      'expecteds' => [
        {'message' => 'tcptest1'},
        {'message' => 'tcptest2'},
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
        {'k' => 'tcptest2', 'message' => 456}
      ],
    },
  }

  data(test_case_data)
  test 'test_msg_size' do |data|
    format = data['format']
    payloads = data['payloads']
    expecteds = data['expecteds']

    d = create_driver(BASE_CONFIG + "format #{format}")
    d.run(expect_records: 2) do
      payloads.each do |payload|
        create_tcp_socket('127.0.0.1', PORT) do |sock|
          sock.send(payload, 0)
        end
      end
    end

    assert_equal 2, d.events.size
    expecteds.each_with_index do |expected_record, i|
      assert_equal "tcp", d.events[i][0]
      assert d.events[i][1].is_a?(Fluent::EventTime)
      assert_equal expected_record, d.events[i][2]
    end
  end

  data(test_case_data)
  test 'test data in a connection' do |data|
    format = data['format']
    payloads = data['payloads']
    expecteds = data['expecteds']

    d = create_driver(BASE_CONFIG + "format #{format}")
    d.run(expect_records: 2) do
      create_tcp_socket('127.0.0.1', PORT) do |sock|
        payloads.each do |payload|
          sock.send(payload, 0)
        end
      end
    end

    assert_equal 2, d.events.size
    expecteds.each_with_index do |expected_record, i|
      assert_equal "tcp", d.events[i][0]
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
      create_tcp_socket('127.0.0.1', PORT) do |sock|
        sock.do_not_reverse_lookup = false
        hostname = sock.peeraddr[2]
        sock.send("test\n", 0)
      end
    end

    assert_equal 1, d.events.size
    event = d.events[0]
    assert_equal "tcp", event[0]
    assert event[1].is_a?(Fluent::EventTime)
    assert_equal hostname, event[2]['host']
  end
end
