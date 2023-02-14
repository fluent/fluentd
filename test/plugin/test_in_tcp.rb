require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_tcp'

class TcpInputTest < Test::Unit::TestCase
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
      tag tcp
    ]
  end

  def ipv4_config
    base_config + %[
      bind 127.0.0.1
      format none
    ]
  end

  def ipv6_config
    base_config + %[
      bind ::1
      format none
    ]
  end

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
    assert_equal "\n", d.instance.delimiter
  end

  test ' configure w/o parse section' do
    assert_raise(Fluent::ConfigError.new("<parse> section is required.")) {
      create_driver(base_config)
    }
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

    d = create_driver(base_config + "format #{format}")
    d.run(expect_records: 2) do
      payloads.each do |payload|
        create_tcp_socket('127.0.0.1', @port) do |sock|
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

    d = create_driver(base_config + "format #{format}")
    d.run(expect_records: 2) do
      create_tcp_socket('127.0.0.1', @port) do |sock|
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
    d = create_driver(base_config + %!
      format none
      source_hostname_key host
    !)
    hostname = nil
    d.run(expect_records: 1) do
      create_tcp_socket('127.0.0.1', @port) do |sock|
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

  test "send_keepalive_packet_can_be_enabled" do
    d = create_driver(base_config + %!
      format none
      send_keepalive_packet true
    !)
    assert_true d.instance.send_keepalive_packet

    d = create_driver(base_config + %!
      format none
    !)
    assert_false d.instance.send_keepalive_packet
  end

  test 'source_address_key' do
    d = create_driver(base_config + %!
      format none
      source_address_key addr
    !)
    address = nil
    d.run(expect_records: 1) do
      create_tcp_socket('127.0.0.1', @port) do |sock|
        address = sock.peeraddr[3]
        sock.send("test\n", 0)
      end
    end

    assert_equal 1, d.events.size
    event = d.events[0]
    assert_equal "tcp", event[0]
    assert event[1].is_a?(Fluent::EventTime)
    assert_equal address, event[2]['addr']
  end

  sub_test_case '<security>' do
    test 'accept from allowed client' do
      d = create_driver(ipv4_config + %!
        <security>
          <client>
            network 127.0.0.1
          </client>
        </security>
      !)
      d.run(expect_records: 1) do
        create_tcp_socket('127.0.0.1', @port) do |sock|
          sock.send("hello\n", 0)
        end
      end

      assert_equal 1, d.events.size
      event = d.events[0]
      assert_equal 'tcp', event[0]
      assert_equal 'hello', event[2]['message']
    end

    test 'deny from disallowed client' do
      d = create_driver(ipv4_config + %!
        <security>
          <client>
            network 200.0.0.0
          </client>
        </security>
      !)
      d.run(shutdown: false, expect_records: 1, timeout: 2) do
        create_tcp_socket('127.0.0.1', @port) do |sock|
          sock.send("hello\n", 0)
        end
      end

      assert_equal 1, d.instance.log.logs.count { |l| l =~ /anonymous client/ }
      assert_equal 0, d.events.size
    ensure
      d.instance_shutdown if d&.instance
    end
  end

  sub_test_case '<extract>' do
    test 'extract tag from record field' do
      d = create_driver(base_config + %!
        <parse>
          @type json
        </parse>
        <extract>
          tag_key tag
        </extract>
      !)
      d.run(expect_records: 1) do
        create_tcp_socket('127.0.0.1', @port) do |sock|
          data = {'msg' => 'hello', 'tag' => 'helper_test'}
          sock.send("#{data.to_json}\n", 0)
        end
      end

      assert_equal 1, d.events.size
      event = d.events[0]
      assert_equal 'helper_test', event[0]
      assert event[1].is_a?(Fluent::EventTime)
      assert_equal 'hello', event[2]['msg']
    end
  end
end
