require_relative 'helper'

class UnusedPortTest < Test::Unit::TestCase
  test "unused_port(protocol: :all) returns a port bindable for both TCP and UDP" do
    port = unused_port(protocol: :all)
    assert_kind_of(Integer, port)
    assert_include(PORT_RANGE_TCP_UDP, port)
    tcp = TCPServer.open("0.0.0.0", port)
    tcp.close
    udp = UDPSocket.new(::Socket::AF_INET)
    udp.bind("0.0.0.0", port)
    udp.close
  end

  test "unused_port_tcp_udp does not support num > 1" do
    assert_raise(RuntimeError.new("not support num > 1")) do
      unused_port_tcp_udp(2)
    end
  end

  sub_test_case "port_bindable_tcp?" do
    test "returns false while the port is held and true after release" do
      held = TCPServer.open("0.0.0.0", 0)
      port = held.addr[1]
      assert_false(port_bindable_tcp?(port))
      held.close
      assert_true(port_bindable_tcp?(port))
    end
  end

  sub_test_case "port_bindable_udp?" do
    test "returns false while the port is held and true after release" do
      held = UDPSocket.new(::Socket::AF_INET)
      held.bind("0.0.0.0", 0)
      port = held.addr[1]
      assert_false(port_bindable_udp?(port))
      held.close
      assert_true(port_bindable_udp?(port))
    end
  end

  sub_test_case "when candidate ports fail the checks" do
    teardown do
      [:port_bindable_udp?, :port_bindable_tcp?].each do |name|
        if singleton_class.method_defined?(name, false)
          singleton_class.send(:remove_method, name)
        end
      end
    end

    test "tries another candidate and succeeds" do
      calls = 0
      candidates = []
      define_singleton_method(:port_bindable_udp?) { |_port| true }
      define_singleton_method(:port_bindable_tcp?) do |port|
        calls += 1
        candidates << port
        calls >= 3
      end

      port = unused_port_tcp_udp
      assert_equal(3, calls)
      assert_equal(candidates.last, port)
      candidates.each do |candidate|
        assert_include(PORT_RANGE_TCP_UDP, candidate)
      end
    end

    test "raises after exhausting retries when the TCP side never succeeds" do
      calls = 0
      define_singleton_method(:port_bindable_udp?) { |_port| true }
      define_singleton_method(:port_bindable_tcp?) do |_port|
        calls += 1
        false
      end

      error = assert_raise(RuntimeError) do
        unused_port_tcp_udp(retries: 3)
      end
      assert_equal("can't find unused port", error.message)
      assert_equal(3, calls)
    end

    test "raises after exhausting retries when the UDP side never succeeds" do
      udp_calls = 0
      tcp_calls = 0
      define_singleton_method(:port_bindable_udp?) do |_port|
        udp_calls += 1
        false
      end
      define_singleton_method(:port_bindable_tcp?) do |_port|
        tcp_calls += 1
        true
      end

      error = assert_raise(RuntimeError) do
        unused_port_tcp_udp(retries: 3)
      end
      assert_equal("can't find unused port", error.message)
      assert_equal(3, udp_calls)
      assert_equal(0, tcp_calls)
    end
  end
end
