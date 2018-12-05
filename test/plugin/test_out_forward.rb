require_relative '../helper'
require 'fluent/test'
require 'fluent/test/startup_shutdown'
require 'fluent/plugin/out_forward'
require 'flexmock/test_unit'

class ForwardOutputTest < Test::Unit::TestCase
  extend Fluent::Test::StartupShutdown

  def setup
    Fluent::Test.setup
  end

  TARGET_HOST = '127.0.0.1'
  TARGET_PORT = unused_port
  CONFIG = %[
    send_timeout 51
    heartbeat_type udp
    <server>
      name test
      host #{TARGET_HOST}
      port #{TARGET_PORT}
    </server>
  ]

  TARGET_CONFIG = %[
    port #{TARGET_PORT}
    bind #{TARGET_HOST}
  ]

  def create_driver(conf=CONFIG)
    d = Fluent::Test::BufferedOutputTestDriver.new(Fluent::ForwardOutput) {
      attr_reader :responses, :exceptions

      def initialize
        super
        @responses = []
        @exceptions = []
      end

      def configure(conf)
        super
        m = Module.new do
          def send_data(tag, chunk)
            @sender.responses << super
          rescue => e
            @sender.exceptions << e
            raise e
          end
        end
        @nodes.each do |node|
          node.singleton_class.prepend(m)
        end
      end
    }.configure(conf)
    router = Object.new
    def router.method_missing(name, *args, **kw_args, &block)
      Engine.root_agent.event_router.__send__(name, *args, **kw_args, &block)
    end
    d.instance.router = router
    d
  end

  def test_configure
    d = create_driver(%[
      self_hostname localhost
      <server>
        name test
        host #{TARGET_HOST}
        port #{TARGET_PORT}
      </server>
    ])
    nodes = d.instance.nodes
    assert_equal 60, d.instance.send_timeout
    assert_equal :tcp, d.instance.heartbeat_type
    assert_equal 1, nodes.length
    node = nodes.first
    assert_equal "test", node.name
    assert_equal '127.0.0.1', node.host
    assert_equal TARGET_PORT, node.port
  end

  def test_configure_udp_heartbeat
    d = create_driver(CONFIG + "\nheartbeat_type udp")
    assert_equal :udp, d.instance.heartbeat_type
  end

  def test_configure_none_heartbeat
    d = create_driver(CONFIG + "\nheartbeat_type none")
    assert_equal :none, d.instance.heartbeat_type
  end

  def test_configure_dns_round_robin
    assert_raise(Fluent::ConfigError) do
      create_driver(CONFIG + "\nheartbeat_type udp\ndns_round_robin true")
    end

    d = create_driver(CONFIG + "\nheartbeat_type tcp\ndns_round_robin true")
    assert_equal true, d.instance.dns_round_robin

    d = create_driver(CONFIG + "\nheartbeat_type none\ndns_round_robin true")
    assert_equal true, d.instance.dns_round_robin
  end

  def test_configure_no_server
    assert_raise(Fluent::ConfigError, 'forward output plugin requires at least one <server> is required') do
      create_driver('')
    end
  end

  def test_compress_default_value
    d = create_driver
    assert_equal :text, d.instance.compress

    node = d.instance.nodes.first
    assert_equal :text, node.instance_variable_get(:@compress)
  end

  def test_set_compress_is_gzip
    d = create_driver(CONFIG + %[compress gzip])
    assert_equal :gzip, d.instance.compress
    assert_equal :gzip, d.instance.buffer.compress

    node = d.instance.nodes.first
    assert_equal :gzip, node.instance_variable_get(:@compress)
  end

  def test_set_compress_is_gzip_in_buffer_section
    mock = flexmock($log)
    mock.should_receive(:log).with("buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>")

    d = create_driver(CONFIG + %[
       <buffer>
         type memory
         compress gzip
       </buffer>
     ])
    assert_equal :text, d.instance.compress
    assert_equal :gzip, d.instance.buffer.compress

    node = d.instance.nodes.first
    assert_equal :text, node.instance_variable_get(:@compress)
  end

  def test_phi_failure_detector
    d = create_driver(CONFIG + %[phi_failure_detector false \n phi_threshold 0])
    node = d.instance.nodes.first
    stub(node.failure).phi { raise 'Should not be called' }
    node.tick
    assert_equal node.available, true

    d = create_driver(CONFIG + %[phi_failure_detector true \n phi_threshold 0])
    node = d.instance.nodes.first
    node.tick
    assert_equal node.available, false
  end

  def test_wait_response_timeout_config
    d = create_driver(CONFIG)
    assert_equal false, d.instance.require_ack_response
    assert_equal 190, d.instance.ack_response_timeout

    d = create_driver(CONFIG + %[
      require_ack_response true
      ack_response_timeout 2s
    ])
    assert d.instance.require_ack_response
    assert_equal 2, d.instance.ack_response_timeout
  end

  def test_send_with_time_as_integer
    target_input_driver = create_target_input_driver

    d = create_driver(CONFIG + %[flush_interval 1s])

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end

    target_input_driver.run do
      d.run do
        records.each do |record|
          d.emit record, time
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]
    assert(emits[0][1].is_a?(Integer))
    assert(emits[1][1].is_a?(Integer))

    assert_equal [nil], d.instance.responses # not attempt to receive responses, so nil is returned
    assert_empty d.instance.exceptions
  end

  def test_send_without_time_as_integer
    target_input_driver = create_target_input_driver

    d = create_driver(CONFIG + %[
      flush_interval 1s
      time_as_integer false
    ])

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end

    target_input_driver.run do
      d.run do
        records.each do |record|
          d.emit record, time
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]
    assert_equal_event_time(time, emits[0][1])
    assert_equal_event_time(time, emits[1][1])

    assert_equal [nil], d.instance.responses # not attempt to receive responses, so nil is returned
    assert_empty d.instance.exceptions
  end

  def test_send_comprssed_message_pack_stream_if_compress_is_gzip
    target_input_driver = create_target_input_driver

    d = create_driver(CONFIG + %[
      flush_interval 1s
      compress gzip
    ])

    time = event_time('2011-01-02 13:14:15 UTC')

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end

    target_input_driver.run do
      d.run do
        records.each do |record|
          d.emit record, time
        end
      end
    end

    event_streams = target_input_driver.event_streams
    assert_true event_streams[0].is_a?(Fluent::CompressedMessagePackEventStream)

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]
  end

  def test_send_to_a_node_supporting_responses
    target_input_driver = create_target_input_driver(response_stub: true)

    d = create_driver(CONFIG + %[flush_interval 1s])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end

    target_input_driver.run do
      d.run do
        records.each do |record|
          d.emit record, time
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]

    assert_equal [nil], d.instance.responses # not attempt to receive responses, so nil is returned
    assert_empty d.instance.exceptions
  end

  def test_send_to_a_node_not_supporting_responses
    target_input_driver = create_target_input_driver

    d = create_driver(CONFIG + %[flush_interval 1s])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end

    target_input_driver.run do
      d.run do
        records.each do |record|
          d.emit record, time
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]

    assert_equal [nil], d.instance.responses # not attempt to receive responses, so nil is returned
    assert_empty d.instance.exceptions
  end

  def test_require_a_node_supporting_responses_to_respond_with_ack
    target_input_driver = create_target_input_driver

    d = create_driver(CONFIG + %[
      flush_interval 1s
      require_ack_response true
      ack_response_timeout 1s
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end

    target_input_driver.run do
      d.run do
        records.each do |record|
          d.emit record, time
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]

    assert_equal 1, d.instance.responses.length
    assert d.instance.responses[0].has_key?('ack')
    assert_empty d.instance.exceptions
  end

  def test_require_a_node_not_supporting_responses_to_respond_with_ack
    target_input_driver = create_target_input_driver(response_stub: ->(_option) { nil }, disconnect: true)

    d = create_driver(CONFIG + %[
      flush_interval 1s
      require_ack_response true
      ack_response_timeout 1s
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end
    d.run_timeout = 2

    assert_raise Fluent::ForwardOutputACKTimeoutError do
      target_input_driver.run do
        d.run do
          records.each do |record|
            d.emit record, time
          end
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]

    node = d.instance.nodes.first
    assert_equal false, node.available # node is regarded as unavailable when timeout

    assert_empty d.instance.responses # send_data() raises exception, so response is missing
    assert_equal 1, d.instance.exceptions.size
  end

  # bdf1f4f104c00a791aa94dc20087fe2011e1fd83
  def test_require_a_node_not_supporting_responses_2_to_respond_with_ack
    # in_forward, that doesn't support ack feature, and disconnect immediately
    target_input_driver = create_target_input_driver(response_stub: ->(_option) { nil }, disconnect: true)

    d = create_driver(CONFIG + %[
      flush_interval 1s
      require_ack_response true
      ack_response_timeout 5s
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    d.register_run_post_condition do
      d.instance.responses.length == 1
    end
    d.run_timeout = 2

    assert_raise Fluent::ForwardOutputACKTimeoutError do
      target_input_driver.run do
        d.run do
          records.each do |record|
            d.emit record, time
          end
        end
      end
    end

    emits = target_input_driver.emits
    assert_equal ['test', time, records[0]], emits[0]
    assert_equal ['test', time, records[1]], emits[1]

    assert_equal 0, d.instance.responses.size
    assert_equal 1, d.instance.exceptions.size # send_data() fails and to be retried

    node = d.instance.nodes.first
    assert_equal false, node.available # node is regarded as unavailable when unexpected EOF
  end

  def test_authentication_with_shared_key
    input_conf = TARGET_CONFIG + %[
                   <security>
                     self_hostname in.localhost
                     shared_key fluentd-sharedkey
                     <client>
                       host 127.0.0.1
                     </client>
                   </security>
                 ]
    target_input_driver = create_target_input_driver(conf: input_conf)

    output_conf = %[
      send_timeout 51
      <security>
        self_hostname localhost
        shared_key fluentd-sharedkey
      </security>
      <server>
        name test
        host #{TARGET_HOST}
        port #{TARGET_PORT}
        shared_key fluentd-sharedkey
      </server>
    ]
    d = create_driver(output_conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    records = [
      {"a" => 1},
      {"a" => 2}
    ]

    target_input_driver.run do
      sleep 0.1 until target_input_driver.instance.instance_eval{ @thread } && target_input_driver.instance.instance_eval{ @thread }.status

      d.run do
        records.each do |record|
          d.emit(record, time)
        end

        # run_post_condition of Fluent::Test::*Driver are buggy:
        t = Time.now
        while Time.now < t + 15 && target_input_driver.emits.size < 2
          sleep 0.1
        end
      end
    end

    emits = target_input_driver.emits
    assert{ emits != [] }
    assert_equal(['test', time, records[0]], emits[0])
    assert_equal(['test', time, records[1]], emits[1])
  end

  def test_authentication_with_user_auth
    input_conf = TARGET_CONFIG + %[
                   <security>
                     self_hostname in.localhost
                     shared_key fluentd-sharedkey
                     user_auth true
                     <user>
                       username fluentd
                       password fluentd
                     </user>
                     <client>
                       host 127.0.0.1
                     </client>
                   </security>
                 ]
    target_input_driver = create_target_input_driver(conf: input_conf)

    output_conf = %[
      send_timeout 51
      <security>
        self_hostname localhost
        shared_key fluentd-sharedkey
      </security>
      <server>
        name test
        host #{TARGET_HOST}
        port #{TARGET_PORT}
        shared_key fluentd-sharedkey
        username fluentd
        password fluentd
      </server>
    ]
    d = create_driver(output_conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    records = [
      {"a" => 1},
      {"a" => 2}
    ]

    target_input_driver.run do
      sleep 0.1 until target_input_driver.instance.instance_eval{ @thread } && target_input_driver.instance.instance_eval{ @thread }.status

      d.run do
        records.each do |record|
          d.emit(record, time)
        end

        # run_post_condition of Fluent::Test::*Driver are buggy:
        t = Time.now
        while Time.now < t + 15 && target_input_driver.emits.size < 2
          sleep 0.1
        end
      end
    end

    emits = target_input_driver.emits
    assert{ emits != [] }
    assert_equal(['test', time, records[0]], emits[0])
    assert_equal(['test', time, records[1]], emits[1])
  end

  def create_target_input_driver(response_stub: nil, disconnect: false, conf: TARGET_CONFIG)
    require 'fluent/plugin/in_forward'

    # TODO: Support actual TCP heartbeat test
    Fluent::Test::InputTestDriver.new(Fluent::ForwardInput) {
      if response_stub.nil?
        # do nothing because in_forward responds for ack option in default
      else
        define_method(:response) do |options|
          return response_stub.(options)
        end
      end
    }.configure(conf)
  end

  def test_heartbeat_type_none
    d = create_driver(CONFIG + "\nheartbeat_type none")
    node = d.instance.nodes.first
    assert_equal Fluent::ForwardOutput::NoneHeartbeatNode, node.class

    d.instance.start
    assert_nil d.instance.instance_variable_get(:@loop)   # no HeartbeatHandler, or HeartbeatRequestTimer
    assert_nil d.instance.instance_variable_get(:@thread) # no HeartbeatHandler, or HeartbeatRequestTimer

    stub(node.failure).phi { raise 'Should not be called' }
    node.tick
    assert_equal node.available, true
  end

  def test_heartbeat_type_udp
    d = create_driver(CONFIG + "\nheartbeat_type udp")

    d.instance.start
    usock = d.instance.instance_variable_get(:@usock)
    timer = d.instance.instance_variable_get(:@timer)
    hb = d.instance.instance_variable_get(:@hb)
    assert_equal UDPSocket, usock.class
    assert_equal Fluent::ForwardOutput::HeartbeatRequestTimer, timer.class
    assert_equal Fluent::ForwardOutput::HeartbeatHandler, hb.class

    mock(usock).send("\0", 0, Socket.pack_sockaddr_in(TARGET_PORT, '127.0.0.1')).once
    timer.disable # call send_heartbeat at just once
    timer.on_timer
  end
end
