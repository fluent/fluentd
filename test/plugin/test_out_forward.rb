require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_forward'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TARGET_HOST = '127.0.0.1'
  TARGET_PORT = 13999
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
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::ForwardOutput) {
      attr_reader :responses, :exceptions

      def initialize
        super
        @responses = []
        @exceptions = []
      end

      def send_data(node, tag, chunk)
        # Original #send_data returns nil when it does not wait for responses or when on response timeout.
        @responses << super(node, tag, chunk)
      rescue => e
        @exceptions << e
        raise e
      end
    }.configure(conf)
  end

  def test_configure
    d = create_driver(%[
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
    assert_equal 13999, node.port
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
    assert_equal true, d.instance.nodes.first.conf.dns_round_robin

    d = create_driver(CONFIG + "\nheartbeat_type none\ndns_round_robin true")
    assert_equal true, d.instance.dns_round_robin
    assert_equal true, d.instance.nodes.first.conf.dns_round_robin
  end

  def test_configure_no_server
    assert_raise(Fluent::ConfigError, 'forward output plugin requires at least one <server> is required') do
      create_driver('')
    end
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
    assert_equal false, d.instance.extend_internal_protocol
    assert_equal false, d.instance.require_ack_response
    assert_equal 190, d.instance.ack_response_timeout

    d = create_driver(CONFIG + %[
      require_ack_response true
      ack_response_timeout 2s
    ])
    assert d.instance.extend_internal_protocol
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

  def test_send_to_a_node_supporting_responses
    target_input_driver = create_target_input_driver(true)

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
    target_input_driver = create_target_input_driver(true)

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
    # in_forward, that doesn't support ack feature, and keep connection alive
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
    target_input_driver = create_target_input_driver(false, true)

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

    assert_raise Fluent::ForwardOutputConnectionClosedError do
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

  def create_target_input_driver(do_respond=false, disconnect=false, conf=TARGET_CONFIG)
    require 'fluent/plugin/in_forward'

    # TODO: Support actual TCP heartbeat test
    DummyEngineDriver.new(Fluent::ForwardInput) {
      handler_class = Class.new(Fluent::ForwardInput::Handler) { |klass|
        attr_reader :chunk_counter # for checking if received data is successfully deserialized

        def initialize(sock, log, on_message)
          @sock = sock
          @log = log
          @chunk_counter = 0
          @on_message = on_message
          @source = nil
        end

        if do_respond
          def write(data)
            @sock.write data
          rescue
            @sock.close_write
            @sock.close
          end
        else
          def write(data)
            # do nothing
          end
        end

        def close
          unless @sock.closed?
            @sock.close_write
            @sock.close
          end
        end
      }

      define_method(:start) do
        @thread = Thread.new do
          Socket.tcp_server_loop(@bind, @port) do |sock, client_addrinfo|
            begin
              handler = handler_class.new(sock, @log, method(:on_message))
              loop do
                raw_data = sock.recv(1024)
                handler.on_read(raw_data)
                # chunk_counter is reset to zero only after all the data have been received and successfully deserialized.
                break if handler.chunk_counter == 0
                break if sock.closed?
              end
              if disconnect
                handler.close
                sock = nil
              end
              sleep  # wait for connection to be closed by client
            ensure
              if sock && !sock.closed?
                sock.close_write
                sock.close
              end
            end
          end
        end
      end

      def shutdown
        @thread.kill
        @thread.join
      end
    }.configure(conf).inject_router()
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

  class DummyEngineDriver < Fluent::Test::TestDriver
    def initialize(klass, &block)
      super(klass, &block)
      @engine = DummyEngineClass.new
    end

    def inject_router
      @instance.router = @engine
      self
    end

    def run(&block)
      super(&block)
    end

    def emits
      all = []
      @engine.emit_streams.each {|tag,events|
        events.each {|time,record|
          all << [tag, time, record]
        }
      }
      all
    end

    class DummyEngineClass
      attr_reader :emit_streams

      def initialize
        @emit_streams ||= []
      end

      def clear!
        @emit_streams = []
      end

      def emit_stream(tag, es)
        @emit_streams << [tag, es.to_a]
      end
    end
  end
end
