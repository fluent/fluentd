require_relative '../helper'
require 'fluent/test'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TARGET_HOST = '127.0.0.1'
  TARGET_PORT = 13999
  CONFIG = %[
    send_timeout 51
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
    Fluent::Test::OutputTestDriver.new(Fluent::ForwardOutput) {
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
    d = create_driver
    nodes = d.instance.nodes
    assert_equal 51, d.instance.send_timeout
    assert_equal :udp, d.instance.heartbeat_type
    assert_equal 1, nodes.length
    node = nodes.first
    assert_equal "test", node.name
    assert_equal '127.0.0.1', node.host
    assert_equal 13999, node.port
  end

  def test_configure_tcp_heartbeat
    d = create_driver(CONFIG + "\nheartbeat_type tcp")
    assert_equal :tcp, d.instance.heartbeat_type
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

    node = d.instance.nodes.first
    assert_equal false, node.available # node is regarded as unavailable when timeout

    assert_empty d.instance.responses # send_data() raises exception, so response is missing
    assert_equal 1, d.instance.exceptions.size
  end

  def create_target_input_driver(do_respond=false, conf=TARGET_CONFIG)
    require 'fluent/plugin/in_forward'

    DummyEngineDriver.new(Fluent::ForwardInput) {
      handler_class = Class.new(Fluent::ForwardInput::Handler) { |klass|
        attr_reader :chunk_counter # for checking if received data is successfully deserialized

        def initialize(sock, log, on_message)
          @sock = sock
          @log = log
          @chunk_counter = 0
          @on_message = on_message
        end

        if do_respond
          def write(data)
            @sock.write data
          rescue => e
            @sock.close
          end
        else
          def write(data)
            # do nothing
          end
        end

        def close
          @sock.close
        end
      }

      define_method(:start) do
        @thread = Thread.new do
          Socket.tcp_server_loop(@host, @port) do |sock, client_addrinfo|
            begin
              handler = handler_class.new(sock, @log, method(:on_message))
              loop do
                raw_data = sock.recv(1024)
                handler.on_read(raw_data)
                # chunk_counter is reset to zero only after all the data have been received and successfully deserialized.
                break if handler.chunk_counter == 0
              end
              sleep  # wait for connection to be closed by client
            ensure
              sock.close
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

  class DummyEngineDriver < Fluent::Test::TestDriver
    def initialize(klass, &block)
      super(klass, &block)
      @engine = DummyEngineClass.new
      @klass = klass
      # To avoid accessing Fluent::Engine, set Engine as a plugin's class constant (Fluent::SomePlugin::Engine).
      # But this makes it impossible to run tests concurrently by threading in a process.
      @klass.const_set(:Engine, @engine)
    end

    def inject_router
      @instance.router = @engine
      self
    end

    def run(&block)
      super(&block)
      @klass.class_eval do
        remove_const(:Engine)
      end
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

      def emit_stream(tag, es)
        @emit_streams << [tag, es.to_a]
      end
    end
  end
end
