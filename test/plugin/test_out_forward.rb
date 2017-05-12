require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_forward'
require 'flexmock/test_unit'

require 'fluent/test/driver/input'
require 'fluent/plugin/in_forward'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @d = nil
  end

  def teardown
    @d.instance_shutdown if @d
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
    Fluent::Test::Driver::Output.new(Fluent::Plugin::ForwardOutput) {
      attr_reader :response_chunk_ids, :exceptions, :sent_chunk_ids

      def initialize
        super
        @sent_chunk_ids = []
        @response_chunk_ids = []
        @exceptions = []
      end

      def try_write(chunk)
        retval = super
        @sent_chunk_ids << chunk.unique_id
        retval
      end

      def read_ack_from_sock(sock, unpacker)
        retval = super
        @response_chunk_ids << retval
        retval
      rescue => e
        @exceptions << e
        raise e
      end
    }.configure(conf)
  end

  data('ack true' => true,
       'ack false' => false)
  test 'TLS transport and ack parameter combination' do |ack|
    input_conf = TARGET_CONFIG + %[
                   <transport tls>
                     insecure true
                   </transport>
                 ]
    target_input_driver = create_target_input_driver(conf: input_conf)

    output_conf = %[
      send_timeout 5
      require_ack_response #{ack}
      ack_response_timeout 1s
      transport tls
      tls_insecure_mode true
      <server>
        host #{TARGET_HOST}
        port #{TARGET_PORT}
      </server>
      <buffer>
        #flush_mode immediate
        flush_interval 0s
        flush_at_shutdown false # suppress errors in d.instance_shutdown
      </buffer>
    ]
    @d = d = create_driver(output_conf)

    time = event_time("2011-01-02 13:14:15 UTC")
    records = [{"a" => 1}, {"a" => 2}]
    target_input_driver.run(expect_records: 2, timeout: 3) do
      #d.run(default_tag: 'test', wait_flush_completion: false, shutdown: false) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
        end
      end
    end

    events = target_input_driver.events
    assert{ events != [] }
    assert_equal(['test', time, records[0]], events[0])
    assert_equal(['test', time, records[1]], events[1])
  end

  def create_target_input_driver(response_stub: nil, disconnect: false, conf: TARGET_CONFIG)
    require 'fluent/plugin/in_forward'

    # TODO: Support actual TCP heartbeat test
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ForwardInput) {
      if response_stub.nil?
        # do nothing because in_forward responds for ack option in default
      else
        define_method(:response) do |options|
          return response_stub.(options)
        end
      end
    }.configure(conf)
  end
end
