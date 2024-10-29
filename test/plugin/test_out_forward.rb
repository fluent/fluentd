require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_forward'
require 'flexmock/test_unit'

require 'fluent/test/driver/input'
require 'fluent/plugin/in_forward'

class ForwardOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
    @d = nil
    # forward plugin uses TCP and UDP sockets on the same port number
    @target_port = unused_port(protocol: :all)
  end

  def teardown
    @d.instance_shutdown if @d
    @port = nil
  end

  TMP_DIR = File.join(__dir__, "../tmp/out_forward#{ENV['TEST_ENV_NUMBER']}")

  TARGET_HOST = '127.0.0.1'

  def config
    %[
      send_timeout 51
      heartbeat_type udp
      <server>
        name test
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ]
  end

  def target_config
    %[
      port #{@target_port}
      bind #{TARGET_HOST}
    ]
  end

  def create_driver(conf=config)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::ForwardOutput) {
      attr_reader :sent_chunk_ids, :ack_handler, :discovery_manager

      def initialize
        super
        @sent_chunk_ids = []
      end

      def try_write(chunk)
        retval = super
        @sent_chunk_ids << chunk.unique_id
        retval
      end
    }.configure(conf)
  end

  test 'configure' do
    @d = d = create_driver(%[
      self_hostname localhost
      <server>
        name test
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ])
    nodes = d.instance.nodes
    assert_equal 60, d.instance.send_timeout
    assert_equal :transport, d.instance.heartbeat_type
    assert_equal 1, nodes.length
    assert_nil d.instance.connect_timeout
    node = nodes.first
    assert_equal "test", node.name
    assert_equal '127.0.0.1', node.host
    assert_equal @target_port, node.port
  end

  test 'configure_traditional' do
    @d = d = create_driver(<<EOL)
      self_hostname localhost
      <server>
        name test
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
      buffer_chunk_limit 10m
EOL
    instance = d.instance
    assert instance.chunk_key_tag
    assert !instance.chunk_key_time
    assert_equal [], instance.chunk_keys
    assert{ instance.buffer.is_a?(Fluent::Plugin::MemoryBuffer) }
    assert_equal( 10*1024*1024, instance.buffer.chunk_limit_size )
  end

  test 'configure timeouts' do
    @d = d = create_driver(%[
      send_timeout 30
      connect_timeout 10
      hard_timeout 15
      ack_response_timeout 20
      <server>
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ])
    assert_equal 30, d.instance.send_timeout
    assert_equal 10, d.instance.connect_timeout
    assert_equal 15, d.instance.hard_timeout
    assert_equal 20, d.instance.ack_response_timeout
  end

  test 'configure_udp_heartbeat' do
    @d = d = create_driver(config + "\nheartbeat_type udp")
    assert_equal :udp, d.instance.heartbeat_type
  end

  test 'configure_none_heartbeat' do
    @d = d = create_driver(config + "\nheartbeat_type none")
    assert_equal :none, d.instance.heartbeat_type
  end

  test 'configure_expire_dns_cache' do
    @d = d = create_driver(config + "\nexpire_dns_cache 5")
    assert_equal 5, d.instance.expire_dns_cache
  end

  test 'configure_dns_round_robin udp' do
    assert_raise(Fluent::ConfigError) do
      create_driver(config + "\nheartbeat_type udp\ndns_round_robin true")
    end
  end

  test 'configure_dns_round_robin transport' do
    @d = d = create_driver(config + "\nheartbeat_type transport\ndns_round_robin true")
    assert_equal true, d.instance.dns_round_robin
  end

  test 'configure_dns_round_robin none' do
    @d = d = create_driver(config + "\nheartbeat_type none\ndns_round_robin true")
    assert_equal true, d.instance.dns_round_robin
  end

  test 'configure_no_server' do
    assert_raise(Fluent::ConfigError, 'forward output plugin requires at least one <server> is required') do
      create_driver('')
    end
  end

  test 'configure with ignore_network_errors_at_startup' do
    normal_conf = config_element('match', '**', {}, [
        config_element('server', '', {'name' => 'test', 'host' => 'unexisting.yaaaaaaaaaaaaaay.host.example.com'})
      ])

    if Socket.const_defined?(:ResolutionError) # as of Ruby 3.3
      error_class = Socket::ResolutionError
    else
      error_class = SocketError
    end

    assert_raise error_class do
      create_driver(normal_conf)
    end

    conf = config_element('match', '**', {'ignore_network_errors_at_startup' => 'true'}, [
        config_element('server', '', {'name' => 'test', 'host' => 'unexisting.yaaaaaaaaaaaaaay.host.example.com'})
      ])
    @d = d = create_driver(conf)
    expected_log = "failed to resolve node name when configured"
    expected_detail = "server=\"test\" error_class=#{error_class.name}"
    logs = d.logs
    assert{ logs.any?{|log| log.include?(expected_log) && log.include?(expected_detail) } }
  end

  data('CA cert'     => 'tls_ca_cert_path',
       'non CA cert' => 'tls_cert_path')
  test 'configure tls_cert_path/tls_ca_cert_path' do |param|
    dummy_cert_path = File.join(TMP_DIR, "dummy_cert.pem")
    FileUtils.touch(dummy_cert_path)
    conf = %[
      send_timeout 5
      transport tls
      tls_insecure_mode true
      #{param} #{dummy_cert_path}
      <server>
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ]

    @d = d = create_driver(conf)
    # In the plugin, tls_ca_cert_path is used for both cases
    assert_equal([dummy_cert_path], d.instance.tls_ca_cert_path)
  end

  sub_test_case "certstore loading parameters for Windows" do
    test 'certstore related config parameters' do
      omit "certstore related values raise error on not Windows" if Fluent.windows?
      conf = %[
        send_timeout 5
        transport tls
        tls_cert_logical_store_name Root
        tls_cert_thumbprint a909502dd82ae41433e6f83886b00d4277a32a7b
        <server>
          host #{TARGET_HOST}
          port #{@target_port}
        </server>
      ]

      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test 'cert_logical_store_name and tls_cert_thumbprint default values' do
      conf = %[
        send_timeout 5
        transport tls
        <server>
          host #{TARGET_HOST}
          port #{@target_port}
        </server>
      ]

      @d = d = create_driver(conf)
      assert_nil d.instance.tls_cert_logical_store_name
      assert_nil d.instance.tls_cert_thumbprint
    end

    data('CA cert'     => 'tls_ca_cert_path',
         'non CA cert' => 'tls_cert_path')
    test 'specify tls_cert_logical_store_name and tls_cert_path should raise error' do |param|
      omit "Loading CertStore feature works only Windows" unless Fluent.windows?
      dummy_cert_path = File.join(TMP_DIR, "dummy_cert.pem")
      FileUtils.touch(dummy_cert_path)
      conf = %[
        send_timeout 5
        transport tls
        #{param} #{dummy_cert_path}
        tls_cert_logical_store_name Root
        <server>
          host #{TARGET_HOST}
          port #{@target_port}
        </server>
      ]

      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test 'configure cert_logical_store_name and tls_cert_thumbprint' do
      omit "Loading CertStore feature works only Windows" unless Fluent.windows?
      conf = %[
        send_timeout 5
        transport tls
        tls_cert_logical_store_name Root
        tls_cert_thumbprint a909502dd82ae41433e6f83886b00d4277a32a7b
        <server>
          host #{TARGET_HOST}
          port #{@target_port}
        </server>
      ]

      @d = d = create_driver(conf)
      assert_equal "Root", d.instance.tls_cert_logical_store_name
      assert_equal "a909502dd82ae41433e6f83886b00d4277a32a7b", d.instance.tls_cert_thumbprint
    end
  end

  test 'server is an abbreviation of static type of service_discovery' do
    @d = d = create_driver(%[
<server>
  host 127.0.0.1
  port 1234
</server>

<service_discovery>
  @type static

  <service>
    host 127.0.0.1
    port 1235
  </service>
</service_discovery>
    ])


    assert_equal(
      [
        { host: '127.0.0.1', port: 1234 },
        { host: '127.0.0.1', port: 1235 },
      ],
      d.instance.discovery_manager.services.collect do |service|
        { host: service.host, port: service.port }
      end
    )
  end

  test 'pass username and password as empty string to HandshakeProtocol' do
    config_path = File.join(TMP_DIR, "sd_file.conf")
    File.open(config_path, 'w') do |file|
      file.write(%[
- 'host': 127.0.0.1
  'port': 1234
  'weight': 1
])
    end

    mock(Fluent::Plugin::ForwardOutput::HandshakeProtocol).new(log: anything, hostname: nil, shared_key: anything, password: '', username: '')
    @d = d = create_driver(%[
<service_discovery>
  @type file
  path #{config_path}
</service_discovery>
    ])

    assert_equal 1, d.instance.discovery_manager.services.size
    assert_equal '127.0.0.1', d.instance.discovery_manager.services[0].host
    assert_equal 1234, d.instance.discovery_manager.services[0].port
  end

  test 'compress_default_value' do
    @d = d = create_driver
    assert_equal :text, d.instance.compress

    node = d.instance.nodes.first
    assert_equal :text, node.instance_variable_get(:@compress)
  end

  test 'set_compress_is_gzip' do
    @d = d = create_driver(config + %[compress gzip])
    assert_equal :gzip, d.instance.compress
    assert_equal :gzip, d.instance.buffer.compress

    node = d.instance.nodes.first
    assert_equal :gzip, node.instance_variable_get(:@compress)
  end

  test 'set_compress_is_gzip_in_buffer_section' do
    mock = flexmock($log)
    mock.should_receive(:log).with("buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>")

    @d = d = create_driver(config + %[
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

  test 'phi_failure_detector disabled' do
    @d = d = create_driver(config + %[phi_failure_detector false \n phi_threshold 0])
    node = d.instance.nodes.first
    stub(node.failure).phi { raise 'Should not be called' }
    node.tick
    assert_true node.available?
  end

  test 'phi_failure_detector enabled' do
    @d = d = create_driver(config + %[phi_failure_detector true \n phi_threshold 0])
    node = d.instance.nodes.first
    node.tick
    assert_false node.available?
  end

  test 'require_ack_response is disabled in default' do
    @d = d = create_driver(config)
    assert_equal false, d.instance.require_ack_response
    assert_equal 190, d.instance.ack_response_timeout
  end

  test 'require_ack_response can be enabled' do
    @d = d = create_driver(config + %[
      require_ack_response true
      ack_response_timeout 2s
    ])
    d.instance_start
    assert d.instance.require_ack_response
    assert_equal 2, d.instance.ack_response_timeout
  end

  test 'suspend_flush is disable before before_shutdown' do
    @d = d = create_driver(config + %[
      require_ack_response true
      ack_response_timeout 2s
    ])
    d.instance_start
    assert_false d.instance.instance_variable_get(:@suspend_flush)
  end

  test 'suspend_flush should be enabled and try_flush returns nil after before_shutdown' do
    @d = d = create_driver(config + %[
      require_ack_response true
      ack_response_timeout 2s
    ])
    d.instance_start
    d.instance.before_shutdown
    assert_true d.instance.instance_variable_get(:@suspend_flush)
    assert_nil d.instance.try_flush
  end

  test 'verify_connection_at_startup is disabled in default' do
    @d = d = create_driver(config)
    assert_false d.instance.verify_connection_at_startup
  end

  test 'verify_connection_at_startup can be enabled' do
    @d = d = create_driver(config + %[
      verify_connection_at_startup true
    ])
    assert_true d.instance.verify_connection_at_startup
  end

  test 'send tags in str (utf-8 strings)' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    tag_in_utf8 = "test.utf8".encode("utf-8")
    tag_in_ascii = "test.ascii".encode("ascii-8bit")

    emit_events = [
      [tag_in_utf8, time, {"a" => 1}],
      [tag_in_ascii, time, {"a" => 2}],
    ]

    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    assert_rr do
      target_input_driver.run(expect_records: 2) do
        d.run do
          emit_events.each do |tag, t, record|
            d.feed(tag, t, record)
          end
        end
      end
    end

    events = target_input_driver.events
    assert_equal_event_time(time, events[0][1])
    assert_equal ['test.utf8', time, emit_events[0][2]], events[0]
    assert_equal Encoding::UTF_8, events[0][0].encoding
    assert_equal_event_time(time, events[1][1])
    assert_equal ['test.ascii', time, emit_events[1][2]], events[1]
    assert_equal Encoding::UTF_8, events[1][0].encoding
  end

  test 'send_with_time_as_integer' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]

    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    assert_rr do
      target_input_driver.run(expect_records: 2) do
        d.run(default_tag: 'test') do
          records.each do |record|
            d.feed(time, record)
          end
        end
      end
    end

    events = target_input_driver.events
    assert_equal_event_time(time, events[0][1])
    assert_equal ['test', time, records[0]], events[0]
    assert_equal_event_time(time, events[1][1])
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'send_without_time_as_integer' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[
      flush_interval 1s
      time_as_integer false
    ])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    assert_rr do
      target_input_driver.run(expect_records: 2) do
        d.run(default_tag: 'test') do
          records.each do |record|
            d.feed(time, record)
          end
        end
      end
    end

    events = target_input_driver.events
    assert_equal_event_time(time, events[0][1])
    assert_equal ['test', time, records[0]], events[0]
    assert_equal_event_time(time, events[1][1])
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'send_comprssed_message_pack_stream_if_compress_is_gzip' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[
      flush_interval 1s
      compress gzip
    ])

    time = event_time('2011-01-02 13:14:15 UTC')

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    target_input_driver.run(expect_records: 2) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
        end
      end
    end

    event_streams = target_input_driver.event_streams
    assert_true event_streams[0][1].is_a?(Fluent::CompressedMessagePackEventStream)

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'send_to_a_node_supporting_responses' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    # not attempt to receive responses
    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    assert_rr do
      target_input_driver.run(expect_records: 2) do
        d.run(default_tag: 'test') do
          records.each do |record|
            d.feed(time, record)
          end
        end
      end
    end

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'send_to_a_node_not_supporting_responses' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    # not attempt to receive responses
    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    assert_rr do
      target_input_driver.run(expect_records: 2) do
        d.run(default_tag: 'test') do
          records.each do |record|
            d.feed(time, record)
          end
        end
      end
    end

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'a node supporting responses' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[
      require_ack_response true
      <buffer tag>
        flush_mode immediate
        retry_type periodic
        retry_wait 30s
        flush_at_shutdown true
      </buffer>
    ])

    time = event_time("2011-01-02 13:14:15 UTC")

    acked_chunk_ids = []
    nacked = false
    mock.proxy(d.instance.ack_handler).read_ack_from_sock(anything) do |info, success|
      if success
        acked_chunk_ids << info.chunk_id
      else
        nacked = true
      end
      [info, success]
    end

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    target_input_driver.run(expect_records: 2, timeout: 5) do
      d.end_if { acked_chunk_ids.size > 0 || nacked }
      d.run(default_tag: 'test', wait_flush_completion: false, shutdown: false) do
        d.feed([[time, records[0]], [time,records[1]]])
      end
    end

    assert(!nacked, d.instance.log.logs.join)

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]

    assert_equal 1, acked_chunk_ids.size
    assert_equal d.instance.sent_chunk_ids.first, acked_chunk_ids.first
  end

  test 'a node supporting responses after stop' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(config + %[
      require_ack_response true
      <buffer tag>
        flush_mode immediate
        retry_type periodic
        retry_wait 30s
        flush_at_shutdown true
      </buffer>
    ])

    time = event_time("2011-01-02 13:14:15 UTC")

    acked_chunk_ids = []
    nacked = false
    mock.proxy(d.instance.ack_handler).read_ack_from_sock(anything) do |info, success|
      if success
        acked_chunk_ids << info.chunk_id
      else
        nacked = true
      end
      [info, success]
    end

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    target_input_driver.run(expect_records: 2, timeout: 5) do
      d.end_if { acked_chunk_ids.size > 0 || nacked }
      d.run(default_tag: 'test', wait_flush_completion: false, shutdown: false) do
        d.instance.stop
        d.feed([[time, records[0]], [time,records[1]]])
        d.instance.before_shutdown
        d.instance.shutdown
        d.instance.after_shutdown
      end
    end

    assert(!nacked, d.instance.log.logs.join)

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]

    assert_equal 1, acked_chunk_ids.size
    assert_equal d.instance.sent_chunk_ids.first, acked_chunk_ids.first
  end

  data('ack true' => true,
       'ack false' => false)
  test 'TLS transport and ack parameter combination' do |ack|
    omit "TLS and 'ack false' always fails on AppVeyor. Need to debug" if Fluent.windows? && !ack

    input_conf = target_config + %[
                   <transport tls>
                     insecure true
                   </transport>
                 ]
    target_input_driver = create_target_input_driver(conf: input_conf)

    output_conf = %[
      send_timeout 5
      require_ack_response #{ack}
      transport tls
      tls_insecure_mode true
      <server>
        host #{TARGET_HOST}
        port #{@target_port}
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
      d.run(default_tag: 'test', wait_flush_completion: false, shutdown: false) do
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

  test 'a destination node not supporting responses by just ignoring' do
    target_input_driver = create_target_input_driver(response_stub: ->(_option) { nil }, disconnect: false)

    @d = d = create_driver(config + %[
      require_ack_response true
      ack_response_timeout 1s
      <buffer tag>
        flush_mode immediate
        retry_type periodic
        retry_wait 30s
        flush_at_shutdown false # suppress errors in d.instance_shutdown
        flush_thread_interval 30s
      </buffer>
    ])

    node = d.instance.nodes.first
    delayed_commit_timeout_value = nil

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    target_input_driver.end_if{ d.instance.rollback_count > 0 }
    target_input_driver.end_if{ !node.available? }
    target_input_driver.run(expect_records: 2, timeout: 25) do
      d.run(default_tag: 'test', timeout: 20, wait_flush_completion: false, shutdown: false, flush: false) do
        delayed_commit_timeout_value = d.instance.delayed_commit_timeout
        d.feed([[time, records[0]], [time,records[1]]])
      end
    end

    assert_equal (1 + 2), delayed_commit_timeout_value

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]

    assert{ d.instance.rollback_count > 0 }

    logs = d.instance.log.logs
    assert{ logs.any?{|log| log.include?("no response from node. regard it as unavailable.") } }
  end

  test 'a destination node not supporting responses by disconnection' do
    target_input_driver = create_target_input_driver(response_stub: ->(_option) { nil }, disconnect: true)

    @d = d = create_driver(config + %[
      require_ack_response true
      ack_response_timeout 1s
      <buffer tag>
        flush_mode immediate
        retry_type periodic
        retry_wait 30s
        flush_at_shutdown false # suppress errors in d.instance_shutdown
        flush_thread_interval 30s
      </buffer>
    ])

    node = d.instance.nodes.first
    delayed_commit_timeout_value = nil

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    target_input_driver.end_if{ d.instance.rollback_count > 0 }
    target_input_driver.end_if{ !node.available? }
    target_input_driver.run(expect_records: 2, timeout: 25) do
      d.run(default_tag: 'test', timeout: 20, wait_flush_completion: false, shutdown: false, flush: false) do
        delayed_commit_timeout_value = d.instance.delayed_commit_timeout
        d.feed([[time, records[0]], [time,records[1]]])
      end
    end

    assert_equal (1 + 2), delayed_commit_timeout_value

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]

    assert{ d.instance.rollback_count > 0 }

    logs = d.instance.log.logs
    assert{ logs.any?{|log| log.include?("no response from node. regard it as unavailable.") } }
  end

  test 'authentication_with_shared_key' do
    input_conf = target_config + %[
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
        port #{@target_port}
        shared_key fluentd-sharedkey
      </server>
    ]
    @d = d = create_driver(output_conf)

    time = event_time("2011-01-02 13:14:15 UTC")
    records = [
      {"a" => 1},
      {"a" => 2}
    ]

    target_input_driver.run(expect_records: 2, timeout: 15) do
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

  test 'keepalive + shared_key' do
    input_conf = target_config + %[
                   <security>
                     self_hostname in.localhost
                     shared_key fluentd-sharedkey
                   </security>
                 ]
    target_input_driver = create_target_input_driver(conf: input_conf)

    output_conf = %[
      send_timeout 51
      keepalive true
      <security>
        self_hostname localhost
        shared_key fluentd-sharedkey
      </security>
      <server>
        name test
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ]
    @d = d = create_driver(output_conf)

    time = event_time('2011-01-02 13:14:15 UTC')
    records = [{ 'a' => 1 }, { 'a' => 2 }]
    records2 = [{ 'b' => 1}, { 'b' => 2}]
    target_input_driver.run(expect_records: 4, timeout: 15) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
        end

        d.flush # emit buffer to reuse same socket later
        records2.each do |record|
          d.feed(time, record)
        end
      end
    end

    events = target_input_driver.events
    assert{ events != [] }
    assert_equal(['test', time, records[0]], events[0])
    assert_equal(['test', time, records[1]], events[1])
    assert_equal(['test', time, records2[0]], events[2])
    assert_equal(['test', time, records2[1]], events[3])
  end

   test 'authentication_with_user_auth' do
    input_conf = target_config + %[
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
        port #{@target_port}
        shared_key fluentd-sharedkey
        username fluentd
        password fluentd
      </server>
    ]
    @d = d = create_driver(output_conf)

    time = event_time("2011-01-02 13:14:15 UTC")
    records = [
      {"a" => 1},
      {"a" => 2}
    ]

    target_input_driver.run(expect_records: 2, timeout: 15) do
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

  # This test is not 100% but test failed with previous Node implementation which has race condition
  test 'Node with security is thread-safe on multi threads' do
    input_conf = target_config + %[
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
        port #{@target_port}
        shared_key fluentd-sharedkey
      </server>
    ]
    @d = d = create_driver(output_conf)

    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
    target_input_driver.run(timeout: 15) do
      d.run(shutdown: false) do
        node = d.instance.nodes.first
        arr = []
        4.times {
          arr << Thread.new {
            node.send_data('test', chunk) rescue nil
          }
        }
        arr.each { |a| a.join }
      end
    end

    logs = d.logs
    assert_false(logs.any? { |log|
                   log.include?("invalid format for PONG message") || log.include?("shared key mismatch")
                 }, "Actual log:\n#{logs.join}")
  end

  def create_target_input_driver(response_stub: nil, disconnect: false, conf: target_config)
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

  test 'heartbeat_type_none' do
    @d = d = create_driver(config + "\nheartbeat_type none")
    node = d.instance.nodes.first
    assert_equal Fluent::Plugin::ForwardOutput::NoneHeartbeatNode, node.class

    d.instance_start
    assert_nil d.instance.instance_variable_get(:@loop)   # no HeartbeatHandler, or HeartbeatRequestTimer
    assert_nil d.instance.instance_variable_get(:@thread) # no HeartbeatHandler, or HeartbeatRequestTimer

    stub(node.failure).phi { raise 'Should not be called' }
    node.tick
    assert_true node.available?
  end

  test 'heartbeat_type_udp' do
    @d = d = create_driver(config + "\nheartbeat_type udp")

    d.instance_start
    usock = d.instance.instance_variable_get(:@usock)
    servers = d.instance.instance_variable_get(:@_servers)
    timers = d.instance.instance_variable_get(:@_timers)
    assert_equal Fluent::PluginHelper::Socket::WrappedSocket::UDP, usock.class
    assert_kind_of UDPSocket, usock
    assert servers.find{|s| s.title == :out_forward_heartbeat_receiver }
    assert timers.include?(:out_forward_heartbeat_request)

    mock(usock).send("\0", 0, Socket.pack_sockaddr_in(@target_port, '127.0.0.1')).once
    d.instance.send(:on_heartbeat_timer)
  end

  test 'acts_as_secondary' do
    i = Fluent::Plugin::ForwardOutput.new
    conf = config_element(
      'match',
      'primary.**',
      {'@type' => 'forward'},
      [
        config_element('server', '', {'host' => '127.0.0.1'}),
        config_element('secondary', '', {}, [
            config_element('server', '', {'host' => '192.168.1.2'}),
            config_element('server', '', {'host' => '192.168.1.3'})
          ]),
      ]
    )
    assert_nothing_raised do
      i.configure(conf)
    end
  end

  test 'when out_forward has @id' do
    # cancel https://github.com/fluent/fluentd/blob/077508ac817b7637307434d0c978d7cdc3d1c534/lib/fluent/plugin_id.rb#L43-L53
    # it always return true in test
    mock.proxy(Fluent::Plugin).new_sd('static', parent: anything) { |v|
      stub(v).plugin_id_for_test? { false }
    }.once

    output = Fluent::Test::Driver::Output.new(Fluent::Plugin::ForwardOutput) {
      def plugin_id_for_test?
        false
      end
    }

    assert_nothing_raised do
      output.configure(config + %[
        @id unique_out_forward
      ])
    end
  end

  sub_test_case 'verify_connection_at_startup' do
    test 'nodes are not available' do
      @d = d = create_driver(config + %[
        verify_connection_at_startup true
      ])
      e = assert_raise Fluent::UnrecoverableError do
        d.instance_start
      end
      if Fluent.windows?
        assert_match(/No connection could be made because the target machine actively refused it/, e.message)
      else
        assert_match(/Connection refused/, e.message)
      end

      d.instance_shutdown
    end

    test 'nodes_shared_key_miss_match' do
      input_conf = target_config + %[
                   <security>
                     self_hostname in.localhost
                     shared_key fluentd-sharedkey
                   </security>
                 ]
      target_input_driver = create_target_input_driver(conf: input_conf)
      output_conf = %[
        transport tcp
        verify_connection_at_startup true
        <security>
          self_hostname localhost
          shared_key key_miss_match
        </security>

        <server>
          host #{TARGET_HOST}
          port #{@target_port}
        </server>
      ]
      @d = d = create_driver(output_conf)

      target_input_driver.run(expect_records: 1, timeout: 1) do
        e = assert_raise Fluent::UnrecoverableError do
          d.instance_start
        end
        assert_match(/failed to establish connection/, e.message)
      end
    end

    test 'nodes_shared_key_miss_match with TLS' do
      input_conf = target_config + %[
                   <security>
                     self_hostname in.localhost
                     shared_key fluentd-sharedkey
                   </security>
                   <transport tls>
                     insecure true
                   </transport>
                 ]
      target_input_driver = create_target_input_driver(conf: input_conf)
      output_conf = %[
        transport tls
        tls_insecure_mode true
        verify_connection_at_startup true
        <security>
          self_hostname localhost
          shared_key key_miss_match
        </security>

        <server>
          host #{TARGET_HOST}
          port #{@target_port}
        </server>
      ]
      @d = d = create_driver(output_conf)

      target_input_driver.run(expect_records: 1, timeout: 1) do
        e = assert_raise Fluent::UnrecoverableError do
          d.instance_start
        end

        assert_match(/failed to establish connection/, e.message)
      end
    end

    test 'nodes_shared_key_match' do
      input_conf = target_config + %[
                       <security>
                         self_hostname in.localhost
                         shared_key fluentd-sharedkey
                       </security>
                     ]
      target_input_driver = create_target_input_driver(conf: input_conf)
      output_conf = %[
          verify_connection_at_startup true
          <security>
            self_hostname localhost
            shared_key fluentd-sharedkey
          </security>
          <server>
            name test
            host #{TARGET_HOST}
            port #{@target_port}
          </server>
      ]
      @d = d = create_driver(output_conf)

      time = event_time("2011-01-02 13:14:15 UTC")
      records = [{ "a" => 1 }, { "a" => 2 }]

      target_input_driver.run(expect_records: 2, timeout: 3) do
        d.run(default_tag: 'test') do
          records.each do |record|
            d.feed(time, record)
          end
        end
      end

      events = target_input_driver.events
      assert_false events.empty?
      assert_equal(['test', time, records[0]], events[0])
      assert_equal(['test', time, records[1]], events[1])
    end
  end

  test 'Create new connection per send_data' do
    target_input_driver = create_target_input_driver(conf: target_config)
    output_conf = config
    d = create_driver(output_conf)

    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
    mock.proxy(d.instance).socket_create_tcp(TARGET_HOST, @target_port,
                                             linger_timeout: anything,
                                             send_timeout: anything,
                                             recv_timeout: anything,
                                             connect_timeout: anything
                                            ) { |sock| mock(sock).close.once; sock }.twice

    target_input_driver.run(timeout: 15) do
      d.run do
        node = d.instance.nodes.first
        2.times do
          node.send_data('test', chunk) rescue nil
        end
      end
    end
  end

  test 'if no available node' do
    # do not create output driver
    d = create_driver(%[
    <server>
      name test
      standby
      host #{TARGET_HOST}
      port #{@target_port}
    </server>
    ])
    assert_nothing_raised { d.run }
  end

  sub_test_case 'keepalive' do
    test 'Do not create connection per send_data' do
      target_input_driver = create_target_input_driver(conf: target_config)
      output_conf = config + %[
        keepalive true
        keepalive_timeout 2
      ]
      d = create_driver(output_conf)

      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
      mock.proxy(d.instance).socket_create_tcp(TARGET_HOST, @target_port,
                                               linger_timeout: anything,
                                               send_timeout: anything,
                                               recv_timeout: anything,
                                               connect_timeout: anything
                                              ) { |sock| mock(sock).close.once; sock }.once

      target_input_driver.run(timeout: 15) do
        d.run do
          node = d.instance.nodes.first
          2.times do
            node.send_data('test', chunk) rescue nil
          end
        end
      end
    end

    test 'create timer of purging obsolete sockets' do
      output_conf = config + %[keepalive true]
      @d = d = create_driver(output_conf)

      mock(d.instance).timer_execute(:out_forward_heartbeat_request, 1).once
      mock(d.instance).timer_execute(:out_forward_keep_alived_socket_watcher, 5).once
      d.instance_start
    end

    sub_test_case 'with require_ack_response' do
      test 'Create connection per send_data' do
        target_input_driver = create_target_input_driver(conf: target_config)
        output_conf = config + %[
          require_ack_response true
          keepalive true
          keepalive_timeout 2
        ]
        d = create_driver(output_conf)

        chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
        mock.proxy(d.instance).socket_create_tcp(TARGET_HOST, @target_port,
                                                 linger_timeout: anything,
                                                 send_timeout: anything,
                                                 recv_timeout: anything,
                                                 connect_timeout: anything) { |sock|
          mock(sock).close.once; sock
        }.twice

        target_input_driver.run(timeout: 15) do
          d.run do
            node = d.instance.nodes.first
            2.times do
              node.send_data('test', chunk) rescue nil
            end
          end
        end
      end
    end
  end
end
