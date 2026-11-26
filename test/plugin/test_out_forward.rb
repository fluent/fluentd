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

  test 'send_keepalive_packet is disabled by default' do
    @d = d = create_driver(config)
    assert_false d.instance.send_keepalive_packet
  end

  test 'send_keepalive_packet can be enabled' do
    @d = d = create_driver(config + %[
      keepalive true
      send_keepalive_packet true
    ])
    assert_true d.instance.send_keepalive_packet
  end

  test 'send_keepalive_packet without keepalive raises error' do
    assert_raise(Fluent::ConfigError.new("'send_keepalive_packet' is enabled but 'keepalive' is not. Enable 'keepalive' to use TCP keepalive.")) do
      create_driver(config + %[
        send_keepalive_packet true
      ])
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

  sub_test_case 'configure compress' do
    data('default', ['', :text])
    data('gzip', ['compress gzip', :gzip])
    data('zstd', ['compress zstd', :zstd])
    test 'should be applied' do |(option, expected)|
      @d = d = create_driver(config + option)
      node = d.instance.nodes.first

      assert_equal(
        [expected, expected],
        [d.instance.compress, node.instance_variable_get(:@compress)]
      )
    end

    data('default' => '')
    data('gzip' => 'compress gzip')
    data('zstd' => 'compress zstd')
    test 'should log as experimental only for zstd' do |option|
      @d = d = create_driver(config + option)

      log_message = "zstd compression feature is an experimental new feature"
      assert do
        if d.instance.compress == :zstd
          d.logs.any? { |log| log.include?(log_message) }
        else
          d.logs.none? { |log| log.include?(log_message) }
        end
      end
    end

    # TODO add tests that we cannot configure the different compress type between owner and buffer except for :text
    data('gzip', ['compress gzip', :text, :gzip])
    data('zstd', ['compress zstd', :text, :zstd])
    test 'can configure buffer compress separately when owner uses :text' do |(buffer_option, expected_owner_compress, expected_buffer_compress)|
      @d = d = create_driver(config + %[
         <buffer>
           type memory
           #{buffer_option}
         </buffer>
       ])
      node = d.instance.nodes.first

      assert_equal(
        [expected_owner_compress, expected_owner_compress, expected_buffer_compress],
        [d.instance.compress, node.instance_variable_get(:@compress), d.instance.buffer.compress],
      )

      log_message = "buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>"
      assert do
        d.logs.any? { |log| log.include?(log_message) }
      end
    end
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

  test 'send_compressed_message_pack_stream_if_compress_is_gzip' do
    target_input_driver = create_target_input_driver(conf: target_config + "skip_invalid_event false")

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

  test 'send_compressed_message_pack_stream_if_compress_is_zstd' do
    target_input_driver = create_target_input_driver(conf: target_config + "skip_invalid_event false")

    @d = d = create_driver(config + %[
      flush_interval 1s
      compress zstd
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

  data(
    'with ack' => ['require_ack_response true', false],
    'with ack and keepalive' => ["require_ack_response true\nkeepalive true", true],
    'without ack' => ['', true],
  )
  test 'a failed send is not rolled back by the ack check' do |(conf, available)|
    server = TCPServer.new(TARGET_HOST, 0)
    socks = []
    server_thread = Thread.new do
      loop { socks << server.accept }
    end

    @d = d = create_driver(%[
      #{conf}
      ack_response_timeout 1s
      <server>
        host #{TARGET_HOST}
        port #{server.local_address.ip_port}
      </server>
    ])
    node = d.instance.nodes.first
    stub(node).send_data_actual { raise Errno::ECONNRESET }
    rollbacks = 0
    stub(d.instance).rollback_write { |*| rollbacks += 1 }
    ack_checks = 0
    stub.proxy(d.instance).ack_check { |ret| ack_checks += 1; ret }

    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
    d.run(shutdown: false) do
      assert_raise(Errno::ECONNRESET) do
        node.send_data('test', chunk)
      end
      unless available
        waiting(5, plugin: d.instance) do
          sleep 0.1 while node.available?
        end
        # The ack check disables the node before it rolls back, so wait until that call returns.
        checked = ack_checks
        waiting(5, plugin: d.instance) do
          sleep 0.1 while ack_checks == checked
        end
      end
      assert_equal(available, node.available?)
      # The flush retries a failed send by itself. A rollback here sends the chunk again.
      assert_equal(0, rollbacks)
    end
  ensure
    server_thread&.kill
    socks&.each(&:close)
    server&.close
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

  data(
    'without name' => ['', TARGET_HOST],
    'with name' => ['name mynode', 'mynode'],
  )
  test 'handshake failure shows the name of the server, or the host without a name' do |(name_line, expected)|
    server = TCPServer.new(TARGET_HOST, 0)
    server_thread = Thread.new do
      sock = server.accept
      sock.write(['HELO'].to_msgpack)
    ensure
      sock&.close
    end

    @d = d = create_driver(%[
      heartbeat_type none
      <security>
        self_hostname localhost
        shared_key fluentd-sharedkey
      </security>
      <server>
        #{name_line}
        host #{TARGET_HOST}
        port #{server.local_address.ip_port}
      </server>
    ])

    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
    d.run(shutdown: false) do
      assert_raise(Fluent::Plugin::ForwardOutput::ConnectionClosedError.new("failed to establish connection with node #{expected}")) do
        d.instance.nodes.first.send_data('test', chunk)
      end
    end
    assert(d.logs.any? { |log| log.include?("received invalid helo message from #{expected}\n") }, d.logs.join)
  ensure
    server_thread&.kill
    server&.close
  end

  data(
    'without name' => ['', 'name=nil'],
    'with name' => ['name mynode', 'name="mynode"'],
  )
  test 'detached and recovered warnings show the name of the server as a field' do |(name_line, expected)|
    @d = d = create_driver(%[
      <server>
        #{name_line}
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ])
    node = d.instance.nodes.first
    server = "#{expected} host=\"#{TARGET_HOST}\" port=#{@target_port}"

    hard_timeout = true
    stub(node.failure).hard_timeout? { hard_timeout }
    assert_true node.tick
    assert(d.logs.any? { |log| log.include?("detached forwarding server #{server} hard_timeout=true\n") }, d.logs.join)

    stub(node.failure).sample_size { d.instance.recover_sample_size + 1 }
    assert_true node.heartbeat
    assert(d.logs.any? { |log| log.include?("recovered forwarding server #{server}\n") }, d.logs.join)

    hard_timeout = false
    stub(node.failure).phi { d.instance.phi_threshold + 1 }
    assert_true node.tick
    assert(d.logs.any? { |log| log.include?("detached forwarding server #{server} phi=#{d.instance.phi_threshold + 1} phi_threshold=#{d.instance.phi_threshold}\n") }, d.logs.join)
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
                                             connect_timeout: anything,
                                             send_keepalive_packet: anything
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
                                               connect_timeout: anything,
                                               send_keepalive_packet: anything
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
                                                 connect_timeout: anything,
                                                 send_keepalive_packet: anything) { |sock|
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

  test 'can use metrics plugins and fallback methods' do
    @d = create_driver

    %w[healthy_nodes_count_metrics registered_nodes_count_metrics].each do |metric_name|
      assert_true @d.instance.instance_variable_get(:"@#{metric_name}").is_a?(Fluent::Plugin::Metrics)
    end

    assert_equal 0, @d.instance.healthy_nodes_count
    assert_equal 0, @d.instance.registered_nodes_count
  end

  test 'establish_connection_timeout' do
    @d = d = create_driver(%[
      hard_timeout 1
      <server>
        host #{TARGET_HOST}
        port #{@target_port}
      </server>
    ])

    node = d.instance.nodes.first
    mock_sock = flexmock('socket')
    mock_sock.should_receive(:read_nonblock).with(512).and_return('').at_least.once

    ri = Fluent::Plugin::ForwardOutput::ConnectionManager::RequestInfo.new(:helo)

    assert_true node.available?
    node.establish_connection(mock_sock, ri)
    assert_false node.available?

    logs = d.logs
    assert{ logs.any?{|log| log.include?('handshake timeout after 1.0s') } }
  end

  sub_test_case 'ack_check discards tainted keepalive sockets (#4057)' do
    def create_ack_driver
      create_driver(config + %[
        require_ack_response true
        keepalive true
      ])
    end

    # A keepalive socket whose ack failed or came back mismatched may still
    # carry an in-flight/stale ack for a previous chunk. Returning it to the
    # reuse pool lets that stale ack be read against a chunk sent on the reused
    # socket, permanently offsetting every following ack and flooding the log
    # with 'ack in response and chunk id in sent data are different' warnings.
    data(
      'mismatched ack' => Fluent::Plugin::ForwardOutput::AckHandler::Result::CHUNKID_UNMATCHED,
      'failed ack' => Fluent::Plugin::ForwardOutput::AckHandler::Result::FAILED,
    )
    test 'discards the socket instead of reusing it' do |result|
      @d = d = create_ack_driver
      instance = d.instance
      node = instance.nodes.first
      sock = 'sock'

      # One-shot: only the explicit ack_check below yields; the collect_response
      # that after_shutdown triggers during teardown stays silent.
      fake_ack_handler = Object.new
      fake_ack_handler.instance_variable_set(:@yielded, false)
      fake_ack_handler.define_singleton_method(:collect_response) do |_interval, &block|
        next if @yielded
        @yielded = true
        block.call('chunk-id', node, sock, result)
      end
      instance.instance_variable_set(:@ack_handler, fake_ack_handler)
      stub(instance).rollback_write

      cm = instance.instance_variable_get(:@connection_manager)
      mock(cm).discard(sock).once
      mock(cm).close(sock).never

      instance.__send__(:ack_check, 0)
    end

    test 'reuses the socket on a successful ack' do
      @d = d = create_ack_driver
      instance = d.instance
      node = instance.nodes.first
      sock = 'sock'

      fake_ack_handler = Object.new
      fake_ack_handler.instance_variable_set(:@yielded, false)
      fake_ack_handler.define_singleton_method(:collect_response) do |_interval, &block|
        next if @yielded
        @yielded = true
        block.call('chunk-id', node, sock, Fluent::Plugin::ForwardOutput::AckHandler::Result::SUCCESS)
      end
      instance.instance_variable_set(:@ack_handler, fake_ack_handler)
      stub(instance).commit_write

      cm = instance.instance_variable_get(:@connection_manager)
      mock(cm).close(sock).once
      mock(cm).discard(sock).never

      instance.__send__(:ack_check, 0)
    end
  end

  sub_test_case 'a node disabled outside the heartbeat' do
    def create_standby_driver(primary_host: TARGET_HOST, conf: '', standby: true, stub_primary_heartbeat: true)
      if standby
        standby_config = %[
          <server>
            name standby
            host #{TARGET_HOST}
            port #{unused_port(protocol: :tcp)}
            standby true
          </server>
        ]
      end
      @d = create_driver(%[
        heartbeat_type transport
        recover_wait 0
        #{conf}
        <server>
          name primary
          host #{primary_host}
          port #{@target_port}
        </server>
        #{standby_config}
      ])
      @instance = @d.instance
      @primary = @instance.nodes.find { |n| n.name == 'primary' }
      @standby = @instance.nodes.find { |n| n.name == 'standby' }
      @instance.nodes.each do |node|
        next if node == @primary && !stub_primary_heartbeat

        stub(node).send_heartbeat { false }
      end
    end

    def other_regular_server
      %[
        <server>
          name other
          host #{TARGET_HOST}
          port #{unused_port(protocol: :tcp)}
        </server>
      ]
    end

    def run_heartbeat_timer
      @instance.__send__(:on_heartbeat_timer)
    end

    def selected_node
      _, node = @instance.__send__(:service_discovery_select_service) { |n| n }
      node
    end

    def data_chunk(tag = 'test')
      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, tag, nil))
      chunk.concat([Fluent::EventTime.now, {'message' => 'test'}].to_msgpack, 1)
      chunk
    end

    def stub_node_socket(node, host: TARGET_HOST, &factory)
      stub(@instance).socket_create_tcp(host, node.port, linger_timeout: anything, send_timeout: anything,
                                       recv_timeout: anything, connect_timeout: anything,
                                       send_keepalive_packet: anything, &factory)
    end

    def reset_socket
      StringIO.new.tap { |socket| stub(socket).write { raise Errno::ECONNRESET } }
    end

    def forwarded_tags(sockets)
      tags = []
      sockets.each do |socket|
        MessagePack::Unpacker.new.feed_each(socket.string) { |message| tags << message.first }
      end
      tags
    end

    data(
      'without keepalive or ACKs' => '',
      'with keepalive and without ACKs' => 'keepalive true',
      'with keepalive and ACKs' => "keepalive true\nrequire_ack_response true",
    )
    test 'a primary that resets writes is replaced by the standby' do |conf|
      create_standby_driver(conf: "#{conf}\nrecover_wait 3s")
      4.times { @primary.heartbeat }
      stub(@primary).send_heartbeat { @primary.heartbeat }
      primary_sockets = []
      stub_node_socket(@primary) do
        StringIO.new.tap do |socket|
          stub(socket).write { raise Errno::ECONNRESET }
          primary_sockets << socket
        end
      end
      standby_sockets = []
      stub_node_socket(@standby) { StringIO.new.tap { |socket| standby_sockets << socket } }

      chunk = data_chunk
      write = @instance.require_ack_response ? :try_write : :write
      @instance.public_send(write, chunk)
      assert_false @primary.available?
      assert_equal @standby, selected_node
      assert_equal ['test'], forwarded_tags(standby_sockets)
      assert_true @d.logs.any? { |line| line.include?('detached forwarding server') && line.include?('primary') }
      @instance.__send__(:on_purge_obsolete_socks) if @instance.keepalive
      assert_true primary_sockets.all?(&:closed?)

      4.times { run_heartbeat_timer }
      assert_equal @primary, selected_node
      @instance.public_send(write, chunk)
      assert_equal @standby, selected_node
      assert_equal ['test', 'test'], forwarded_tags(standby_sockets)
    end

    test 'one reset on a keepalive socket leaves the primary available when another regular server succeeds' do
      create_standby_driver(conf: "keepalive true\n#{other_regular_server}")
      other = @instance.nodes.find { |node| node.name == 'other' }
      load_balancer = @instance.instance_variable_get(:@load_balancer)
      load_balancer.instance_variable_set(:@weight_array, [@primary, other, @primary, other])

      stale_socket = StringIO.new
      primary_connections = 0
      stub_node_socket(@primary) do
        primary_connections += 1
        stale_socket
      end
      @primary.verify_connection
      resets = 0
      stub(stale_socket).write do
        resets += 1
        raise Errno::ECONNRESET
      end
      healthy_socket = StringIO.new
      stub_node_socket(other) { healthy_socket }

      @instance.write(data_chunk)

      assert_equal 1, primary_connections
      assert_equal 1, resets
      assert_true @primary.available?
      assert_equal ['test'], forwarded_tags([healthy_socket])
    end

    data('one reset' => 1, 'two resets' => 2)
    test 'a local chunk error after write disconnects keeps its error and leaves the primary available' do |reset_count|
      create_standby_driver
      primary_attempts = 0
      stub_node_socket(@primary) do
        primary_attempts += 1
        primary_attempts <= reset_count ? reset_socket : StringIO.new
      end
      standby_attempts = 0
      stub_node_socket(@standby) do
        standby_attempts += 1
        raise Errno::ECONNREFUSED
      end
      chunk = data_chunk
      stub(chunk).open(compressed: :text) { raise ArgumentError, 'invalid chunk' }

      error = assert_raise(ArgumentError) { @instance.write(chunk) }

      assert_equal 'invalid chunk', error.message
      assert_operator primary_attempts, :>, reset_count
      assert_equal 0, standby_attempts
      assert_true @primary.available?
    end

    test 'a recovering primary cannot prevent the failed flush from reaching the standby' do
      create_standby_driver
      stub(@primary).send_heartbeat { @primary.heartbeat }
      stub_node_socket(@primary) do
        StringIO.new.tap { |socket| stub(socket).write { raise Errno::ECONNRESET } }
      end
      standby_sockets = []
      stub_node_socket(@standby) { StringIO.new.tap { |socket| standby_sockets << socket } }

      before_standby_send = lambda do
        run_heartbeat_timer
        assert_equal @primary, selected_node
      end
      lb = @instance.instance_variable_get(:@load_balancer)
      lb.define_singleton_method(:select_standby) do |failed_nodes, tried_standbys, &block|
        before_standby_send.call
        super(failed_nodes, tried_standbys, &block)
      end

      3.times do
        @instance.write(data_chunk)
        assert_false @primary.available?
        run_heartbeat_timer
        assert_equal @primary, selected_node
      end
      assert_equal ['test', 'test', 'test'], forwarded_tags(standby_sockets)
    end

    test 'the promoted standby can reconnect in the same flush' do
      create_standby_driver(conf: 'keepalive true')
      stub_node_socket(@primary) do
        StringIO.new.tap { |socket| stub(socket).write { raise Errno::EPIPE } }
      end
      broken_socket = StringIO.new
      stub(broken_socket).write { raise Errno::EPIPE }
      healthy_socket = StringIO.new
      sockets = [broken_socket, healthy_socket]
      stub_node_socket(@standby) { sockets.shift }

      @instance.write(data_chunk)

      assert_empty sockets
      assert_true @standby.available?
      assert_equal ['test'], forwarded_tags([healthy_socket])
    end

    def first_standby_config
      %[
        <server>
          name first-standby
          host #{TARGET_HOST}
          port #{unused_port(protocol: :tcp)}
          standby true
        </server>
      ]
    end

    test 'a later healthy standby accepts the chunk when earlier standby writes reset' do
      create_standby_driver(conf: first_standby_config)
      first_standby = @instance.nodes.find { |node| node.name == 'first-standby' }
      stub_node_socket(@primary) { reset_socket }
      standby_attempts = 0
      stub_node_socket(first_standby) do
        standby_attempts += 1
        reset_socket
      end
      healthy_socket = StringIO.new
      stub_node_socket(@standby) { healthy_socket }

      assert_equal @standby, @instance.write(data_chunk).last

      assert_operator standby_attempts, :>=, 2
      assert_false @primary.available?
      assert_false first_standby.available?
      assert_equal ['test'], forwarded_tags([healthy_socket])
    end

    test 'a later standby send failure surfaces its error and keeps all nodes available' do
      create_standby_driver(conf: first_standby_config)
      first_standby = @instance.nodes.find { |node| node.name == 'first-standby' }
      stub_node_socket(@primary) { reset_socket }
      stub_node_socket(first_standby) { reset_socket }
      stub_node_socket(@standby) { raise Errno::ECONNREFUSED }

      assert_raise(Errno::ECONNREFUSED) { @instance.write(data_chunk) }
      assert_true @instance.nodes.all?(&:available?)
    end

    test 'a local chunk error from the first standby stays unrecoverable' do
      create_standby_driver(conf: first_standby_config)
      first_standby = @instance.nodes.find { |node| node.name == 'first-standby' }
      stub_node_socket(@primary) { reset_socket }
      stub_node_socket(first_standby) { StringIO.new }
      stub_node_socket(@standby) { raise Errno::ECONNREFUSED }
      chunk = data_chunk
      stub(chunk).open(compressed: :text) { raise ArgumentError, 'invalid chunk' }

      assert_raise(ArgumentError) { @instance.write(chunk) }
      assert_true @instance.nodes.all?(&:available?)
    end

    test 'a newer successful write prevents an older flush from detaching the primary' do
      create_standby_driver(conf: "recover_wait 3s\n<buffer>\nflush_thread_count 2\n</buffer>")
      resetting_primary = true
      primary_sockets = []
      standby_sockets = []
      stub_node_socket(@primary) do
        resetting_primary ? reset_socket : StringIO.new.tap { |socket| primary_sockets << socket }
      end
      started_standby = Queue.new
      resume_standby = Queue.new
      stub_node_socket(@standby) do
        if standby_sockets.empty?
          started_standby << true
          resume_standby.pop
        end
        StringIO.new.tap { |socket| standby_sockets << socket }
      end

      older_flush = Thread.new { @instance.write(data_chunk('older')) }
      waiting(5) { started_standby.pop }
      resetting_primary = false
      assert_equal @primary, @instance.write(data_chunk('newer')).last

      resume_standby << true
      assert_equal @standby, waiting(5) { older_flush.value }.last
      assert_true @primary.available?
      assert_equal @primary, @instance.write(data_chunk('next')).last
      assert_equal ['newer', 'next'], forwarded_tags(primary_sockets)
      assert_equal ['older'], forwarded_tags(standby_sockets)
    ensure
      resume_standby&.push(true)
      older_flush&.join(5)
    end

    data('one server' => :single, 'two regular servers' => :regulars,
         'unavailable standby' => :unavailable, 'standby refuses connections' => :refused)
    test 'a failed flush can retry a recovered server when no other server accepted the chunk' do |mode|
      create_standby_driver(conf: mode == :regulars ? other_regular_server : '', standby: [:unavailable, :refused].include?(mode))
      @standby.disable! if mode == :unavailable
      stub_node_socket(@standby) { raise Errno::ECONNREFUSED } if mode == :refused
      regular_nodes = @instance.nodes.reject(&:standby?)
      initial_samples = regular_nodes.map { |node| node.failure.sample_size }
      resetting = true
      reset_nodes = []
      healthy_sockets = []
      regular_nodes.each do |node|
        stub_node_socket(node) do
          if resetting
            reset_nodes << node
            reset_socket
          else
            StringIO.new.tap { |socket| healthy_sockets << socket }
          end
        end
      end
      chunk = data_chunk
      error = mode == :refused ? Errno::ECONNREFUSED : Errno::ECONNRESET

      assert_raise(error) { @instance.write(chunk) }
      assert_equal regular_nodes.map(&:name).sort, reset_nodes.map(&:name).uniq.sort
      assert_true regular_nodes.all?(&:available?)
      assert_equal initial_samples, regular_nodes.map { |node| node.failure.sample_size }

      resetting = false
      @instance.write(chunk)

      assert_equal ['test'], forwarded_tags(healthy_sockets)
    end

    data('before the first recovery heartbeat' => false, 'after a failed recovery heartbeat' => true)
    test 'a primary detached after write disconnects recovers after its DNS address changes' do |fail_heartbeat_first|
      create_standby_driver(primary_host: 'localhost', stub_primary_heartbeat: false)
      old_address = @primary.resolved_host
      new_address = old_address == TARGET_HOST ? '127.0.0.2' : TARGET_HOST
      address = old_address
      resolutions = []
      stub(@primary).resolve_dns! do
        resolutions << address
        address
      end
      refuse_old_address = false
      stub_node_socket(@primary, host: old_address) do
        raise Errno::ECONNREFUSED if refuse_old_address

        reset_socket
      end
      standby_socket = StringIO.new
      stub_node_socket(@standby) { standby_socket }
      new_sockets = []
      stub_node_socket(@primary, host: new_address) { StringIO.new.tap { |socket| new_sockets << socket } }

      @instance.write(data_chunk)

      assert_false @primary.available?
      assert_equal ['test'], forwarded_tags([standby_socket])
      refuse_old_address = true
      if fail_heartbeat_first
        run_heartbeat_timer
        assert_false @primary.available?
        assert_equal [old_address], resolutions
      end

      address = new_address
      run_heartbeat_timer
      assert_true @primary.available?
      assert_equal(fail_heartbeat_first ? [old_address, new_address] : [new_address], resolutions)

      @instance.write(data_chunk)

      assert_equal ['test'], forwarded_tags(new_sockets)
      assert_equal ['test'], forwarded_tags([standby_socket])
    end

    test 'clearing a failed node waits for heartbeat sampling to finish' do
      create_standby_driver
      generation = @primary.write_generation
      sampling = Queue.new
      resume_sampling = Queue.new
      stub(@primary.failure).phi do
        sampling << true
        resume_sampling.pop
        sampling << @primary.failure.sample_size
        0.0
      end

      heartbeat = Thread.new { @primary.tick }
      waiting(5) { sampling.pop }
      detach = Thread.new { @primary.detach_after_send_failure(Errno::EPIPE.new, generation) }
      waiting(5) { Thread.pass until detach.stop? }
      assert_true detach.alive?

      resume_sampling << true
      assert_false heartbeat.value
      assert_operator sampling.pop, :>, 0
      assert_true detach.value
      assert_equal 0, @primary.failure.sample_size
    ensure
      resume_sampling&.push(true)
      heartbeat&.join(5)
      detach&.join(5)
    end

    test 'a temporary write failure can reconnect in the same flush' do
      create_standby_driver(conf: 'keepalive true')
      broken_socket = StringIO.new
      stub(broken_socket).write { raise Errno::EPIPE }
      healthy_socket = StringIO.new
      sockets = [broken_socket, healthy_socket]
      stub_node_socket(@primary) { sockets.shift }

      @instance.write(data_chunk)

      @instance.__send__(:on_purge_obsolete_socks)
      assert_predicate broken_socket, :closed?
      assert_empty sockets
      assert_true @primary.available?
      assert_equal 'test', MessagePack.unpack(healthy_socket.string).first
    end

    data('EOF' => false, 'late ACK' => true)
    test 'the ACK for a failed socket does not detach a primary that reconnected' do |late_ack|
      create_standby_driver(conf: 'require_ack_response true')
      server = TCPServer.new(TARGET_HOST, 0)
      sockets, peers = 2.times.map do
        socket = TCPSocket.new(TARGET_HOST, server.local_address.ip_port)
        [socket, server.accept]
      end.transpose
      broken_socket, healthy_socket = sockets
      stub(broken_socket).write { raise Errno::EPIPE }
      attempts = sockets.dup
      stub_node_socket(@primary) { attempts.shift }
      chunk = data_chunk
      mock(@instance).rollback_write(anything, update_retry: anything).never
      if late_ack
        peers.first.write({'ack' => Base64.encode64(chunk.unique_id)}.to_msgpack)
        mock(@instance).commit_write(chunk.unique_id).once
      else
        mock(@instance).commit_write(chunk.unique_id).never
      end
      peers.first.close

      @instance.try_write(chunk)
      assert_empty attempts
      assert_true @primary.available?

      @instance.__send__(:ack_check, 1)
      assert_false @d.logs.any? { |line| line.include?('unexpected error while receiving ack') }
      assert_true @primary.available?
      assert_predicate broken_socket, :closed?
      assert_false healthy_socket.closed?
    ensure
      (Array(sockets) + Array(peers) + [server]).compact.each { |socket| socket.close unless socket.closed? }
    end

    test 'a local chunk read error does not disable the primary' do
      create_standby_driver
      stub_node_socket(@primary) { StringIO.new }
      chunk = data_chunk
      stub(chunk).open(compressed: :text) { raise Errno::EPIPE }

      assert_raise(Errno::EPIPE) { @instance.write(chunk) }
      run_heartbeat_timer
      assert_true @primary.available?
      assert_equal @primary, selected_node
    end

    test 'a disconnect error from the custom chunk reader does not disable the primary' do
      create_standby_driver
      stub_node_socket(@primary) { StringIO.new }
      chunk = data_chunk
      source = Object.new
      stub(source).size { 16 }
      stub(source).read { raise Errno::ECONNRESET }
      stub(chunk).open(compressed: :text).yields(source)

      assert_raise(Errno::ECONNRESET) { @instance.write(chunk) }
      run_heartbeat_timer
      assert_true @primary.available?
      assert_equal @primary, selected_node
    end

    test 'the heartbeat timer switches to the standby node' do
      create_standby_driver
      @primary.disable!
      run_heartbeat_timer

      assert_equal @standby, selected_node
    end

    test 'rebuilds the weight array only once 
    while the availability stays the same' do
      create_standby_driver
      mock.proxy(@instance.instance_variable_get(:@load_balancer)).rebalance(anything).once

      @primary.disable!
      2.times { run_heartbeat_timer }
    end

    test 'the heartbeat timer switches to the standby node again after the primary recovers' do
      create_standby_driver
      @primary.disable!
      run_heartbeat_timer
      stub(@primary).send_heartbeat { @primary.heartbeat }
      run_heartbeat_timer
      assert_equal @primary, selected_node

      stub(@primary).send_heartbeat { false }
      @primary.disable!
      run_heartbeat_timer

      assert_equal @standby, selected_node
    end

    test 'the heartbeat timer switches to the standby node for a primary disabled at startup' do
      create_standby_driver(primary_host: 'unexisting.yaaaaaaaaaaaaaay.host.example.com', conf: 'ignore_network_errors_at_startup true')
      assert_false @primary.available?

      run_heartbeat_timer

      assert_equal @standby, selected_node
    end
  end
end
