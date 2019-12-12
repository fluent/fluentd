require 'openssl'
require 'tempfile'

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
  end

  def teardown
    @d.instance_shutdown if @d
  end

  TMP_DIR = File.join(__dir__, "../tmp/out_forward#{ENV['TEST_ENV_NUMBER']}")

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
        port #{TARGET_PORT}
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
    assert_equal TARGET_PORT, node.port
  end

  test 'configure_traditional' do
    @d = d = create_driver(<<EOL)
      self_hostname localhost
      <server>
        name test
        host #{TARGET_HOST}
        port #{TARGET_PORT}
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
        port #{TARGET_PORT}
      </server>
    ])
    assert_equal 30, d.instance.send_timeout
    assert_equal 10, d.instance.connect_timeout
    assert_equal 15, d.instance.hard_timeout
    assert_equal 20, d.instance.ack_response_timeout
  end

  test 'configure_udp_heartbeat' do
    @d = d = create_driver(CONFIG + "\nheartbeat_type udp")
    assert_equal :udp, d.instance.heartbeat_type
  end

  test 'configure_none_heartbeat' do
    @d = d = create_driver(CONFIG + "\nheartbeat_type none")
    assert_equal :none, d.instance.heartbeat_type
  end

  test 'configure_expire_dns_cache' do
    @d = d = create_driver(CONFIG + "\nexpire_dns_cache 5")
    assert_equal 5, d.instance.expire_dns_cache
  end

  test 'configure_dns_round_robin udp' do
    assert_raise(Fluent::ConfigError) do
      create_driver(CONFIG + "\nheartbeat_type udp\ndns_round_robin true")
    end
  end

  test 'configure_dns_round_robin transport' do
    @d = d = create_driver(CONFIG + "\nheartbeat_type transport\ndns_round_robin true")
    assert_equal true, d.instance.dns_round_robin
  end

  test 'configure_dns_round_robin none' do
    @d = d = create_driver(CONFIG + "\nheartbeat_type none\ndns_round_robin true")
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
    assert_raise SocketError do
      create_driver(normal_conf)
    end

    conf = config_element('match', '**', {'ignore_network_errors_at_startup' => 'true'}, [
        config_element('server', '', {'name' => 'test', 'host' => 'unexisting.yaaaaaaaaaaaaaay.host.example.com'})
      ])
    @d = d = create_driver(conf)
    expected_log = "failed to resolve node name when configured"
    expected_detail = 'server="test" error_class=SocketError'
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
        port #{TARGET_PORT}
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
          port #{TARGET_PORT}
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
          port #{TARGET_PORT}
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
          port #{TARGET_PORT}
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
          port #{TARGET_PORT}
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

    assert_equal 2, d.instance.discovery_manager.services.size
    assert_equal '127.0.0.1', d.instance.discovery_manager.services[0].host
    assert_equal 1234, d.instance.discovery_manager.services[0].port
    assert_equal '127.0.0.1', d.instance.discovery_manager.services[1].host
    assert_equal 1235, d.instance.discovery_manager.services[1].port
  end

  test 'compress_default_value' do
    @d = d = create_driver
    assert_equal :text, d.instance.compress

    node = d.instance.nodes.first
    assert_equal :text, node.instance_variable_get(:@compress)
  end

  test 'set_compress_is_gzip' do
    @d = d = create_driver(CONFIG + %[compress gzip])
    assert_equal :gzip, d.instance.compress
    assert_equal :gzip, d.instance.buffer.compress

    node = d.instance.nodes.first
    assert_equal :gzip, node.instance_variable_get(:@compress)
  end

  test 'set_compress_is_gzip_in_buffer_section' do
    mock = flexmock($log)
    mock.should_receive(:log).with("buffer is compressed.  If you also want to save the bandwidth of a network, Add `compress` configuration in <match>")

    @d = d = create_driver(CONFIG + %[
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
    @d = d = create_driver(CONFIG + %[phi_failure_detector false \n phi_threshold 0])
    node = d.instance.nodes.first
    stub(node.failure).phi { raise 'Should not be called' }
    node.tick
    assert_true node.available?
  end

  test 'phi_failure_detector enabled' do
    @d = d = create_driver(CONFIG + %[phi_failure_detector true \n phi_threshold 0])
    node = d.instance.nodes.first
    node.tick
    assert_false node.available?
  end

  test 'require_ack_response is disabled in default' do
    @d = d = create_driver(CONFIG)
    assert_equal false, d.instance.require_ack_response
    assert_equal 190, d.instance.ack_response_timeout
  end

  test 'require_ack_response can be enabled' do
    @d = d = create_driver(CONFIG + %[
      require_ack_response true
      ack_response_timeout 2s
    ])
    assert d.instance.require_ack_response
    assert_equal 2, d.instance.ack_response_timeout
  end

  test 'verify_connection_at_startup is disabled in default' do
    @d = d = create_driver(CONFIG)
    assert_false d.instance.verify_connection_at_startup
  end

  test 'verify_connection_at_startup can be enabled' do
    @d = d = create_driver(CONFIG + %[
      verify_connection_at_startup true
    ])
    assert_true d.instance.verify_connection_at_startup
  end

  test 'send tags in str (utf-8 strings)' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(CONFIG + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    tag_in_utf8 = "test.utf8".encode("utf-8")
    tag_in_ascii = "test.ascii".encode("ascii-8bit")

    emit_events = [
      [tag_in_utf8, time, {"a" => 1}],
      [tag_in_ascii, time, {"a" => 2}],
    ]

    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    target_input_driver.run(expect_records: 2) do
      d.run do
        emit_events.each do |tag, t, record|
          d.feed(tag, t, record)
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

    @d = d = create_driver(CONFIG + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]

    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    target_input_driver.run(expect_records: 2) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
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

    @d = d = create_driver(CONFIG + %[
      flush_interval 1s
      time_as_integer false
    ])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    target_input_driver.run(expect_records: 2) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
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

    @d = d = create_driver(CONFIG + %[
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

    @d = d = create_driver(CONFIG + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    # not attempt to receive responses
    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    target_input_driver.run(expect_records: 2) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
        end
      end
    end

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'send_to_a_node_not_supporting_responses' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(CONFIG + %[flush_interval 1s])

    time = event_time("2011-01-02 13:14:15 UTC")

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    # not attempt to receive responses
    stub(d.instance.ack_handler).read_ack_from_sock(anything).never
    target_input_driver.run(expect_records: 2) do
      d.run(default_tag: 'test') do
        records.each do |record|
          d.feed(time, record)
        end
      end
    end

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]
  end

  test 'a node supporting responses' do
    target_input_driver = create_target_input_driver

    @d = d = create_driver(CONFIG + %[
      require_ack_response true
      ack_response_timeout 1s
      <buffer tag>
        flush_mode immediate
        retry_type periodic
        retry_wait 30s
        flush_at_shutdown false # suppress errors in d.instance_shutdown
      </buffer>
    ])

    time = event_time("2011-01-02 13:14:15 UTC")

    acked_chunk_ids = []
    mock.proxy(d.instance.ack_handler).read_ack_from_sock(anything) do |info, success|
      if success
        acked_chunk_ids << info.chunk_id
      end
      [chunk_id, success]
    end

    records = [
      {"a" => 1},
      {"a" => 2}
    ]
    target_input_driver.run(expect_records: 2) do
      d.end_if { acked_chunk_ids.size > 0 }
      d.run(default_tag: 'test', wait_flush_completion: false, shutdown: false) do
        d.feed([[time, records[0]], [time,records[1]]])
      end
    end

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

    input_conf = TARGET_CONFIG + %[
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

  data('ack true' => true,
       'ack false' => false)
  test 'TLS transport client cert chain' do |ack|
    omit "TLS and 'ack false' always fails on AppVeyor. Need to debug" if Fluent.windows? && !ack

    # Cert generation based on
    # https://ruby-doc.org/stdlib-2.4.0/libdoc/openssl/rdoc/OpenSSL/X509/Certificate.html
    # examples
    root = create_certificate("root-ca", 1)
    root_ca = root[0]

    root_ca_file = Tempfile.new("root-cert")
    root_ca_file.write(root_ca.to_pem())
    root_ca_file.close
    # No need to write root key to file

    intermediate = create_certificate("intermediate-ca", 2, root)
    # The intermediate_ca is not written to a file - it will be in the
    # client and service certificates' chain.

    # Test both (root->cert) and (root->intermediate->cert)
    parents = [root, intermediate]
    parents.each do |parent|
      client_cert, client_key = create_certificate("client-cert", 3, parent, false)
      client_cert_file = Tempfile.new("client-cert")
      client_cert_file.write(client_cert.to_pem())
      if parent != root
        client_cert_file.write(parent[0].to_pem())
      end
      client_cert_file.close

      client_key_file = Tempfile.new("client-key")
      client_key_file.write(client_key.to_pem())
      client_key_file.close

      server_cert, server_key = create_certificate("server-cert", 4, parent, false)

      server_cert_file = Tempfile.new("server-cert")
      server_cert_file.write(server_cert.to_pem())
      if parent != root
        server_cert_file.write(parent[0].to_pem())
      end
      server_cert_file.close

      server_key_file = Tempfile.new("server-key")
      server_key_file.write(server_key.to_pem())
      server_key_file.close

      input_conf = TARGET_CONFIG + %[
                     <transport tls>
                       cert_path #{server_cert_file.path}
                       private_key_path #{server_key_file.path}
                       client_cert_auth true
                       ca_path #{root_ca_file.path}
                     </transport>
                   ]
      target_input_driver = create_target_input_driver(conf: input_conf)

      output_conf = %[
        send_timeout 5
        require_ack_response #{ack}
        transport tls
        tls_verify_hostname false
        tls_cert_path #{root_ca_file.path}
        tls_client_cert_path #{client_cert_file.path}
        tls_client_private_key_path #{client_key_file.path}
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
        d.run(default_tag: 'test', wait_flush_completion: false, shutdown: false) do
          records.each do |record|
            d.feed(time, record)
          end
        end
      end

      events = target_input_driver.events
      assert_not_equal(events, [])
      assert_equal(['test', time, records[0]], events[0])
      assert_equal(['test', time, records[1]], events[1])
    end
  end

  test 'a destination node not supporting responses by just ignoring' do
    target_input_driver = create_target_input_driver(response_stub: ->(_option) { nil }, disconnect: false)

    @d = d = create_driver(CONFIG + %[
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

    @d = d = create_driver(CONFIG + %[
      require_ack_response true
      ack_response_timeout 5s
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

    assert_equal (5 + 2), delayed_commit_timeout_value

    events = target_input_driver.events
    assert_equal ['test', time, records[0]], events[0]
    assert_equal ['test', time, records[1]], events[1]

    assert{ d.instance.rollback_count > 0 }

    logs = d.instance.log.logs
    assert{ logs.any?{|log| log.include?("no response from node. regard it as unavailable.") } }
  end

  test 'authentication_with_shared_key' do
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
    input_conf = TARGET_CONFIG + %[
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
        port #{TARGET_PORT}
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
    assert_false(logs.any? { |log| log.include?("invalid format for PONG message") || log.include?("shared key mismatch") }, "'#{logs.last.strip}' happens")
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

  def create_certificate(name, serial, parent = nil, ca = true)
    key = OpenSSL::PKey::RSA.new 2048
    cert = OpenSSL::X509::Certificate.new

    parent_cert = cert
    parent_key = key
    if parent
      parent_cert = parent[0]
      parent_key = parent[1]
    end

    cert.version = 2
    cert.serial = serial
    cert.subject = OpenSSL::X509::Name.parse "/CN=#{name}"
    cert.issuer = parent_cert.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now
    cert.not_after = cert.not_before + 60*60 # 1 hour
    ef = OpenSSL::X509::ExtensionFactory.new
    ef.subject_certificate = cert
    ef.issuer_certificate = parent_cert
    cert.add_extension(ef.create_extension("subjectKeyIdentifier", "hash", false))
    if ca
      cert.add_extension(ef.create_extension("basicConstraints", "CA:TRUE", true))
      cert.add_extension(ef.create_extension("keyUsage", "keyCertSign, cRLSign", true))
      cert.add_extension(ef.create_extension("authorityKeyIdentifier", "keyid:always", false))
    else
      cert.add_extension(ef.create_extension("keyUsage", "digitalSignature", true))
    end
    cert.sign(parent_key, OpenSSL::Digest::SHA256.new)
    [cert, key]
  end

  test 'heartbeat_type_none' do
    @d = d = create_driver(CONFIG + "\nheartbeat_type none")
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
    @d = d = create_driver(CONFIG + "\nheartbeat_type udp")

    d.instance_start
    usock = d.instance.instance_variable_get(:@usock)
    servers = d.instance.instance_variable_get(:@_servers)
    timers = d.instance.instance_variable_get(:@_timers)
    assert_equal Fluent::PluginHelper::Socket::WrappedSocket::UDP, usock.class
    assert_kind_of UDPSocket, usock
    assert servers.find{|s| s.title == :out_forward_heartbeat_receiver }
    assert timers.include?(:out_forward_heartbeat_request)

    mock(usock).send("\0", 0, Socket.pack_sockaddr_in(TARGET_PORT, '127.0.0.1')).once
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
    mock.proxy(Fluent::Plugin).new_sd(:static, anything) { |v|
      stub(v).plugin_id_for_test? { false }
    }.once

    output = Fluent::Test::Driver::Output.new(Fluent::Plugin::ForwardOutput) {
      def plugin_id_for_test?
        false
      end
    }

    assert_nothing_raised do
      output.configure(CONFIG + %[
        @id unique_out_forward
      ])
    end
  end

  sub_test_case 'verify_connection_at_startup' do
    test 'nodes are not available' do
      @d = d = create_driver(CONFIG + %[
        verify_connection_at_startup true
      ])
      e = assert_raise Fluent::UnrecoverableError do
        d.instance_start
      end
      assert_match(/Connection refused/, e.message)

      d.instance_shutdown
    end

    test 'nodes_shared_key_miss_match' do
      input_conf = TARGET_CONFIG + %[
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
          port #{TARGET_PORT}
        </server>
      ]
      @d = d = create_driver(output_conf)

      target_input_driver.run(expect_records: 1, timeout: 1) do
        e = assert_raise Fluent::UnrecoverableError do
          d.instance_start
        end
        assert_match(/Failed to establish connection/, e.message)
      end
    end

    test 'nodes_shared_key_miss_match with TLS' do
      input_conf = TARGET_CONFIG + %[
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
          port #{TARGET_PORT}
        </server>
      ]
      @d = d = create_driver(output_conf)

      target_input_driver.run(expect_records: 1, timeout: 1) do
        e = assert_raise Fluent::UnrecoverableError do
          d.instance_start
        end

        assert_match(/Failed to establish connection/, e.message)
      end
    end

    test 'nodes_shared_key_match' do
      input_conf = TARGET_CONFIG + %[
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
            port #{TARGET_PORT}
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
    target_input_driver = create_target_input_driver(conf: TARGET_CONFIG)
    output_conf = CONFIG
    d = create_driver(output_conf)
    d.instance_start

    begin
      chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
      mock.proxy(d.instance).socket_create_tcp(TARGET_HOST, TARGET_PORT, anything) { |sock| mock(sock).close.once; sock }.twice

      target_input_driver.run(timeout: 15) do
        d.run(shutdown: false) do
          node = d.instance.nodes.first
          2.times do
            node.send_data('test', chunk) rescue nil
          end
        end
      end
    ensure
      d.instance_shutdown
    end
  end

  test 'if no available node' do
    # do not create output driver
    d = create_driver(%[
    <server>
      name test
      standby
      host #{TARGET_HOST}
      port #{TARGET_PORT}
    </server>
    ])
    d.instance_start
    assert_nothing_raised { d.run }
  end

  sub_test_case 'keepalive' do
    test 'Do not create connection per send_data' do
      target_input_driver = create_target_input_driver(conf: TARGET_CONFIG)
      output_conf = CONFIG + %[
        keepalive true
        keepalive_timeout 2
      ]
      d = create_driver(output_conf)
      d.instance_start

      begin
        chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
        mock.proxy(d.instance).socket_create_tcp(TARGET_HOST, TARGET_PORT, anything) { |sock| mock(sock).close.once; sock }.once

        target_input_driver.run(timeout: 15) do
          d.run(shutdown: false) do
            node = d.instance.nodes.first
            2.times do
              node.send_data('test', chunk) rescue nil
            end
          end
        end
      ensure
        d.instance_shutdown
      end
    end

    sub_test_case 'with require_ack_response' do
      test 'Create connection per send_data' do
        target_input_driver = create_target_input_driver(conf: TARGET_CONFIG)
        output_conf = CONFIG + %[
          require_ack_response true
          keepalive true
          keepalive_timeout 2
        ]
        d = create_driver(output_conf)
        d.instance_start

        begin
          chunk = Fluent::Plugin::Buffer::MemoryChunk.new(Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil))
          mock.proxy(d.instance).socket_create_tcp(TARGET_HOST, TARGET_PORT, anything) { |sock| mock(sock).close.once; sock }.twice

          target_input_driver.run(timeout: 15) do
            d.run(shutdown: false) do
              node = d.instance.nodes.first
              2.times do
                node.send_data('test', chunk) rescue nil
              end
            end
          end
        ensure
          d.instance_shutdown
        end
      end
    end
  end
end
