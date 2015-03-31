require_relative '../helper'
require 'fluent/test'
require 'base64'
require 'socket'
require 'openssl'
require 'json'
require 'msgpack'

require 'fluent/plugin/in_forward'

class ForwardInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @responses = []  # for testing responses after sending data
  end

  PORT, REMOTE_PORT = unused_port(2) # REMOTE_PORT is used for dummy source of heartbeat

  SHARED_KEY = 'foobar2'
  USER_NAME = 'tagomoris'
  USER_PASSWORD = 'fluentd'

  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
  ]
  SSL_CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
    <ssl>
      version TLSv1.2
      cert_auto_generate yes
      digest SHA512
      algorithm RSA
      key_length 4096
    </ssl>
  ]
  CONFIG_AUTH = %[
    port #{PORT}
    bind 127.0.0.1
    <security>
      shared_key foobar1
      user_auth true
      <user>
        username #{USER_NAME}
        password #{USER_PASSWORD}
      </user>
      <client>
        network 127.0.0.0/8
        shared_key #{SHARED_KEY}
        users ["#{USER_NAME}"]
      </client>
    </security>
  ]
  SSL_CONFIG_AUTH = %[
    port #{PORT}
    bind 127.0.0.1
    <ssl>
      cert_auto_generate yes
    </ssl>
    <security>
      shared_key foobar1
      user_auth true
      <user>
        username #{USER_NAME}
        password #{USER_PASSWORD}
      </user>
      <client>
        network 127.0.0.0/8
        shared_key #{SHARED_KEY}
        users ["#{USER_NAME}"]
      </client>
    </security>
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ForwardInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal PORT, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 0, d.instance.linger_timeout
    assert !d.instance.backlog
  end

  def test_configure_auth
    d = create_driver(CONFIG_AUTH)
    assert_equal PORT, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 0, d.instance.linger_timeout
    assert !d.instance.backlog

    assert d.instance.security
    assert_equal 1, d.instance.security.users.size
    assert_equal 1, d.instance.security.clients.size
  end

  def test_configure_ssl
    d = create_driver(SSL_CONFIG)
    assert_equal PORT, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert d.instance.ssl_options
    assert_equal :TLSv1_2, d.instance.ssl_options.version
    assert d.instance.ssl_options.cert_auto_generate
    assert_equal OpenSSL::Digest::SHA512, d.instance.ssl_options.digest
    assert_equal OpenSSL::PKey::RSA, d.instance.ssl_options.algorithm
    assert_equal 4096, d.instance.ssl_options.key_length
  end

  def connect
    TCPSocket.new('127.0.0.1', PORT)
  end

  def connect_ssl
    sock = OpenSSL::SSL::SSLSocket.new(TCPSocket.new('127.0.0.1', PORT))
    sock.sync = true
    sock.sync_close = true
    sock.connect
    sock
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    records = [
      ["tag", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      records.each {|tag,time,record|
        send_data false, false, [tag, 0, record].to_msgpack
      }
    end
    assert_equal records, d.emits.sort{|a,b| a[0] <=> b[0] }
  end

  def test_message
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time+1, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      records.each {|tag,time,record|
        send_data false, false, [tag, time, record].to_msgpack
      }
    end
    assert_equal records, d.emits.sort{|a,b| a[1] <=> b[1] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_forward(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time+1, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      entries = []
      records.each {|tag,time,record|
        entries << [time, record]
      }
      send_data auth, ssl, ["tag1", entries].to_msgpack
    end
    assert_equal records, d.emits.sort{|a,b| a[1] <=> b[1] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_packed_forward(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time+1, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      entries = ''
      records.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data auth, ssl, ["tag1", entries].to_msgpack
    end
    assert_equal records, d.emits.sort{|a,b| a[1] <=> b[1] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true]) # with json, auth doesn't work
  def test_message_json(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time+1, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      records.each {|tag,time,record|
        send_data auth, ssl, [tag, time, record].to_json
      }
    end
    assert_equal records, d.emits.sort{|a,b| a[1] <=> b[1] }
  end

  def test_send_large_chunk_warning
    d = create_driver(CONFIG + %[
      chunk_size_warn_limit 16M
      chunk_size_limit 32M
    ])

    time = Time.parse("2014-04-25 13:14:15 UTC").to_i

    # generate over 16M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack
    assert chunk.size > (16 * 1024 * 1024)
    assert chunk.size < (32 * 1024 * 1024)

    d.expected_emits_length = 16
    d.run_timeout = 2
    d.run do
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:emit_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
      end
    end

    # check emitted data
    emits = d.emits
    assert_equal 16, emits.size
    assert emits.map(&:first).all?{|t| t == "test.tag" }
    assert_equal (0...16).to_a, emits.map{|tag, t, record| t - time }

    # check log
    assert d.instance.log.logs.select{|line|
      line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_warn_limit':/ &&
      line =~ / tag="test.tag" source="host: 127.0.0.1, addr: 127.0.0.1, port: \d+" limit=16777216 size=16777501/
    }.size == 1, "large chunk warning is not logged"
  end

  def test_send_large_chunk_only_warning
    d = create_driver(CONFIG + %[
      chunk_size_warn_limit 16M
    ])
    time = Time.parse("2014-04-25 13:14:15 UTC").to_i

    # generate over 16M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack

    d.expected_emits_length = 16
    d.run_timeout = 2
    d.run do
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:emit_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
      end
    end

    # check log
    assert d.instance.log.logs.select{ |line|
      line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_warn_limit':/ &&
      line =~ / tag="test.tag" source="host: 127.0.0.1, addr: 127.0.0.1, port: \d+" limit=16777216 size=16777501/
    }.size == 1, "large chunk warning is not logged"
  end

  def test_send_large_chunk_limit
    d = create_driver(CONFIG + %[
      chunk_size_warn_limit 16M
      chunk_size_limit 32M
    ])

    time = Time.parse("2014-04-25 13:14:15 UTC").to_i

    # generate over 32M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...32).map{|i| [time + i, {"data" => str}] } ].to_msgpack
    assert chunk.size > (32 * 1024 * 1024)

    d.end_if {
      d.instance.log.logs.select{|line| line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_limit', dropped:/ }.size == 1
    }
    d.run_timeout = 2
    # d.run => send_data
    d.run do
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:emit_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
      end
    end

    # check emitted data
    emits = d.emits
    assert_equal 0, emits.size

    # check log
    assert d.instance.log.logs.select{|line|
      line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_limit', dropped:/ &&
      line =~ / tag="test.tag" source="host: 127.0.0.1, addr: 127.0.0.1, port: \d+" limit=33554432 size=33554989/
    }.size == 1, "large chunk warning is not logged"
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_respond_to_message_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    expected_acks = []

    d.run do
      events.each {|tag,time,record|
        op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
        expected_acks << op['chunk']
        send_data auth, ssl, [tag, time, record, op].to_msgpack, true
      }
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_respond_to_forward_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    expected_acks = []

    d.run do
      entries = []
      events.each {|tag,time,record|
        entries << [time, record]
      }
      op = { 'chunk' => Base64.encode64(entries.object_id.to_s) }
      expected_acks << op['chunk']
      send_data auth, ssl, ["tag1", entries, op].to_msgpack, true
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_respond_to_packed_forward_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    expected_acks = []

    d.run do
      entries = ''
      events.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      op = { 'chunk' => Base64.encode64(entries.object_id.to_s) }
      expected_acks << op['chunk']
      send_data auth, ssl, ["tag1", entries, op].to_msgpack, true
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true]) # with json, auth doesn't work
  def test_respond_to_message_json_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    expected_acks = []

    d.run do
      events.each {|tag,time,record|
        op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
        expected_acks << op['chunk']
        send_data auth, ssl, [tag, time, record, op].to_json, true
      }
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal expected_acks, @responses.map { |res| JSON.parse(res)['ack'] }
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_not_respond_to_message_not_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    d.run do
      events.each {|tag,time,record|
        send_data auth, ssl, [tag, time, record].to_msgpack, true
      }
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal ["", ""], @responses
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_not_respond_to_forward_not_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    d.run do
      entries = []
      events.each {|tag,time,record|
        entries << [time, record]
      }
      send_data auth, ssl, ["tag1", entries].to_msgpack, true
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal [""], @responses
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true], 'auth' => [CONFIG_AUTH, true, false], 'auth_ssl' => [SSL_CONFIG_AUTH, true, true])
  def test_not_respond_to_packed_forward_not_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    d.run do
      entries = ''
      events.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data auth, ssl, ["tag1", entries].to_msgpack, true
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal [""], @responses
  end

  data('tcp' => [CONFIG, false, false], 'ssl' => [SSL_CONFIG, false, true]) # with json, auth doen NOT work
  def test_not_respond_to_message_json_not_requiring_ack(data)
    conf, auth, ssl = data
    d = create_driver(conf)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time+1, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    d.run do
      events.each {|tag,time,record|
        send_data auth, ssl, [tag, time, record].to_json, true
      }
    end

    assert_equal events, d.emits.sort{|a,b| a[1] <=> b[1] }
    assert_equal ["", ""], @responses
  end

  def test_heartbeat_reply
    heartbeat_data = nil
    heartbeat_addr = nil

    bind = '0.0.0.0'
    listener_thread = Thread.new {
      usock = if IPAddr.new(IPSocket.getaddress(bind)).ipv4?
                    UDPSocket.new
                  else
                    UDPSocket.new(::Socket::AF_INET6)
                  end
      usock.bind(bind, REMOTE_PORT)
      heartbeat_arrived = false
      until heartbeat_arrived
        if IO.select([usock])
          heartbeat_data, heartbeat_addr = usock.recvfrom_nonblock(1024)
          heartbeat_arrived = true
        end
      end
    }
    until listener_thread.alive?
      sleep 0.01
    end

    d = create_driver
    d.end_if { ! heartbeat_data.nil? }
    d.run_timeout = 60
    d.run do
      sock = if IPAddr.new(IPSocket.getaddress(bind)).ipv4?
               UDPSocket.new
             else
               UDPSocket.new(::Socket::AF_INET6)
             end
      until heartbeat_data
        sock.send("\0", 0, '127.0.0.1', REMOTE_PORT)
        unless heartbeat_data
          sleep 1
        end
      end
    end

    assert_equal "\0", heartbeat_data
    assert_equal "127.0.0.1", heartbeat_addr[2]
  end

  CONFIG_AUTH_TEST = %[
    port #{PORT}
    bind 127.0.0.1
    <security>
      shared_key foobar1
      user_auth true
      <user>
        username #{USER_NAME}
        password #{USER_PASSWORD}
      </user>
      <client>
        network 127.0.0.0/8
        shared_key #{SHARED_KEY}
      </client>
    </security>
  ]

  data('tcp' => false, 'ssl' => true)
  def test_auth_invalid_shared_key(data)
    ssl = data
    conf = if ssl
             CONFIG_AUTH_TEST + %[<ssl>\n cert_auto_generate yes \n</ssl>\n]
           else
             CONFIG_AUTH_TEST
           end
    d = create_driver(conf)
    error = nil
    d.run do
      io = ssl ? connect_ssl : connect
      begin
        simulate_auth_sequence(io, 'fake shared key', USER_NAME, USER_PASSWORD)
      rescue => e
        error = e
        # "assert_raise" raises LocalJumpError here
      end
    end
    assert_equal RuntimeError, error.class
    assert_equal "Authentication Failure: shared_key mismatch", error.message
  end

  data('tcp' => false, 'ssl' => true)
  def test_auth_invalid_password(data)
    ssl = data
    conf = if ssl
             CONFIG_AUTH_TEST + %[<ssl>\n cert_auto_generate yes \n</ssl>\n]
           else
             CONFIG_AUTH_TEST
           end
    d = create_driver(conf)
    error = nil
    d.run do
      io = ssl ? connect_ssl : connect
      begin
        simulate_auth_sequence(io, SHARED_KEY, USER_NAME, 'fake-password')
      rescue RuntimeError => e
        error = e
        # "assert_raise" raises LocalJumpError here
      end
    end
    assert_equal "Authentication Failure: username/password mismatch", error.message
  end

  CONFIG_AUTH_TEST_2 = %[
    port #{PORT}
    bind 127.0.0.1
    <security>
      shared_key foobar1
      user_auth true
      <user>
        username tagomoris
        password fluentd
      </user>
      <user>
        username frsyuki
        password embulk
      </user>
      <client>
        network 127.0.0.0/8
        shared_key #{SHARED_KEY}
        users ["tagomoris"]
      </client>
    </security>
  ]

  data('tcp' => false, 'ssl' => true)
  def test_auth_disallowed_user(data)
    ssl = data
    conf = if ssl
             CONFIG_AUTH_TEST_2 + %[<ssl>\n cert_auto_generate yes \n</ssl>\n]
           else
             CONFIG_AUTH_TEST_2
           end
    d = create_driver(conf)
    error = nil
    d.run do
      io = ssl ? connect_ssl : connect
      begin
        simulate_auth_sequence(io, SHARED_KEY, 'frsyuki', 'embulk')
      rescue RuntimeError => e
        error = e
        # "assert_raise" raises LocalJumpError here
      end
    end
    assert_equal "Authentication Failure: username/password mismatch", error.message
  end

  CONFIG_AUTH_TEST_3 = %[
    port #{PORT}
    bind 127.0.0.1
    <security>
      shared_key foobar1
      user_auth true
      allow_anonymous_source no
      <user>
        username tagomoris
        password fluentd
      </user>
      <client>
        network 192.168.0.0/24
        shared_key #{SHARED_KEY}
        users ["tagomoris"]
      </client>
    </security>
  ]

  data('tcp' => false, 'ssl' => true)
  def test_auth_anonymous_host(data)
    ssl = data
    conf = if ssl
             CONFIG_AUTH_TEST_3 + %[<ssl>\n cert_auto_generate yes \n</ssl>\n]
           else
             CONFIG_AUTH_TEST_3
           end
    d = create_driver(conf)
    error = nil
    d.run do
      io = ssl ? connect_ssl : connect
      begin
        simulate_auth_sequence(io, SHARED_KEY, 'frsyuki', 'embulk')
      rescue RuntimeError => e
        error = e
        # "assert_raise" raises LocalJumpError here
      end
    end
    assert_equal "Authentication Failure: anonymous source host '127.0.0.1' denied", error.message
  end

  # res
  # '' : socket is disconnected without any data
  # nil: socket read timeout
  def read_data(io, timeout)
    res = ''
    timeout_at = Time.now + timeout
    begin
      buf = ''
      while io.read_nonblock(2048, buf)
        if buf == ''
          sleep 0.01
          break if Time.now >= timeout_at
          next
        end
        res << buf
        buf = ''
      end
      res = nil # timeout
    rescue IO::EAGAINWaitReadable
      sleep 0.01
      retry if res == ''
      # if res is not empty, all data in socket buffer are read, so do not retry
    rescue OpenSSL::SSL::SSLErrorWaitReadable
      sleep 0.01
      retry if res == ''
      # if res is not empty, all data in socket buffer are read, so do not retry
    rescue IOError, EOFError, Errno::ECONNRESET
      # socket disconnected
    end
    res
  end

  def simulate_auth_sequence(io, shared_key=SHARED_KEY, username=USER_NAME, password=USER_PASSWORD)
    auth_response_timeout = 2
    shared_key_salt = 'salt'

    # reading helo
    helo_data = read_data(io, auth_response_timeout)
    # ['HELO', options(hash)]
    helo = MessagePack.unpack(helo_data)
    raise "Invalid HELO header" unless helo[0] == 'HELO'
    raise "Invalid HELO option object" unless helo[1].is_a?(Hash)
    @options = helo[1]

    # sending ping
    ping = [
      'PING',
      'selfhostname',
      shared_key_salt,
      Digest::SHA512.new.update(shared_key_salt).update('selfhostname').update(shared_key).hexdigest,
    ]
    if @options['auth'] # auth enabled -> value is auth salt
      pass_digest = Digest::SHA512.new.update(@options['auth']).update(username).update(password).hexdigest
      ping.push(username, pass_digest)
    else
      ping.push('', '')
    end
    io.write ping.to_msgpack
    io.flush

    # reading pong
    pong_data = read_data(io, auth_response_timeout)
    # ['PING', bool(auth_result), string(reason_if_failed), self_hostname, shared_key_digest]
    pong = MessagePack.unpack(pong_data)
    raise "Invalid PONG header" unless pong[0] == 'PONG'
    raise "Authentication Failure: #{pong[2]}" unless pong[1]
    clientside_calculated = Digest::SHA512.new.update(shared_key_salt).update(pong[3]).update(shared_key).hexdigest
    raise "Shared key digest mismatch" unless clientside_calculated == pong[4]

    # authentication success
    true
  end

  def send_data(auth, ssl, data, try_to_receive_response=false, response_timeout=5)
    io = ssl ? connect_ssl : connect

    if auth
      simulate_auth_sequence(io)
    end

    res = nil
    io.write data
    io.flush
    if try_to_receive_response
      @responses << read_data(io, response_timeout)
    end
  ensure
    io.close rescue nil # SSL socket requires any writes to close sockets
  end

end
