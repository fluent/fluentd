require_relative '../helper'

require 'fluent/test'
require 'base64'

require 'fluent/env'
require 'fluent/plugin/in_forward'

class ForwardInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @responses = []  # for testing responses after sending data
  end

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
  ]
  PEERADDR = ['?', '0000', '127.0.0.1', '127.0.0.1']

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ForwardInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal PORT, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 0, d.instance.linger_timeout
    assert_equal 0.5, d.instance.blocking_timeout
    assert !d.instance.backlog
  end

  # TODO: Will add Loop::run arity check with stub/mock library

  def connect
    TCPSocket.new('127.0.0.1', PORT)
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, 0, record].to_msgpack
      }
    end
  end

  def test_message
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
      }
    end
  end

  def test_message_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a" => 1}
    d.expect_emit "tag2", time, {"a" => 2}

    d.run do
      entries = d.expected_emits.map { |tag, time, record| [tag, time, record] }
      # These entries are skipped
      entries << ['tag1', true, {'a' => 3}] << ['tag2', time, 'invalid record']

      entries.each { |tag, time, record|
        # Without ack, logs are sometimes not saved to logs during test.
        send_data Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s, true
      }
    end

    assert_equal 2, d.instance.log.logs.count { |line| line =~ /got invalid event and drop it/ }
  end

  def test_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = []
      d.expected_emits.each {|tag,time,record|
        entries << [time, record]
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
  end

  def test_forward_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a" => 1}
    d.expect_emit "tag1", time, {"a" => 2}

    d.run do
      entries = d.expected_emits.map { |tag, time, record| [time, record] }
      # These entries are skipped
      entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end

    assert_equal 2, d.instance.log.logs.count { |line| line =~ /skip invalid event/ }
  end

  def test_packed_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        Fluent::Engine.msgpack_factory.packer(entries).write([time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
  end

  def test_packed_forward_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a" => 1}
    d.expect_emit "tag1", time, {"a" => 2}

    d.run do
      entries = d.expected_emits.map { |tag ,time, record| [time, record] }
      # These entries are skipped
      entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

      packed_entries = ''
      entries.each { |time, record|
        Fluent::Engine.msgpack_factory.packer(packed_entries).write([time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", packed_entries]).to_s
    end

    assert_equal 2, d.instance.log.logs.count { |line| line =~ /skip invalid event/ }
  end

  def test_message_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, time, record].to_json
      }
    end
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

    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, PEERADDR)
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

    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, PEERADDR)
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

    # d.run => send_data
    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, PEERADDR)
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

  data('string chunk' => 'broken string',
       'integer chunk' => 10)
  def test_send_broken_chunk(data)
    d = create_driver

    # d.run => send_data
    d.run do
      d.instance.send(:on_message, data, 1000000000, PEERADDR)
    end

    # check emitted data
    emits = d.emits
    assert_equal 0, emits.size

    # check log
    assert d.instance.log.logs.select{|line|
      line =~ / \[warn\]: incoming chunk is broken: source="host: 127.0.0.1, addr: 127.0.0.1, port: \d+" msg=#{data.inspect}/
    }.size == 1, "should not accept broken chunk"
  end

  def test_respond_to_message_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      events.each {|tag,time,record|
        op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
        expected_acks << op['chunk']
        send_data [tag, time, record, op].to_msgpack, true
      }
    end

    assert_equal events, d.emits
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  # FIX: response is not pushed into @responses because IO.select has been blocked until InputForward shutdowns
  def test_respond_to_forward_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      entries = []
      events.each {|tag,time,record|
        entries << [time, record]
      }
      op = { 'chunk' => Base64.encode64(entries.object_id.to_s) }
      expected_acks << op['chunk']
      send_data ["tag1", entries, op].to_msgpack, true
    end

    assert_equal events, d.emits
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  def test_respond_to_packed_forward_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      entries = ''
      events.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      op = { 'chunk' => Base64.encode64(entries.object_id.to_s) }
      expected_acks << op['chunk']
      send_data ["tag1", entries, op].to_msgpack, true
    end

    assert_equal events, d.emits
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  def test_respond_to_message_json_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      events.each {|tag,time,record|
        op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
        expected_acks << op['chunk']
        send_data [tag, time, record, op].to_json, true
      }
    end

    assert_equal events, d.emits
    assert_equal expected_acks, @responses.map { |res| JSON.parse(res)['ack'] }
  end

  def test_not_respond_to_message_not_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      events.each {|tag,time,record|
        send_data [tag, time, record].to_msgpack, true
      }
    end

    assert_equal events, d.emits
    assert_equal [nil, nil], @responses
  end

  def test_not_respond_to_forward_not_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      entries = []
      events.each {|tag,time,record|
        entries << [time, record]
      }
      send_data ["tag1", entries].to_msgpack, true
    end

    assert_equal events, d.emits
    assert_equal [nil], @responses
  end

  def test_not_respond_to_packed_forward_not_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      entries = ''
      events.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data ["tag1", entries].to_msgpack, true
    end

    assert_equal events, d.emits
    assert_equal [nil], @responses
  end

  def test_not_respond_to_message_json_not_requiring_ack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      events.each {|tag,time,record|
        send_data [tag, time, record].to_json, true
      }
    end

    assert_equal events, d.emits
    assert_equal [nil, nil], @responses
  end

  def send_data(data, try_to_receive_response=false, response_timeout=1)
    io = connect
    begin
      io.write data
      if try_to_receive_response
        if IO.select([io], nil, nil, response_timeout)
          res = io.recv(1024)
        end
        # timeout means no response, so push nil to @responses
      end
    ensure
      io.close
    end
    @responses << res if try_to_receive_response
  end

  LOCALHOST_HOSTNAME_GETTER = ->(){sock = UDPSocket.new(::Socket::AF_INET); sock.do_not_reverse_lookup = false; sock.connect("127.0.0.1", 2048); sock.peeraddr[2] }
  LOCALHOST_HOSTNAME = LOCALHOST_HOSTNAME_GETTER.call
  DUMMY_SOCK = Struct.new(:remote_host, :remote_addr, :remote_port).new(LOCALHOST_HOSTNAME, "127.0.0.1", 0)

  sub_test_case 'source_hostname_key and source_address_key features' do
    test 'resolve_hostname must be true with source_hostname_key' do
      assert_raise(Fluent::ConfigError) {
        create_driver(CONFIG + <<EOS)
resolve_hostname false
source_hostname_key hostname
EOS
      }
    end
    data(
      both: [:hostname, :address],
      hostname: [:hostname],
      address: [:address],
    )
    test 'message protocol' do |keys|
      execute_test(*keys) { |events|
        events.each { |tag, time, record|
          send_data [tag, time, record].to_msgpack
        }
      }
    end

    data(
      both: [:hostname, :address],
      hostname: [:hostname],
      address: [:address],
    )
    test 'forward protocol' do |keys|
      execute_test(*keys) { |events|
        entries = []
        events.each {|tag,time,record|
          entries << [time, record]
        }
        send_data ['tag1', entries].to_msgpack
      }
    end

    data(
      both: [:hostname, :address],
      hostname: [:hostname],
      address: [:address],
    )
    test 'packed forward protocol' do |keys|
      execute_test(*keys) { |events|
        entries = ''
        events.each { |tag, time, record|
          Fluent::Engine.msgpack_factory.packer(entries).write([time, record]).flush
        }
        send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
      }
    end

    def execute_test(*keys, &block)
      conf = CONFIG.dup
      if keys.include?(:hostname)
        conf << <<EOL
source_hostname_key source_hostname
EOL
      end
      if keys.include?(:address)
        conf << <<EOL
source_address_key source_address
EOL
      end
      d = create_driver(conf)

      time = Time.parse("2011-01-02 13:14:15 UTC").to_i
      events = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]
      d.expected_emits_length = events.length

      d.run do
        block.call(events)
      end

      d.emits.each { |tag, _time, record|
        if keys.include?(:hostname)
          assert_true record.has_key?('source_hostname')
          assert_equal DUMMY_SOCK.remote_host, record['source_hostname']
          unless keys.include?(:address)
            assert_false record.has_key?('source_address')
          end
        end
        if keys.include?(:address)
          assert_true record.has_key?('source_address')
          assert_equal DUMMY_SOCK.remote_addr, record['source_address']
          unless keys.include?(:hostname)
            assert_false record.has_key?('source_hostname')
          end
        end
      }
    end
  end

  # TODO heartbeat
end
