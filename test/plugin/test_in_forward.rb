require_relative '../helper'

require 'fluent/test'
require 'base64'

require 'fluent/env'
require 'fluent/plugin/in_forward'

class ForwardInputTest < Test::Unit::TestCase
  class << self
    def startup
      socket_manager_path = ServerEngine::SocketManager::Server.generate_path
      @server = ServerEngine::SocketManager::Server.open(socket_manager_path)
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
    end

    def shutdown
      @server.close
    end
  end

  def setup
    Fluent::Test.setup
    @responses = []  # for testing responses after sending data
  end

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ForwardInput).configure(conf)
  end

  class Configure < self
    def test_simple
      d = create_driver
      assert_equal PORT, d.instance.port
      assert_equal '127.0.0.1', d.instance.bind
      assert_equal 0, d.instance.linger_timeout
      assert_equal 0.5, d.instance.blocking_timeout
      assert !d.instance.backlog
    end
  end

  # TODO: Will add Loop::run arity check with stub/mock library

  def connect
    TCPSocket.new('127.0.0.1', PORT)
  end

  class Message < self
    def test_time
      d = create_driver

      time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")
      Fluent::Engine.now = time

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}],
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        records.each {|tag, _time, record|
          send_data Fluent::Engine.msgpack_factory.packer.write([tag, 0, record]).to_s
        }
      end
      assert_equal(records, d.emits.sort_by{|a, b| a[0] <=> b[0] })
    end

    def test_plain
      d = create_driver

      time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}],
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        records.each {|tag, _time, record|
          send_data Fluent::Engine.msgpack_factory.packer.write([tag, _time, record]).to_s
        }
      end
      assert_equal(records, d.emits)
    end

    def test_time_as_integer
      d = create_driver

      time = Time.parse("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}],
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        records.each {|tag, _time, record|
          send_data Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
        }
      end

      assert_equal(records, d.emits)
    end

    def test_skip_invalid_event
      d = create_driver(CONFIG + "skip_invalid_event true")

      time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a" => 1}],
        ["tag2", time, {"a" => 2}],
      ]

      d.run do
        entries = records.map { |tag, _time, record| [tag, _time, record] }
        # These entries are skipped
        entries << ['tag1', true, {'a' => 3}] << ['tag2', time, 'invalid record']

        entries.each {|tag, _time, record|
          # Without ack, logs are sometimes not saved to logs during test.
          send_data Fluent::Engine.msgpack_factory.packer.write([tag, _time, record]).to_s, true
        }
      end

      assert_equal 2, d.instance.log.logs.count { |line| line =~ /got invalid event and drop it/ }
      assert_equal records[0], d.emits[0]
      assert_equal records[1], d.emits[1]
    end

    def test_json
      d = create_driver

      time = Time.parse("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}],
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        records.each {|tag, _time, record|
          send_data [tag, _time, record].to_json
        }
      end

      assert_equal(records, d.emits.sort_by{|a, b| a[1] <=> b[1] })
    end
  end

  class Forward < self
    def test_plain
      d = create_driver

      time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        entries = []
        records.each {|tag, _time, record|
          entries << [_time, record]
        }
        send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
      end
      assert_equal(records, d.emits)
    end

    def test_time_as_integer
      d = create_driver

      time = Time.parse("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        entries = []
        records.each {|tag, _time, record|
          entries << [_time, record]
        }
        send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
      end

      assert_equal(records, d.emits)
    end

    def test_skip_invalid_event
      d = create_driver(CONFIG + "skip_invalid_event true")

      time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a" => 1}],
        ["tag1", time, {"a" => 2}],
      ]

      d.expected_emits_length = records.length
      d.run_timeout = 2
      d.run do
        entries = records.map { |tag, _time, record| [_time, record] }
        # These entries are skipped
        entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

        send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
      end

      assert_equal 2, d.instance.log.logs.count { |line| line =~ /skip invalid event/ }
    end
  end

  def test_packed_forward
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      entries = ''
      records.each {|_tag, _time, record|
        Fluent::Engine.msgpack_factory.packer(entries).write([_time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal(records, d.emits)
  end

  def test_packed_forward_with_time_as_integer
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      entries = ''
      records.each {|_tag, _time, record|
        Fluent::Engine.msgpack_factory.packer(entries).write([_time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal(records, d.emits)
  end

  def test_packed_forward_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a" => 1}],
      ["tag1", time, {"a" => 2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      entries = records.map { |_tag , _time, record| [_time, record] }
      # These entries are skipped
      entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

      packed_entries = ''
      entries.each {|_time, record|
        Fluent::Engine.msgpack_factory.packer(packed_entries).write([_time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", packed_entries]).to_s
    end

    assert_equal 2, d.instance.log.logs.count { |line| line =~ /skip invalid event/ }
  end

  def test_send_large_chunk_warning
    d = create_driver(CONFIG + %[
      chunk_size_warn_limit 16M
      chunk_size_limit 32M
    ])

    time = Fluent::EventTime.parse("2014-04-25 13:14:15 UTC")

    # generate over 16M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack
    assert chunk.size > (16 * 1024 * 1024)
    assert chunk.size < (32 * 1024 * 1024)

    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
        d.instance.send(:emit_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
      end
    end

    # check emitted data
    emits = d.emits
    assert_equal 16, emits.size
    assert emits.map(&:first).all?{|t| t == "test.tag" }
    assert_equal (0...16).to_a, emits.map{|_tag, t, _record| t - time }

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
    time = Fluent::EventTime.parse("2014-04-25 13:14:15 UTC")

    # generate over 16M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack

    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
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

    time = Fluent::EventTime.parse("2014-04-25 13:14:15 UTC")

    # generate over 32M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...32).map{|i| [time + i, {"data" => str}] } ].to_msgpack
    assert chunk.size > (32 * 1024 * 1024)

    # d.run => send_data
    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
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

  data('string chunk' => 'broken string',
       'integer chunk' => 10)
  def test_send_broken_chunk(data)
    d = create_driver

    # d.run => send_data
    d.run do
      d.instance.send(:emit_message, data, 1000000000, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
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

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      events.each {|tag, _time, record|
        op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
        expected_acks << op['chunk']
        send_data [tag, _time, record, op].to_msgpack, true
      }
    end

    assert_equal events, d.emits
    assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
  end

  # FIX: response is not pushed into @responses because IO.select has been blocked until InputForward shutdowns
  def test_respond_to_forward_requiring_ack
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      entries = []
      events.each {|_tag, _time, record|
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

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    expected_acks = []

    d.run do
      entries = ''
      events.each {|_tag, _time, record|
        [_time, record].to_msgpack(entries)
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
      events.each {|tag, _time, record|
        op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
        expected_acks << op['chunk']
        send_data [tag, _time, record, op].to_json, true
      }
    end

    assert_equal events, d.emits
    assert_equal expected_acks, @responses.map { |res| JSON.parse(res)['ack'] }

  end

  def test_not_respond_to_message_not_requiring_ack
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      events.each {|tag, _time, record|
        send_data [tag, _time, record].to_msgpack, true
      }
    end

    assert_equal events, d.emits
    assert_equal ["", ""], @responses
  end

  def test_not_respond_to_forward_not_requiring_ack
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      entries = []
      events.each {|_tag, _time, record|
        entries << [_time, record]
      }
      send_data ["tag1", entries].to_msgpack, true
    end

    assert_equal events, d.emits
    assert_equal [""], @responses
  end

  def test_not_respond_to_packed_forward_not_requiring_ack
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length

    d.run do
      entries = ''
      events.each {|_tag, _time, record|
        [_time, record].to_msgpack(entries)
      }
      send_data ["tag1", entries].to_msgpack, true
    end

    assert_equal events, d.emits
    assert_equal [""], @responses
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
      events.each {|tag, _time, record|
        send_data [tag, _time, record].to_json, true
      }
    end

    assert_equal events, d.emits
    assert_equal ["", ""], @responses
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

  # Data ordering is not assured:
  #  Records in different sockets are processed on different thread, so its scheduling make effect
  #  on order of emitted records.
  #  So, we MUST sort emitted records in different `send_data` before assertion.
  def send_data(data, try_to_receive_response=false, response_timeout=5, auth: false)
    io = connect

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

  # TODO heartbeat
end
