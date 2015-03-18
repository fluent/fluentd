require_relative '../helper'
require 'fluent/test'
require 'base64'
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

  def connect
    TCPSocket.new('127.0.0.1', PORT)
  end

  def test_time
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")
    Fluent::Engine.now = time

    records = [
      ["tag", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      records.each {|tag,time,record|
        send_data Fluent::Engine.msgpack_factory.packer.write([tag, 0, record]).to_s
      }
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_message
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    d.run do
      records.each {|tag,time,record|
        send_data Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
      }
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_message_with_time_as_integer
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      records.each {|tag,time,record|
        send_data Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
      }
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_message_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]

    d.run do
      # These entries are skipped
      records << ['tag1', true, {'a' => 3}] << ['tag2', time, 'invalid record']

      records.each { |tag, time, record|
        # Without ack, logs are sometimes not saved to logs during test.
        send_data Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s, true
      }
    end
    assert_equal 2, d.instance.log.logs.count { |line| line =~ /got invalid event and drop it/ }
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_forward
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.run do
      entries = []
      records.each {|tag,time,record|
        entries << [time, record]
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_forward_with_time_as_integer
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      entries = []
      records.each {|tag,time,record|
        entries << [time, record]
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_forward_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    d.expect_emit "tag1", time, {"a" => 1}
    d.expect_emit "tag1", time, {"a" => 2}

    d.run do
      entries = d.expected_emits.map { |tag, time, record| [time, record] }
      # These entries are skipped
      entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal 2, d.instance.log.logs.count { |line| line =~ /skip invalid event/ }
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_packed_forward
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.run do
      entries = ''
      records.each {|tag,time,record|
        Fluent::Engine.msgpack_factory.packer(entries).write([time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
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
      records.each {|tag,time,record|
        Fluent::Engine.msgpack_factory.packer(entries).write([time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_packed_forward_with_skip_invalid_event
    d = create_driver(CONFIG + "skip_invalid_event true")

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a" => 1}],
      ["tag1", time, {"a" => 2}],
    ]

    d.run do
      entries = records.map { |tag, time, record| [time, record] }
      # These entries are skipped
      entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

      packed_entries = ''
      entries.each { |time, record|
        Fluent::Engine.msgpack_factory.packer(packed_entries).write([time, record]).flush
      }
      send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", packed_entries]).to_s
    end
    assert_equal 2, d.instance.log.logs.count { |line| line =~ /skip invalid event/ }
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
  end

  def test_message_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]

    d.expected_emits_length = records.length
    d.run_timeout = 2
    d.run do
      records.each {|tag,time,record|
        send_data [tag, time, record].to_json
      }
    end
    assert_equal records[0], d.emits[0]
    assert_equal records[1], d.emits[1]
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

    d.expected_emits_length = 16
    d.run_timeout = 2
    d.run do
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
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
    time = Fluent::EventTime.parse("2014-04-25 13:14:15 UTC")

    # generate over 16M chunk
    str = "X" * 1024 * 1024
    chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack

    d.expected_emits_length = 16
    d.run_timeout = 2
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

    d.end_if {
      d.instance.log.logs.select{|line| line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_limit', dropped:/ }.size == 1
    }
    d.run_timeout = 2
    # d.run => send_data
    d.run do
<<<<<<< a524e982c27ac5d2a43747d3593f51e2d706fbdb
      Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
=======
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:emit_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
>>>>>>> Re-implement in_forward standard features w/ v0.14 APIs (tests: green)
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
      d.instance.send(:on_message, data, 1000000000, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
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
    d.run_timeout = 2

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

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
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
    d.run_timeout = 2

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
    d.run_timeout = 2

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

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = events.length
    d.run_timeout = 2

    d.run do
      events.each {|tag,time,record|
        send_data [tag, time, record].to_msgpack, true
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
    d.run_timeout = 2

    d.run do
      entries = []
      events.each {|tag,time,record|
        entries << [time, record]
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
    d.run_timeout = 2

    d.run do
      entries = ''
      events.each {|tag,time,record|
        [time, record].to_msgpack(entries)
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
    d.run_timeout = 2

    d.run do
      events.each {|tag,time,record|
        send_data [tag, time, record].to_json, true
      }
    end

    assert_equal events, d.emits
    assert_equal ["", ""], @responses
  end

  def send_data(data, try_to_receive_response=false, response_timeout=5)
    io = connect
    res = nil
    begin
      io.write data
      if try_to_receive_response
        if IO.select([io], nil, nil, response_timeout)
          res = io.recv(1024)
        else # timeout
          res = nil
        end
        # res == "" if disconnected from server without any responses
        # res == nil if timeout
      end
    ensure
      io.close
    end
    @responses << res if try_to_receive_response
  end

  # TODO heartbeat
end
