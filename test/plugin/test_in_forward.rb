require_relative '../helper'
require 'fluent/test'
require 'base64'

class ForwardInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @responses = []  # for testing responses after sending data
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/in_forward")
  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
  ]

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

  def test_stop_configure
    d = create_driver(CONFIG + %[stop_file #{TMP_DIR}/stop\nstop_check_interval 1])
    assert_equal "#{TMP_DIR}/stop", d.instance.stop_file
    assert_equal 1, d.instance.stop_check_interval
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
        send_data [tag, time, record].to_msgpack
      }
    end
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
      send_data ["tag1", entries].to_msgpack
    end
  end

  def test_packed_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data ["tag1", entries].to_msgpack
    end
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
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
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
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
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
      MessagePack::Unpacker.new.feed_each(chunk) do |obj|
        d.instance.send(:on_message, obj, chunk.size, "host: 127.0.0.1, addr: 127.0.0.1, port: 0000")
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

  def test_stop
    d = create_driver(CONFIG + %[stop_file #{TMP_DIR}/stop])

    File.open("#{TMP_DIR}/stop", "w").close
    stub(d.instance).active? { true }
    mock(d.instance).deactivate
    d.instance.send(:on_stop_check_timer)

    File.unlink("#{TMP_DIR}/stop")
    stub(d.instance).active? { false }
    mock(d.instance).activate
    d.instance.send(:on_stop_check_timer)
  end

  # TODO heartbeat
end
