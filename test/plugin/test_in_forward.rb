require_relative '../helper'

require 'fluent/test/driver/input'
require 'fluent/test/startup_shutdown'
require 'base64'

require 'fluent/env'
require 'fluent/event'
require 'fluent/plugin/in_forward'
require 'fluent/plugin/compressable'

require 'timecop'

class ForwardInputTest < Test::Unit::TestCase
 include Fluent::Plugin::Compressable

  def setup
    Fluent::Test.setup
    @responses = []  # for testing responses after sending data
    @d = nil
  end

  def teardown
    @d.instance_shutdown if @d
  end

  PORT = unused_port

  SHARED_KEY = 'foobar1'
  USER_NAME = 'tagomoris'
  USER_PASSWORD = 'fluentd'

  CONFIG = %[
    port #{PORT}
    bind 127.0.0.1
  ]
  LOCALHOST_HOSTNAME_GETTER = ->(){sock = UDPSocket.new(::Socket::AF_INET); sock.do_not_reverse_lookup = false; sock.connect("127.0.0.1", 2048); sock.peeraddr[2] }
  LOCALHOST_HOSTNAME = LOCALHOST_HOSTNAME_GETTER.call
  DUMMY_SOCK = Struct.new(:remote_host, :remote_addr, :remote_port).new(LOCALHOST_HOSTNAME, "127.0.0.1", 0)
  CONFIG_AUTH = %[
    port #{PORT}
    bind 127.0.0.1
    <security>
      self_hostname localhost
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

  sub_test_case '#configure' do
    test 'simple' do
      @d = d = create_driver
      assert_equal PORT, d.instance.port
      assert_equal '127.0.0.1', d.instance.bind
      assert_equal 0, d.instance.linger_timeout
      assert_equal 0.5, d.instance.blocking_timeout
      assert !d.instance.backlog
    end

    test 'auth' do
      @d = d = create_driver(CONFIG_AUTH)
      assert_equal PORT, d.instance.port
      assert_equal '127.0.0.1', d.instance.bind
      assert_equal 0, d.instance.linger_timeout
      assert !d.instance.backlog

      assert d.instance.security
      assert_equal 1, d.instance.security.users.size
      assert_equal 1, d.instance.security.clients.size
    end
  end

  sub_test_case 'message' do
    test 'time' do
      time = event_time("2011-01-02 13:14:15 UTC")
      begin
        Timecop.freeze(Time.at(time))
        @d = d = create_driver

        records = [
          ["tag1", time, {"a"=>1}],
          ["tag2", time, {"a"=>2}],
        ]

        d.run(expect_records: records.length, timeout: 5) do
          records.each {|tag, _time, record|
            send_data packer.write([tag, 0, record]).to_s
          }
        end
        assert_equal(records, d.events.sort_by {|a| a[0] })
      ensure
        Timecop.return
      end
    end

    test 'plain' do
      @d = d = create_driver

      time = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}],
      ]

      d.run(expect_records: records.length, timeout: 5) do
        records.each {|tag, _time, record|
          send_data packer.write([tag, _time, record]).to_s
        }
      end
      assert_equal(records, d.events)
    end

    test 'time_as_integer' do
      @d = d = create_driver

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time_i, {"a"=>1}],
        ["tag2", time_i, {"a"=>2}],
      ]

      d.run(expect_records: records.length, timeout: 5) do
        records.each {|tag, _time, record|
          send_data packer.write([tag, _time, record]).to_s
        }
      end

      assert_equal(records, d.events)
    end

    test 'skip_invalid_event' do
      @d = d = create_driver(CONFIG + "skip_invalid_event true")

      time = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a" => 1}],
        ["tag2", time, {"a" => 2}],
      ]

      d.run(shutdown: false, expect_records: 2, timeout: 10) do
        entries = []
        # These entries are skipped
        entries << ['tag1', true, {'a' => 3}] << ['tag2', time, 'invalid record']
        entries += records.map { |tag, _time, record| [tag, _time, record] }

        entries.each {|tag, _time, record|
          # Without ack, logs are sometimes not saved to logs during test.
          send_data packer.write([tag, _time, record]).to_s #, try_to_receive_response: true
        }
      end

      logs = d.instance.log.logs
      assert_equal 2, logs.count { |line| line =~ /got invalid event and drop it/ }
      assert_equal records[0], d.events[0]
      assert_equal records[1], d.events[1]

      d.instance_shutdown
    end

    test 'json_using_integer_time' do
      @d = d = create_driver

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time_i, {"a"=>1}],
        ["tag2", time_i, {"a"=>2}],
      ]

      d.run(expect_records: records.length, timeout: 20) do
        records.each {|tag, _time, record|
          send_data [tag, _time, record].to_json
        }
      end

      assert_equal(records, d.events.sort_by {|a| a[1] })
    end

    test 'json_with_newline' do
      @d = d = create_driver

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time_i, {"a"=>1}],
        ["tag2", time_i, {"a"=>2}],
      ]

      d.run(expect_records: records.length, timeout: 20) do
        records.each {|tag, _time, record|
          send_data [tag, _time, record].to_json + "\n"
        sleep 1
        }
      end

      assert_equal(records, d.events.sort_by {|a| a[1] })
    end
  end

  sub_test_case 'forward' do
    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'plain' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      d.run(expect_records: records.length, timeout: 20) do
        entries = []
        records.each {|tag, _time, record|
          entries << [_time, record]
        }
        send_data packer.write(["tag1", entries]).to_s, **options
      end
      assert_equal(records, d.events)
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'time_as_integer' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time_i = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time_i, {"a"=>1}],
        ["tag1", time_i, {"a"=>2}]
      ]

      d.run(expect_records: records.length, timeout: 20) do
        entries = []
        records.each {|tag, _time, record|
          entries << [_time, record]
        }
        send_data packer.write(["tag1", entries]).to_s, **options
      end

      assert_equal(records, d.events)
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'skip_invalid_event' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config + "skip_invalid_event true")

      time = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a" => 1}],
        ["tag1", time, {"a" => 2}],
      ]

      d.run(shutdown: false, expect_records: records.length, timeout: 20) do
        entries = records.map { |tag, _time, record| [_time, record] }
        # These entries are skipped
        entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

        send_data packer.write(["tag1", entries]).to_s, **options
      end

      logs = d.instance.log.out.logs
      assert{ logs.select{|line| line =~ /skip invalid event/ }.size == 2 }

      d.instance_shutdown
    end
  end

  sub_test_case 'packed forward' do
    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'plain' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}],
      ]

      d.run(expect_records: records.length, timeout: 20) do
        entries = ''
        records.each {|_tag, _time, record|
          packer(entries).write([_time, record]).flush
        }
        send_data packer.write(["tag1", entries]).to_s, **options
      end
      assert_equal(records, d.events)
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'time_as_integer' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i

      records = [
        ["tag1", time_i, {"a"=>1}],
        ["tag1", time_i, {"a"=>2}],
      ]

      d.run(expect_records: records.length, timeout: 20) do
        entries = ''
        records.each {|tag, _time, record|
          packer(entries).write([_time, record]).flush
        }
        send_data packer.write(["tag1", entries]).to_s, **options
      end
      assert_equal(records, d.events)
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'skip_invalid_event' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config + "skip_invalid_event true")

      time = event_time("2011-01-02 13:14:15 UTC")

      records = [
        ["tag1", time, {"a" => 1}],
        ["tag1", time, {"a" => 2}],
      ]

      d.run(shutdown: false, expect_records: records.length, timeout: 20) do
        entries = records.map { |tag, _time, record| [_time, record] }
        # These entries are skipped
        entries << ['invalid time', {'a' => 3}] << [time, 'invalid record']

        packed_entries = ''
        entries.each { |_time, record|
          packer(packed_entries).write([_time, record]).flush
        }
        send_data packer.write(["tag1", packed_entries]).to_s, **options
      end

      logs = d.instance.log.logs
      assert_equal 2, logs.count { |line| line =~ /skip invalid event/ }

      d.instance_shutdown
    end
  end

  sub_test_case 'compressed packed forward' do
    test 'set_compress_to_option' do
      @d = d = create_driver

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i
      events = [
        ["tag1", time_i, {"a"=>1}],
        ["tag1", time_i, {"a"=>2}]
      ]

      # create compressed entries
      entries = ''
      events.each do |_tag, _time, record|
        v = [_time, record].to_msgpack
        entries << compress(v)
      end
      chunk = ["tag1", entries, { 'compressed' => 'gzip' }].to_msgpack

      d.run do
        Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
          option = d.instance.send(:on_message, obj, chunk.size, DUMMY_SOCK)
          assert_equal 'gzip', option['compressed']
        end
      end

      assert_equal events, d.events
    end

    test 'create_CompressedMessagePackEventStream_with_gzip_compress_option' do
      @d = d = create_driver

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i
      events = [
        ["tag1", time_i, {"a"=>1}],
        ["tag1", time_i, {"a"=>2}]
      ]

      # create compressed entries
      entries = ''
      events.each do |_tag, _time, record|
        v = [_time, record].to_msgpack
        entries << compress(v)
      end
      chunk = ["tag1", entries, { 'compressed' => 'gzip' }].to_msgpack

      # check CompressedMessagePackEventStream is created
      mock(Fluent::CompressedMessagePackEventStream).new(entries, nil, 0)

      d.run do
        Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
          option = d.instance.send(:on_message, obj, chunk.size, DUMMY_SOCK)
          assert_equal 'gzip', option['compressed']
        end
      end
    end
  end

  sub_test_case 'warning' do
    test 'send_large_chunk_warning' do
      @d = d = create_driver(CONFIG + %[
        chunk_size_warn_limit 16M
        chunk_size_limit 32M
      ])

      time = event_time("2014-04-25 13:14:15 UTC")

      # generate over 16M chunk
      str = "X" * 1024 * 1024
      chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack
      assert chunk.size > (16 * 1024 * 1024)
      assert chunk.size < (32 * 1024 * 1024)

      d.run(shutdown: false) do
        Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
          d.instance.send(:on_message, obj, chunk.size, DUMMY_SOCK)
        end
      end

      # check emitted data
      emits = d.events
      assert_equal 16, emits.size
      assert emits.map(&:first).all?{|t| t == "test.tag" }
      assert_equal (0...16).to_a, emits.map{|_tag, t, _record| t - time }

      # check log
      logs = d.instance.log.logs
      assert_equal 1, logs.select{|line|
        line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_warn_limit':/ &&
        line =~ / tag="test.tag" host="#{LOCALHOST_HOSTNAME}" limit=16777216 size=16777501/
      }.size, "large chunk warning is not logged"

      d.instance_shutdown
    end

    test 'send_large_chunk_only_warning' do
      @d = d = create_driver(CONFIG + %[
        chunk_size_warn_limit 16M
      ])
      time = event_time("2014-04-25 13:14:15 UTC")

      # generate over 16M chunk
      str = "X" * 1024 * 1024
      chunk = [ "test.tag", (0...16).map{|i| [time + i, {"data" => str}] } ].to_msgpack

      d.run(shutdown: false) do
        Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
          d.instance.send(:on_message, obj, chunk.size, DUMMY_SOCK)
        end
      end

      # check log
      logs = d.instance.log.logs
      assert_equal 1, logs.select{ |line|
        line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_warn_limit':/ &&
        line =~ / tag="test.tag" host="#{LOCALHOST_HOSTNAME}" limit=16777216 size=16777501/
      }.size, "large chunk warning is not logged"

      d.instance_shutdown
    end

    test 'send_large_chunk_limit' do
      @d = d = create_driver(CONFIG + %[
        chunk_size_warn_limit 16M
        chunk_size_limit 32M
      ])

      time = event_time("2014-04-25 13:14:15 UTC")

      # generate over 32M chunk
      str = "X" * 1024 * 1024
      chunk = [ "test.tag", (0...32).map{|i| [time + i, {"data" => str}] } ].to_msgpack
      assert chunk.size > (32 * 1024 * 1024)

      # d.run => send_data
      d.run(shutdown: false) do
        Fluent::Engine.msgpack_factory.unpacker.feed_each(chunk) do |obj|
          d.instance.send(:on_message, obj, chunk.size, DUMMY_SOCK)
        end
      end

      # check emitted data
      emits = d.events
      assert_equal 0, emits.size

      # check log
      logs = d.instance.log.logs
      assert_equal 1, logs.select{|line|
        line =~ / \[warn\]: Input chunk size is larger than 'chunk_size_limit', dropped:/ &&
        line =~ / tag="test.tag" host="#{LOCALHOST_HOSTNAME}" limit=33554432 size=33554989/
      }.size, "large chunk warning is not logged"

      d.instance_shutdown
    end

    data('string chunk' => 'broken string',
         'integer chunk' => 10)
    test 'send_broken_chunk' do |data|
      @d = d = create_driver

      # d.run => send_data
      d.run(shutdown: false) do
        d.instance.send(:on_message, data, 1000000000, DUMMY_SOCK)
      end

      # check emitted data
      assert_equal 0, d.events.size

      # check log
      logs = d.instance.log.logs
      assert_equal 1, logs.select{|line|
        line =~ / \[warn\]: incoming chunk is broken: host="#{LOCALHOST_HOSTNAME}" msg=#{data.inspect}/
      }.size, "should not accept broken chunk"

      d.instance_shutdown
    end
  end

  sub_test_case 'respond to required ack' do
    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'message' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}]
      ]

      expected_acks = []

      d.run(expect_records: events.size) do
        events.each {|tag, _time, record|
          op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
          expected_acks << op['chunk']
          send_data [tag, _time, record, op].to_msgpack, try_to_receive_response: true, **options
        }
      end

      assert_equal events, d.events
      assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
    end

    # FIX: response is not pushed into @responses because IO.select has been blocked until InputForward shutdowns
    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'forward' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      expected_acks = []

      d.run(expect_records: events.size) do
        entries = []
        events.each {|_tag, _time, record|
          entries << [_time, record]
        }
        op = { 'chunk' => Base64.encode64(entries.object_id.to_s) }
        expected_acks << op['chunk']
        send_data ["tag1", entries, op].to_msgpack, try_to_receive_response: true, **options
      end

      assert_equal events, d.events
      assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'packed_forward' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      expected_acks = []

      d.run(expect_records: events.size) do
        entries = ''
        events.each {|_tag, _time,record|
          [time, record].to_msgpack(entries)
        }
        op = { 'chunk' => Base64.encode64(entries.object_id.to_s) }
        expected_acks << op['chunk']
        send_data ["tag1", entries, op].to_msgpack, try_to_receive_response: true, **options
      end

      assert_equal events, d.events
      assert_equal expected_acks, @responses.map { |res| MessagePack.unpack(res)['ack'] }
    end

    data(
      tcp: {
        config: CONFIG,
        options: {
          auth: false
        }
      },
      ### Auth is not supported with json
      # auth: {
      #   config: CONFIG_AUTH,
      #   options: {
      #     auth: true
      #   }
      # },
    )
    test 'message_json' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time_i = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time_i, {"a"=>1}],
        ["tag2", time_i, {"a"=>2}]
      ]

      expected_acks = []

      d.run(expect_records: events.size, timeout: 20) do
        events.each {|tag, _time, record|
          op = { 'chunk' => Base64.encode64(record.object_id.to_s) }
          expected_acks << op['chunk']
          send_data [tag, _time, record, op].to_json, try_to_receive_response: true, **options
        }
      end

      assert_equal events, d.events
      assert_equal expected_acks, @responses.map { |res| JSON.parse(res)['ack'] }
    end
  end

  sub_test_case 'not respond without required ack' do
    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'message' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time, {"a"=>1}],
        ["tag2", time, {"a"=>2}]
      ]

      d.run(expect_records: events.size, timeout: 20) do
        events.each {|tag, _time, record|
          send_data [tag, _time, record].to_msgpack, try_to_receive_response: true, **options
        }
      end

      assert_equal events, d.events
      assert_equal [nil, nil], @responses
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'forward' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      d.run(expect_records: events.size, timeout: 20) do
        entries = []
        events.each {|tag, _time, record|
          entries << [_time, record]
        }
        send_data ["tag1", entries].to_msgpack, try_to_receive_response: true, **options
      end

      assert_equal events, d.events
      assert_equal [nil], @responses
    end

    data(tcp: {
           config: CONFIG,
           options: {
             auth: false
           }
         },
         auth: {
           config: CONFIG_AUTH,
           options: {
             auth: true
           }
         })
    test 'packed_forward' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time = event_time("2011-01-02 13:14:15 UTC")

      events = [
        ["tag1", time, {"a"=>1}],
        ["tag1", time, {"a"=>2}]
      ]

      d.run(expect_records: events.size, timeout: 20) do
        entries = ''
        events.each {|tag, _time, record|
          [_time, record].to_msgpack(entries)
        }
        send_data ["tag1", entries].to_msgpack, try_to_receive_response: true, **options
      end

      assert_equal events, d.events
      assert_equal [nil], @responses
    end

    data(
      tcp: {
        config: CONFIG,
        options: {
          auth: false
        }
      },
      ### Auth is not supported with json
      # auth: {
      #   config: CONFIG_AUTH,
      #   options: {
      #     auth: true
      #   }
      # },
    )
    test 'message_json' do |data|
      config = data[:config]
      options = data[:options]
      @d = d = create_driver(config)

      time_i = event_time("2011-01-02 13:14:15 UTC").to_i

      events = [
        ["tag1", time_i, {"a"=>1}],
        ["tag2", time_i, {"a"=>2}]
      ]

      d.run(expect_records: events.size, timeout: 20) do
        events.each {|tag, _time, record|
          send_data [tag, _time, record].to_json, try_to_receive_response: true, **options
        }
      end

      assert_equal events, d.events
      assert_equal [nil, nil], @responses
    end
  end

  def packer(*args)
    Fluent::Engine.msgpack_factory.packer(*args)
  end

  def unpacker
    Fluent::Engine.msgpack_factory.unpacker
  end

  # res
  # '' : socket is disconnected without any data
  # nil: socket read timeout
  def read_data(io, timeout, &block)
    res = ''
    select_timeout = 2
    clock_id = Process::CLOCK_MONOTONIC_RAW rescue Process::CLOCK_MONOTONIC
    timeout_at = Process.clock_gettime(clock_id) + timeout
    begin
      buf = ''
      io_activated = false
      while Process.clock_gettime(clock_id) < timeout_at
        if IO.select([io], nil, nil, select_timeout)
          io_activated = true
          buf = io.readpartial(2048)
          res ||= ''
          res << buf

          break if block.call(res)
        end
      end
      res = nil unless io_activated # timeout without no data arrival
    rescue Errno::EAGAIN
      sleep 0.01
      retry if res == ''
      # if res is not empty, all data in socket buffer are read, so do not retry
    rescue IOError, EOFError, Errno::ECONNRESET
      # socket disconnected
    end
    res
  end

  def simulate_auth_sequence(io, shared_key=SHARED_KEY, username=USER_NAME, password=USER_PASSWORD)
    auth_response_timeout = 30
    shared_key_salt = 'salt'

    # reading helo
    helo_data = read_data(io, auth_response_timeout){|data| MessagePack.unpack(data) rescue nil }
    raise "Authentication packet timeout" unless helo_data
    raise "Authentication connection closed" if helo_data == ''
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
      Digest::SHA512.new
        .update(shared_key_salt)
        .update('selfhostname')
        .update(@options['nonce'])
        .update(shared_key).hexdigest,
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
    pong_data = read_data(io, auth_response_timeout){|data| MessagePack.unpack(data) rescue nil }
    raise "PONG packet timeout" unless pong_data
    raise "PONG connection closed" if pong_data == ''
    # ['PING', bool(auth_result), string(reason_if_failed), self_hostname, shared_key_digest]
    pong = MessagePack.unpack(pong_data)
    raise "Invalid PONG header" unless pong[0] == 'PONG'
    raise "Authentication Failure: #{pong[2]}" unless pong[1]
    clientside_calculated = Digest::SHA512.new
      .update(shared_key_salt)
      .update(pong[3])
      .update(@options['nonce'])
      .update(shared_key).hexdigest
    raise "Shared key digest mismatch" unless clientside_calculated == pong[4]

    # authentication success
    true
  end

  def connect
    TCPSocket.new('127.0.0.1', PORT)
  end

  # Data ordering is not assured:
  #  Records in different sockets are processed on different thread, so its scheduling make effect
  #  on order of emitted records.
  #  So, we MUST sort emitted records in different `send_data` before assertion.
  def send_data(data, try_to_receive_response: false, response_timeout: 5, auth: false)
    io = connect

    if auth
      simulate_auth_sequence(io)
    end

    io.write data
    io.flush
    if try_to_receive_response
      @responses << read_data(io, response_timeout){|d| MessagePack.unpack(d) rescue nil }
    end
  ensure
    io.close rescue nil # SSL socket requires any writes to close sockets
  end

  sub_test_case 'source_hostname_key and source_address_key features' do
    data(
      both: [:hostname, :address],
      hostname: [:hostname],
      address: [:address],
    )
    test 'message protocol' do |keys|
      execute_test_with_source_hostname_key(*keys) { |events|
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
      execute_test_with_source_hostname_key(*keys) { |events|
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
      execute_test_with_source_hostname_key(*keys) { |events|
        entries = ''
        events.each { |tag, time, record|
          Fluent::Engine.msgpack_factory.packer(entries).write([time, record]).flush
        }
        send_data Fluent::Engine.msgpack_factory.packer.write(["tag1", entries]).to_s
      }
    end
  end

  def execute_test_with_source_hostname_key(*keys, &block)
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
    @d = d = create_driver(conf)

    time = event_time("2011-01-02 13:14:15 UTC")
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]

    d.run(expect_records: events.size) do
      block.call(events)
    end

    d.events.each { |tag, _time, record|
      if keys.include?(:hostname)
        assert_true record.has_key?('source_hostname')
        assert_equal DUMMY_SOCK.remote_host, record['source_hostname']
      end
      if keys.include?(:address)
        assert_true record.has_key?('source_address')
        assert_equal DUMMY_SOCK.remote_addr, record['source_address']
      end
    }
  end

  # TODO heartbeat
end
