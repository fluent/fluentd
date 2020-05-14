require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_unix'

class UnixInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @d = nil
  end

  def teardown
    @d.instance_shutdown if @d
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/in_unix#{ENV['TEST_ENV_NUMBER']}"
  CONFIG = %[
    path #{TMP_DIR}/unix
    backlog 1000
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::UnixInput).configure(conf)
  end

  def packer(*args)
    Fluent::MessagePackFactory.msgpack_packer(*args)
  end

  def unpacker
    Fluent::MessagePackFactory.msgpack_unpacker
  end

  def send_data(data)
    io = UNIXSocket.new("#{TMP_DIR}/unix")
    begin
      io.write data
    ensure
      io.close
    end
  end

  def test_configure
    @d = create_driver
    assert_equal "#{TMP_DIR}/unix", @d.instance.path
    assert_equal 1000, @d.instance.backlog
  end

  def test_time
    @d = create_driver

    time = Fluent::EventTime.now
    records = [
      ["tag1", 0, {"a" => 1}],
      ["tag2", nil, {"a" => 2}],
    ]

    @d.run(expect_records: records.length, timeout: 5) do
      records.each {|tag, _time, record|
        send_data packer.write([tag, _time, record]).to_s
      }
    end

    @d.events.each_with_index { |e, i|
      orig = records[i]
      assert_equal(orig[0], e[0])
      assert_true(time <= e[1])
      assert_equal(orig[2], e[2])
    }
  end

  def test_message
    @d = create_driver

    time = Fluent::EventTime.now
    records = [
      ["tag1", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]

    @d.run(expect_records: records.length, timeout: 5) do
      records.each {|tag, _time, record|
        send_data packer.write([tag, _time, record]).to_s
      }
    end

    assert_equal(records, @d.events)
  end

  def test_forward
    @d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")
    records = [
      ["tag1", time, {"a" => 1}],
      ["tag1", time, {"a" => 2}]
    ]

    @d.run(expect_records: records.length, timeout: 20) do
      entries = []
      records.each {|tag, _time, record|
        entries << [_time, record]
      }
      send_data packer.write(["tag1", entries]).to_s
    end
    assert_equal(records, @d.events)
  end

  def test_packed_forward
    @d = create_driver

    time = Fluent::EventTime.now
    records = [
      ["tag1", time, {"a" => 1}],
      ["tag1", time, {"a" => 2}],
    ]

    @d.run(expect_records: records.length, timeout: 20) do
      entries = ''
      records.each {|_tag, _time, record|
        packer(entries).write([_time, record]).flush
      }
      send_data packer.write(["tag1", entries]).to_s
    end
    assert_equal(records, @d.events)
  end

  def test_message_json
    @d = create_driver

    time = Fluent::EventTime.now
    records = [
      ["tag1", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]

    @d.run(expect_records: records.length, timeout: 5) do
      tag, _time, record = records[0]
      send_data [tag, _time.to_i, record].to_json
      tag, _time, record = records[1]
      send_data [tag, _time.to_f, record].to_json
    end

    assert_equal(records, @d.events)
  end

  def test_message_with_tag
    @d = create_driver(CONFIG + "tag new_tag")

    time = Fluent::EventTime.now
    records = [
      ["tag1", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]

    @d.run(expect_records: records.length, timeout: 5) do
      records.each {|tag, _time, record|
        send_data packer.write([tag, _time, record]).to_s
      }
    end

    @d.events.each { |event|
      assert_equal("new_tag", event[0])
    }
  end

  data('string chunk' => 'broken string',
       'integer chunk' => 10)
  def test_broken_message(data)
    @d = create_driver
    @d.run(shutdown: false, timeout: 5) do
      @d.instance.__send__(:on_message, data)
    end

    assert_equal 0, @d.events.size

    logs = @d.instance.log.logs
    assert_equal 1, logs.select { |line|
      line =~ / \[warn\]: incoming data is broken: msg=#{data.inspect}/
    }.size, "should not accept broken chunk"
  end
end unless Fluent.windows?
