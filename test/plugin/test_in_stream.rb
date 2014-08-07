require 'fluent/test'
require 'helper'

module StreamInputTest
  def setup
    Fluent::Test.setup
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each {|tag,time,record|
        send_data [tag, 0, record].to_msgpack
      }
    end

    compare_test_result(tests, d.emits)
  end

  def test_message
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each {|tag,time,record|
        send_data [tag, time, record].to_msgpack
      }
    end

    compare_test_result(tests, d.emits)
  end

  def test_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      entries = []
      tests.each {|tag,time,record|
        entries << [time, record]
      }
      send_data ["tag1", entries].to_msgpack
    end

    compare_test_result(tests, d.emits)
  end

  def test_packed_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      entries = ''
      tests.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data ["tag1", entries].to_msgpack
    end

    compare_test_result(tests, d.emits)
  end

  def test_message_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each {|tag,time,record|
        send_data [tag, time, record].to_json
      }
    end

    compare_test_result(tests, d.emits)
  end

  def create_driver(klass, conf)
    Fluent::Test::InputTestDriver.new(klass).configure(conf)
  end

  def send_data(data)
    io = connect
    begin
      io.write data
    ensure
      io.close
    end
  end

  def compare_test_result(tests, emits)
    assert_equal(tests.length, emits.length)
    emits.each_index do |i|
      assert_equal(tests[i], emits[i])
    end
  end
end

class UnixInputTest < Test::Unit::TestCase
  include StreamInputTest

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/in_unix#{ENV['TEST_ENV_NUMBER']}"
  CONFIG = %[
    path #{TMP_DIR}/unix
    backlog 1000
  ]

  def create_driver(conf=CONFIG)
    super(Fluent::UnixInput, conf)
  end

  def test_configure
    d = create_driver
    assert_equal "#{TMP_DIR}/unix", d.instance.path
    assert_equal 1000, d.instance.backlog
  end

  def connect
    UNIXSocket.new("#{TMP_DIR}/unix")
  end
end
