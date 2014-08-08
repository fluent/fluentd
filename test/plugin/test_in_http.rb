require 'fluent/test'
require 'helper'
require 'net/http'

class HttpInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind "127.0.0.1"
    body_size_limit 10m
    keepalive_timeout 5
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::HttpInput).configure(conf, true)
  end

  def test_configure
    d = create_driver
    assert_equal PORT, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
    assert_equal 10*1024*1024, d.instance.body_size_limit
    assert_equal 5, d.instance.keepalive_timeout
    assert_equal false, d.instance.add_http_headers
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
        res = post("/#{tag}", {"json"=>record.to_json})
        assert_equal "200", res.code
      }
    end

    compare_test_result(tests, d.emits)
  end

  def test_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each {|tag,time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time.to_s})
        assert_equal "200", res.code
      }
    end

    compare_test_result(tests, d.emits)

    d.emit_streams.each { |tag, es|
      assert !include_http_header?(es.first[1])
    }
  end

  def test_multi_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    tests = events.map { |ev| [tag, time, ev] }
    d.expected_emits_length = tests.length

    d.run do
      res = post("/#{tag}", {"json"=>events.to_json, "time"=>time.to_s})
      assert_equal "200", res.code
    end

    compare_test_result(tests, d.emits)
  end

  def test_json_with_add_http_headers
    d = create_driver(CONFIG + "add_http_headers true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [["tag1", time, {"a"=>1}], ["tag2", time, {"a"=>2}]]

    d.run do
      records.each {|tag,time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time.to_s})
        assert_equal "200", res.code

      }
    end

    d.emit_streams.each { |tag, es|
      assert include_http_header?(es.first[1])
    }
  end

  def test_application_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each {|tag,time,record|
        res = post("/#{tag}?time=#{time.to_s}", record.to_json, {"content-type"=>"application/json; charset=utf-8"})
        assert_equal "200", res.code
      }
    end

    compare_test_result(tests, d.emits)
  end

  def test_msgpack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each {|tag,time,record|
        res = post("/#{tag}", {"msgpack"=>record.to_msgpack, "time"=>time.to_s})
        assert_equal "200", res.code
      }
    end

    compare_test_result(tests, d.emits)
  end

  def test_multi_msgpack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    tests = events.map { |ev| [tag, time, ev] }
    d.expected_emits_length = tests.length

    d.run do
      res = post("/#{tag}", {"msgpack"=>events.to_msgpack, "time"=>time.to_s})
      assert_equal "200", res.code
    end

    compare_test_result(tests, d.emits)
  end

  def test_with_regexp
    d = create_driver(CONFIG + %[
      format /^(?<field_1>\\\\d+):(?<field_2>\\\\w+)$/
      types field_1:integer
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"field_1" => 1, "field_2" => 'str'}],
      ["tag2", time, {"field_1" => 2, "field_2" => 'str'}]
    ]

    d.run do
      tests.each { |tag, time, record|
        body = record.map { |k, v|
          v.to_s
        }.join(':')
        res = post("/#{tag}?time=#{time.to_s}", body)
        assert_equal "200", res.code
      }
    end

    compare_test_result(tests, d.emits)
  end

  def test_with_csv
    require 'csv'

    d = create_driver(CONFIG + %[
      format csv
      keys foo,bar
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    tests = [
      ["tag1", time, {"foo" => "1", "bar" => 'st"r'}],
      ["tag2", time, {"foo" => "2", "bar" => 'str'}]
    ]
    d.expected_emits_length = tests.length

    d.run do
      tests.each { |tag, time, record|
        body = record.map { |k, v| v }.to_csv
        res = post("/#{tag}?time=#{time.to_s}", body)
        assert_equal "200", res.code
      }
    end

    compare_test_result(tests, d.emits)
  end

  def post(path, params, header = {})
    http = Net::HTTP.new("127.0.0.1", PORT)
    req = Net::HTTP::Post.new(path, header)
    if params.is_a?(String)
      req.body = params
    else
      req.set_form_data(params)
    end
    http.request(req)
  end

  def include_http_header?(record)
    record.keys.find { |header| header.start_with?('HTTP_') }
  end

  def compare_test_result(tests, emits)
    assert_equal(tests.length, emits.length)
    emits.each_index do |i|
      assert_equal(tests[i], emits[i])
    end
  end
end
