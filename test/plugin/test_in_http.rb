require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_http'
require 'net/http'
require 'timecop'

class HttpInputTest < Test::Unit::TestCase
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
  end

  def teardown
    Timecop.return
  end

  PORT = unused_port
  CONFIG = %[
    port #{PORT}
    bind "127.0.0.1"
    body_size_limit 10m
    keepalive_timeout 5
    respond_with_empty_img true
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::HttpInput).configure(conf)
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
    time = event_time("2011-01-02 13:14:15.123 UTC")
    Timecop.freeze(Time.at(time))

    events = [
      ["tag1", time, {"a" => 1}],
      ["tag2", time, {"a" => 2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, _time, record|
        res = post("/#{tag}", {"json"=>record.to_json})
        res_codes << res.code
      end
    end

    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_time_as_float
    d = create_driver
    time = event_time("2011-01-02 13:14:15.123 UTC")
    float_time = time.to_f

    events = [
      ["tag1", time, {"a"=>1}],
    ]
    res_codes = []

    d.run(expect_records: 1) do
      events.each do |tag, t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>float_time.to_s})
        res_codes << res.code
      end
    end
    assert_equal ["200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
  end

  def test_json
    d = create_driver
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    events = [
      ["tag1", time_i, {"a"=>1}],
      ["tag2", time_i, {"a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>t.to_s})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_multi_json
    d = create_driver
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    records = [{"a"=>1},{"a"=>2}]
    events = [
      ["tag1", time_i, records[0]],
      ["tag1", time_i, records[1]],
    ]
    tag = "tag1"
    res_codes = []
    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", {"json"=>records.to_json, "time"=>time_i.to_s})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_multi_json_with_time_field
    d = create_driver
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    time_f = time.to_f

    records = [{"a" => 1, 'time' => time_i},{"a" => 2, 'time' => time_f}]
    events = [
      ["tag1", time, {'a' => 1}],
      ["tag1", time, {'a' => 2}],
    ]
    tag = "tag1"
    res_codes = []
    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", {"json" => records.to_json})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes
    assert_equal events, d.events
    assert_instance_of Fluent::EventTime, d.events[0][1]
    assert_instance_of Fluent::EventTime, d.events[1][1]
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_multi_json_with_nonexistent_time_key
    d = create_driver(CONFIG + %[
      <parse>
        time_key missing
      </parse>
    ])
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    time_f = time.to_f

    records = [{"a" => 1, 'time' => time_i},{"a" => 2, 'time' => time_f}]
    tag = "tag1"
    res_codes = []
    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", {"json" => records.to_json})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes
    assert_equal 2, d.events.size
    assert_not_equal time_i, d.events[0][1].sec # current time is used because "missing" field doesn't exist
    assert_not_equal time_i, d.events[1][1].sec
  end

  def test_json_with_add_remote_addr
    d = create_driver(CONFIG + "add_remote_addr true")
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    events = [
      ["tag1", time, {"REMOTE_ADDR"=>"127.0.0.1", "a"=>1}],
      ["tag2", time, {"REMOTE_ADDR"=>"127.0.0.1", "a"=>2}],
    ]
    res_codes = []
    d.run(expect_records: 2) do
      events.each do |tag, _t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time_i.to_s})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_multi_json_with_add_remote_addr
    d = create_driver(CONFIG + "add_remote_addr true")
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    records = [{"a"=>1},{"a"=>2}]
    tag = "tag1"
    res_codes = []

    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", {"json"=>records.to_json, "time"=>time_i.to_s})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes

    assert_equal "tag1", d.events[0][0]
    assert_equal_event_time time, d.events[0][1]
    assert_equal 1, d.events[0][2]["a"]
    assert{ d.events[0][2].has_key?("REMOTE_ADDR") && d.events[0][2]["REMOTE_ADDR"] =~ /^\d{1,4}(\.\d{1,4}){3}$/ }

    assert_equal "tag1", d.events[1][0]
    assert_equal_event_time time, d.events[1][1]
    assert_equal 2, d.events[1][2]["a"]
  end

  def test_json_with_add_remote_addr_given_x_forwarded_for
    d = create_driver(CONFIG + "add_remote_addr true")
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, _t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time_i.to_s}, {"X-Forwarded-For"=>"129.78.138.66, 127.0.0.1"})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes

    assert_equal "tag1", d.events[0][0]
    assert_equal_event_time time, d.events[0][1]
    assert_equal({"REMOTE_ADDR"=>"129.78.138.66", "a"=>1}, d.events[0][2])

    assert_equal "tag2", d.events[1][0]
    assert_equal_event_time time, d.events[1][1]
    assert_equal({"REMOTE_ADDR"=>"129.78.138.66", "a"=>2}, d.events[1][2])
  end

  def test_multi_json_with_add_remote_addr_given_x_forwarded_for
    d = create_driver(CONFIG + "add_remote_addr true")

    tag = "tag1"
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    records = [{"a"=>1},{"a"=>2}]
    events = [
      [tag, time, {"REMOTE_ADDR"=>"129.78.138.66", "a"=>1}],
      [tag, time, {"REMOTE_ADDR"=>"129.78.138.66", "a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", {"json"=>records.to_json, "time"=>time_i.to_s}, {"X-Forwarded-For"=>"129.78.138.66, 127.0.0.1"})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_add_remote_addr_given_multi_x_forwarded_for
    d = create_driver(CONFIG + "add_remote_addr true")

    tag = "tag1"
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    record = {"a" => 1}
    event = ["tag1", time, {"REMOTE_ADDR" => "129.78.138.66", "a" => 1}]
    res_code = nil

    d.run(expect_records: 1, timeout: 5) do
      res = post("/#{tag}", {"json" => record.to_json, "time" => time_i.to_s}) { |http, req|
        # net/http can't send multiple headers so overwrite it.
        def req.each_capitalized
          block_given? or return enum_for(__method__) { @header.size }
          @header.each do |k, vs|
            vs.each { |v|
              yield capitalize(k), v
            }
          end
        end
        req.add_field("X-Forwarded-For", "129.78.138.66, 127.0.0.1")
        req.add_field("X-Forwarded-For", "8.8.8.8")
      }
      res_code = res.code
    end
    assert_equal "200", res_code
    assert_equal event, d.events.first
    assert_equal_event_time time, d.events.first[1]
  end

  def test_multi_json_with_add_http_headers
    d = create_driver(CONFIG + "add_http_headers true")
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    records = [{"a"=>1},{"a"=>2}]
    tag = "tag1"
    res_codes = []

    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", {"json"=>records.to_json, "time"=>time_i.to_s})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes

    assert_equal "tag1", d.events[0][0]
    assert_equal_event_time time, d.events[0][1]
    assert_equal 1, d.events[0][2]["a"]

    assert_equal "tag1", d.events[1][0]
    assert_equal_event_time time, d.events[1][1]
    assert_equal 2, d.events[1][2]["a"]

    assert include_http_header?(d.events[0][2])
    assert include_http_header?(d.events[1][2])
  end

  def test_json_with_add_http_headers
    d = create_driver(CONFIG + "add_http_headers true")
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time_i.to_s})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes

    assert_equal "tag1", d.events[0][0]
    assert_equal_event_time time, d.events[0][1]
    assert_equal 1, d.events[0][2]["a"]

    assert_equal "tag2", d.events[1][0]
    assert_equal_event_time time, d.events[1][1]
    assert_equal 2, d.events[1][2]["a"]

    assert include_http_header?(d.events[0][2])
    assert include_http_header?(d.events[1][2])
  end

  def test_multi_json_with_custom_parser
    d = create_driver(CONFIG + %[
        <parse>
          @type json
          keep_time_key true
          time_key foo
          time_format %iso8601
        </parse>
    ])

    time = event_time("2011-01-02 13:14:15 UTC")
    time_s = Time.at(time).iso8601

    records = [{"foo"=>time_s,"bar"=>"test1"},{"foo"=>time_s,"bar"=>"test2"}]
    tag = "tag1"
    res_codes = []

    d.run(expect_records: 2, timeout: 5) do
      res = post("/#{tag}", records.to_json, {"Content-Type"=>"application/octet-stream"})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes

    assert_equal "tag1", d.events[0][0]
    assert_equal_event_time time, d.events[0][1]
    assert_equal d.events[0][2], records[0]

    assert_equal "tag1", d.events[1][0]
    assert_equal_event_time time, d.events[1][1]
    assert_equal d.events[1][2], records[1]
  end

  def test_application_json
    d = create_driver
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        res = post("/#{tag}?time=#{time_i.to_s}", record.to_json, {"Content-Type"=>"application/json; charset=utf-8"})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_application_msgpack
    d = create_driver
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        res = post("/#{tag}?time=#{time_i.to_s}", record.to_msgpack, {"Content-Type"=>"application/msgpack"})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end
  
  def test_msgpack
    d = create_driver
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        res = post("/#{tag}", {"msgpack"=>record.to_msgpack, "time"=>time_i.to_s})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_multi_msgpack
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i

    records = [{"a"=>1},{"a"=>2}]
    events = [
      ["tag1", time, records[0]],
      ["tag1", time, records[1]],
    ]
    tag = "tag1"
    res_codes = []
    d.run(expect_records: 2) do
      res = post("/#{tag}", {"msgpack"=>records.to_msgpack, "time"=>time_i.to_s})
      res_codes << res.code
    end
    assert_equal ["200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_with_regexp
    d = create_driver(CONFIG + %[
      format /^(?<field_1>\\d+):(?<field_2>\\w+)$/
      types field_1:integer
    ])

    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"field_1" => 1, "field_2" => 'str'}],
      ["tag2", time, {"field_1" => 2, "field_2" => 'str'}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        body = record.map { |k, v|
          v.to_s
        }.join(':')
        res = post("/#{tag}?time=#{time_i.to_s}", body, {'Content-Type' => 'application/octet-stream'})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_with_csv
    require 'csv'

    d = create_driver(CONFIG + %[
      format csv
      keys foo,bar
    ])
    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"foo" => "1", "bar" => 'st"r'}],
      ["tag2", time, {"foo" => "2", "bar" => 'str'}],
    ]
    res_codes = []

    d.run(expect_records: 2) do
      events.each do |tag, t, record|
        body = record.map { |k, v| v }.to_csv
        res = post("/#{tag}?time=#{time_i.to_s}", body, {'Content-Type' => 'text/comma-separated-values'})
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_resonse_with_empty_img
    d = create_driver(CONFIG + "respond_with_empty_img true")
    assert_equal true, d.instance.respond_with_empty_img

    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []
    res_bodies = []

    d.run do
      events.each do |tag, _t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time_i.to_s})
        res_codes << res.code
        # Ruby returns ASCII-8 encoded string for GIF.
        res_bodies << res.body.force_encoding("UTF-8")
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal [Fluent::Plugin::HttpInput::EMPTY_GIF_IMAGE, Fluent::Plugin::HttpInput::EMPTY_GIF_IMAGE], res_bodies
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_cors_allowed
    d = create_driver(CONFIG + "cors_allow_origins [\"http://foo.com\"]")
    assert_equal ["http://foo.com"], d.instance.cors_allow_origins

    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []
    res_headers = []

    d.run do
      events.each do |tag, _t, record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time_i.to_s}, {"Origin"=>"http://foo.com"})
        res_codes << res.code
        res_headers << res["Access-Control-Allow-Origin"]
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal ["http://foo.com", "http://foo.com"], res_headers
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_cors_allowed_wildcard
    d = create_driver(CONFIG + 'cors_allow_origins ["*"]')

    time = event_time("2011-01-02 13:14:15 UTC")
    events = [
      ["tag1", time, {"a"=>1}],
    ]

    d.run do
      events.each do |tag, time, record|
        headers = {"Origin" => "http://foo.com"}

        res = post("/#{tag}", {"json" => record.to_json, "time" => time.to_i}, headers)

        assert_equal "200", res.code
        assert_equal "*", res["Access-Control-Allow-Origin"]
      end
    end
  end

  def test_cors_preflight
    d = create_driver(CONFIG + 'cors_allow_origins ["*"]')

    d.run do
      header = {
        "Origin" => "http://foo.com",
        "Access-Control-Request-Method" => "POST",
        "Access-Control-Request-Headers" => "Content-Type",
      }
      res = options("/cors.test", {}, header)

      assert_equal "200", res.code
      assert_equal "*", res["Access-Control-Allow-Origin"]
      assert_equal "POST", res["Access-Control-Allow-Methods"]
    end
  end

  def test_content_encoding_gzip
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []
    res_headers = []

    d.run do
      events.each do |tag, time, record|
        header = {'Content-Type'=>'application/json', 'Content-Encoding'=>'gzip'}
        res = post("/#{tag}?time=#{time}", compress_gzip(record.to_json), header)
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_content_encoding_deflate
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    events = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]
    res_codes = []
    res_headers = []

    d.run do
      events.each do |tag, time, record|
        header = {'Content-Type'=>'application/msgpack', 'Content-Encoding'=>'deflate'}
        res = post("/#{tag}?time=#{time}", Zlib.deflate(record.to_msgpack), header)
        res_codes << res.code
      end
    end
    assert_equal ["200", "200"], res_codes
    assert_equal events, d.events
    assert_equal_event_time time, d.events[0][1]
    assert_equal_event_time time, d.events[1][1]
  end

  def test_cors_disallowed
    d = create_driver(CONFIG + "cors_allow_origins [\"http://foo.com\"]")
    assert_equal ["http://foo.com"], d.instance.cors_allow_origins

    time = event_time("2011-01-02 13:14:15 UTC")
    time_i = time.to_i
    res_codes = []

    d.end_if{ res_codes.size == 2 }
    d.run do
      res = post("/tag1", {"json"=>{"a"=>1}.to_json, "time"=>time_i.to_s}, {"Origin"=>"http://bar.com"})
      res_codes << res.code
      res = post("/tag2", {"json"=>{"a"=>1}.to_json, "time"=>time_i.to_s}, {"Origin"=>"http://bar.com"})
      res_codes << res.code
    end
    assert_equal ["403", "403"], res_codes
  end

  $test_in_http_connection_object_ids = []
  $test_in_http_content_types = []
  $test_in_http_content_types_flag = false
  module ContentTypeHook
    def initialize(*args)
      @io_handler = nil
      super
    end
    def on_headers_complete(headers)
      super
      if $test_in_http_content_types_flag
        $test_in_http_content_types << self.content_type
      end
    end

    def on_message_begin
      super
      if $test_in_http_content_types_flag
        $test_in_http_connection_object_ids << @io_handler.object_id
      end
    end
  end

  class Fluent::Plugin::HttpInput::Handler
    prepend ContentTypeHook
  end

  def test_if_content_type_is_initialized_properly
    # This test is to check if Fluent::HttpInput::Handler's @content_type is initialized properly.
    # Especially when in Keep-Alive and the second request has no 'Content-Type'.

    begin
      d = create_driver

      $test_in_http_content_types_flag = true
      d.run do
        # Send two requests the second one has no Content-Type in Keep-Alive
        Net::HTTP.start("127.0.0.1", PORT) do |http|
          req = Net::HTTP::Post.new("/foodb/bartbl", {"connection" => "keepalive", "Content-Type" => "application/json"})
          res = http.request(req)

          req = Net::HTTP::Get.new("/foodb/bartbl", {"connection" => "keepalive"})
          res = http.request(req)
        end

      end
    ensure
      $test_in_http_content_types_flag = false
    end
    assert_equal(['application/json', ''], $test_in_http_content_types)
    # Asserting keepalive
    assert_equal $test_in_http_connection_object_ids[0], $test_in_http_connection_object_ids[1]
  end

  def options(path, params, header = {})
    http = Net::HTTP.new("127.0.0.1", PORT)
    req = Net::HTTP::Options.new(path, header)
    http.request(req)
  end

  def post(path, params, header = {}, &block)
    http = Net::HTTP.new("127.0.0.1", PORT)
    req = Net::HTTP::Post.new(path, header)
    block.call(http, req) if block
    if params.is_a?(String)
      unless header.has_key?('Content-Type')
        header['Content-Type'] = 'application/octet-stream'
      end
      req.body = params
    else
      unless header.has_key?('Content-Type')
        header['Content-Type'] = 'application/x-www-form-urlencoded'
      end
      req.set_form_data(params)
    end
    http.request(req)
  end

  def compress_gzip(data)
    io = StringIO.new
    io.binmode
    Zlib::GzipWriter.wrap(io) { |gz| gz.write data }
    return io.string
  end

  def include_http_header?(record)
    record.keys.find { |header| header.start_with?('HTTP_') }
  end
end
