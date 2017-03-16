require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_http'
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
    respond_with_empty_img true
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

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}", {"json"=>record.to_json})
        assert_equal "200", res.code
      }
    end
  end

  def test_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time.to_s})
        assert_equal "200", res.code
      }
    end

    d.emit_streams.each { |tag, es|
      assert !include_http_header?(es.first[1])
    }
  end

  def test_multi_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    events.each { |ev|
      d.expect_emit tag, time, ev
    }

    d.run do
      res = post("/#{tag}", {"json"=>events.to_json, "time"=>time.to_s})
      assert_equal "200", res.code
    end

  end

  def test_json_with_add_remote_addr
    d = create_driver(CONFIG + "add_remote_addr true")
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"REMOTE_ADDR"=>"127.0.0.1", "a"=>1}
    d.expect_emit "tag2", time, {"REMOTE_ADDR"=>"127.0.0.1", "a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time.to_s})
        assert_equal "200", res.code
      }
    end

  end

  def test_multi_json_with_add_remote_addr
    d = create_driver(CONFIG + "add_remote_addr true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    d.expect_emit "tag1", time, {"REMOTE_ADDR"=>"127.0.0.1", "a"=>1}
    d.expect_emit "tag1", time, {"REMOTE_ADDR"=>"127.0.0.1", "a"=>2}

    d.run do
      res = post("/#{tag}", {"json"=>events.to_json, "time"=>time.to_s})
      assert_equal "200", res.code
    end

  end

  def test_json_with_add_remote_addr_given_x_forwarded_for
    d = create_driver(CONFIG + "add_remote_addr true")
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"REMOTE_ADDR"=>"129.78.138.66", "a"=>1}
    d.expect_emit "tag2", time, {"REMOTE_ADDR"=>"129.78.138.66", "a"=>1}

    d.run do
      d.expected_emits.each {|tag,_time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>_time.to_s}, {"X-Forwarded-For"=>"129.78.138.66, 127.0.0.1"})
        assert_equal "200", res.code
      }
    end

  end

  def test_multi_json_with_add_remote_addr_given_x_forwarded_for
    d = create_driver(CONFIG + "add_remote_addr true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    d.expect_emit "tag1", time, {"REMOTE_ADDR"=>"129.78.138.66", "a"=>1}
    d.expect_emit "tag1", time, {"REMOTE_ADDR"=>"129.78.138.66", "a"=>2}

    d.run do
      res = post("/#{tag}", {"json"=>events.to_json, "time"=>time.to_s}, {"X-Forwarded-For"=>"129.78.138.66, 127.0.0.1"})
      assert_equal "200", res.code
    end

  end

  def test_multi_json_with_add_http_headers
    d = create_driver(CONFIG + "add_http_headers true")

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    d.run do
      res = post("/#{tag}", {"json"=>events.to_json, "time"=>time.to_s})
      assert_equal "200", res.code
    end

    d.emit_streams.each { |tag, es|
      assert include_http_header?(es.first[1])
    }
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

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}?time=#{time.to_s}", record.to_json, {"content-type"=>"application/json; charset=utf-8"})
        assert_equal "200", res.code
      }
    end
  end

  def test_application_msgpack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}?time=#{time.to_s}", record.to_msgpack, {"content-type"=>"application/msgpack; charset=utf-8"})
        assert_equal "200", res.code
      }
    end
  end
  
  def test_msgpack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}", {"msgpack"=>record.to_msgpack, "time"=>time.to_s})
        assert_equal "200", res.code
      }
    end
  end

  def test_multi_msgpack
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    events = [{"a"=>1},{"a"=>2}]
    tag = "tag1"

    events.each { |ev|
      d.expect_emit tag, time, ev
    }

    d.run do
      res = post("/#{tag}", {"msgpack"=>events.to_msgpack, "time"=>time.to_s})
      assert_equal "200", res.code
    end

  end

  def test_with_regexp
    d = create_driver(CONFIG + %[
      format /^(?<field_1>\\d+):(?<field_2>\\w+)$/
      types field_1:integer
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"field_1" => 1, "field_2" => 'str'}
    d.expect_emit "tag2", time, {"field_1" => 2, "field_2" => 'str'}

    d.run do
      d.expected_emits.each { |tag, time, record|
        body = record.map { |k, v|
          v.to_s
        }.join(':')
        res = post("/#{tag}?time=#{time.to_s}", body)
        assert_equal "200", res.code
      }
    end
  end

  def test_with_csv
    require 'csv'

    d = create_driver(CONFIG + %[
      format csv
      keys foo,bar
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"foo" => "1", "bar" => 'st"r'}
    d.expect_emit "tag2", time, {"foo" => "2", "bar" => 'str'}

    d.run do
      d.expected_emits.each { |tag, time, record|
        body = record.map { |k, v| v }.to_csv
        res = post("/#{tag}?time=#{time.to_s}", body)
        assert_equal "200", res.code
      }
    end
  end

  def test_resonse_with_empty_img
    d = create_driver(CONFIG + "respond_with_empty_img true")
    assert_equal true, d.instance.respond_with_empty_img

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>time.to_s})
        assert_equal "200", res.code
        # Ruby returns ASCII-8 encoded string for GIF.
        assert_equal Fluent::HttpInput::EMPTY_GIF_IMAGE, res.body.force_encoding("UTF-8")
      }
    end
  end

  def test_cors_allowed
    d = create_driver(CONFIG + "cors_allow_origins [\"http://foo.com\"]")
    assert_equal ["http://foo.com"], d.instance.cors_allow_origins

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>1}

    d.run do
      d.expected_emits.each {|tag,_time,record|
        res = post("/#{tag}", {"json"=>record.to_json, "time"=>_time.to_s}, {"Origin"=>"http://foo.com"})
        assert_equal "200", res.code
        assert_equal "http://foo.com", res["Access-Control-Allow-Origin"]
      }
    end
  end

  def test_cors_disallowed
    d = create_driver(CONFIG + "cors_allow_origins [\"http://foo.com\"]")
    assert_equal ["http://foo.com"], d.instance.cors_allow_origins

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expected_emits_length = 0
    d.run do
      res = post("/tag1", {"json"=>{"a"=>1}.to_json, "time"=>time.to_s}, {"Origin"=>"http://bar.com"})
      assert_equal "403", res.code
      res = post("/tag2", {"json"=>{"a"=>1}.to_json, "time"=>time.to_s}, {"Origin"=>"http://bar.com"})
      assert_equal "403", res.code
    end
  end

  $test_in_http_connection_object_ids = []
  $test_in_http_content_types = []
  $test_in_http_content_types_flag = false
  module ContentTypeHook
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

  class Fluent::HttpInput::Handler
    prepend ContentTypeHook
  end

  def test_if_content_type_is_initialized_properly
    # This test is to check if Fluent::HttpInput::Handler's @content_type is initialized properly.
    # Especially when in Keep-Alive and the second request has no 'Content-Type'.
    #
    # Actually, in the current implementation of in_http, we can't test it directly.
    # So we replace Fluent::HttpInput::Handler temporally with the extended Handler
    # in order to collect @content_type(s) per request.
    # Finally, we check those collected @content_type(s).

    # Save the original Handler
    orig_handler = Fluent::HttpInput::Handler

    begin
      # Create the extended Handler which can store @content_type per request
      ext_handler = Class.new(Fluent::HttpInput::Handler) do
        @@content_types = []

        def self.content_types
          @@content_types
        end

        def on_message_complete
          @@content_types << @content_type
          super
        end
      end

      # Replace the original Handler temporally with the extended one
      Fluent::HttpInput.module_eval do
        remove_const(:Handler) if const_defined?(:Handler)
        const_set(:Handler, ext_handler)
      end

      d = create_driver

      d.run do
        # Send two requests the second one has no Content-Type in Keep-Alive
        Net::HTTP.start("127.0.0.1", PORT) do |http|
          req = Net::HTTP::Post.new("/foodb/bartbl", {"connection" => "keepalive", "content-type" => "application/json"})
          res = http.request(req)

          req = Net::HTTP::Get.new("/foodb/bartbl", {"connection" => "keepalive"})
          res = http.request(req)
        end

        assert_equal(['application/json', ''], ext_handler.content_types)
      end
    ensure
      # Revert the original Handler
      Fluent::HttpInput.module_eval do
        remove_const(:Handler) if const_defined?(:Handler)
        const_set(:Handler, orig_handler)
      end
    end
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
end
