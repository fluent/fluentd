require_relative '../helper'
require 'fluent/test'
require 'net/http'
require 'fluent/plugin/in_http'

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
