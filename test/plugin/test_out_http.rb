require_relative "../helper"
require 'fluent/test/driver/output'
require 'fluent/plugin/out_http'

require 'webrick'
require 'webrick/https'
require 'net/http'
require 'uri'
require 'json'
require 'aws-sdk-core'

# WEBrick's ProcHandler doesn't handle PUT by default
module WEBrick::HTTPServlet
  class ProcHandler < AbstractServlet
    alias do_PUT do_GET
  end
end

class HTTPOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  TMP_DIR = File.join(__dir__, "../tmp/out_http#{ENV['TEST_ENV_NUMBER']}")
  DEFAULT_LOGGER = ::WEBrick::Log.new(::STDOUT, ::WEBrick::BasicLog::FATAL)

  class << self
    # Use class variable to reduce server start/shutdown time
    def startup
      @@result = nil
      @@auth_handler = nil
      @@http_server_thread = nil
    end

    def shutdown
      @@http_server_thread.kill
      @@http_server_thread.join
    rescue
    end
  end

  def server_port
    19880
  end

  def base_endpoint
    "http://127.0.0.1:#{server_port}"
  end

  def server_config
    config = {BindAddress: '127.0.0.1', Port: server_port}
    # Suppress webrick logs
    config[:Logger] = DEFAULT_LOGGER
    config[:AccessLog] = []
    config
  end

  def http_client(**opts, &block)
    opts = opts.merge(open_timeout: 1, read_timeout: 1)
    if block_given?
      Net::HTTP.start('127.0.0.1', server_port, **opts, &block)
    else
      Net::HTTP.start('127.0.0.1', server_port, **opts)
    end
  end

  def run_http_server
    server = ::WEBrick::HTTPServer.new(server_config)
    server.mount_proc('/test') { |req, res|
      if @@auth_handler
        @@auth_handler.call(req, res)
      end

      @@result.method = req.request_method
      @@result.content_type = req.content_type
      req.each do |key, value|
        @@result.headers[key] = value
      end

      data = []
      case req.content_type
      when 'application/x-ndjson'
        req.body.each_line { |l|
          data << JSON.parse(l)
        }
      when 'application/json'
        data = JSON.parse(req.body)
      when 'text/plain'
        # Use single_value in this test
        req.body.each_line { |line|
          data << line.chomp
        }
      else
        data << req.body
      end
      @@result.data = data

      res.status = 200
      res.body = "success"
    }
    server.mount_proc('/503') { |_, res|
      res.status = 503
      res.body = 'Service Unavailable'
    }
    server.mount_proc('/404') { |_, res|
      res.status = 404
      res.body = 'Not Found'
    }
    # For start check
    server.mount_proc('/') { |_, res|
      res.status = 200
      res.body = 'Hello Fluentd!'
    }
    server.start
  ensure
    server.shutdown rescue nil
  end

  Result = Struct.new("Result", :method, :content_type, :headers, :data)

  setup do
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)

    @@result = Result.new(nil, nil, {}, nil)
    @@http_server_thread ||= Thread.new do
      run_http_server
    end

    now = Time.now
    started = false
    until started
      raise "Server not started" if (Time.now - now > 10.0)
      begin
        http_client { |c| c.request_get('/') }
        started = true
      rescue
        sleep 0.5
      end
    end
  end

  teardown do
    @@result = nil
    @@auth_handler = nil
  end

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::HTTPOutput).configure(conf)
  end

  def config
    %[
      endpoint #{base_endpoint}/test
    ]
  end

  def test_events
    [
      {"message" => "hello", "num" => 10, "bool" => true},
      {"message" => "hello", "num" => 11, "bool" => false}
    ]
  end

  def test_configure
    d = create_driver(config)
    assert_equal "http://127.0.0.1:#{server_port}/test", d.instance.endpoint
    assert_equal :post, d.instance.http_method
    assert_equal 'application/x-ndjson', d.instance.content_type
    assert_equal [503], d.instance.retryable_response_codes
    assert_true d.instance.error_response_as_unrecoverable
    assert_nil d.instance.proxy
    assert_nil d.instance.headers
  end

  def test_configure_with_warn
    d = create_driver(config)
    assert_match(/Status code 503 is going to be removed/, d.instance.log.out.logs.join)
  end

  def test_configure_without_warn
    d = create_driver(<<~CONFIG)
      endpoint #{base_endpoint}/test
      retryable_response_codes [503]
    CONFIG
    assert_not_match(/Status code 503 is going to be removed/, d.instance.log.out.logs.join)
  end

  # Check if an exception is raised on not JSON format use
  data('not_json' => 'msgpack')
  def test_configure_with_json_array_err(format_type)
    assert_raise(Fluent::ConfigError) do
      create_driver(config + %[
        json_array true
        <format>
          @type #{format_type}
        </format>
      ])
    end
  end

  data('json' => ['json', 'application/x-ndjson'],
       'ltsv' => ['ltsv', 'text/tab-separated-values'],
       'msgpack' => ['msgpack', 'application/x-msgpack'],
       'single_value' => ['single_value', 'text/plain'])
  def test_configure_content_type(types)
    format_type, content_type = types
    d = create_driver(config + %[
      <format>
        @type #{format_type}
      </format>
    ])
    assert_equal content_type, d.instance.content_type
  end

  # Check that json_array setting sets content_type = application/json
  data('json' => 'application/json')
  def test_configure_content_type_json_array(content_type)
    d = create_driver(config + "json_array true")

    assert_equal content_type, d.instance.content_type
  end

  data('PUT' => 'put', 'POST' => 'post')
  def test_write_with_method(method)
    d = create_driver(config + "http_method #{method}")
    d.run(default_tag: 'test.http') do
      test_events.each { |event|
        d.feed(event)
      }
    end

    result = @@result
    assert_equal method.upcase, result.method
    assert_equal 'application/x-ndjson', result.content_type
    assert_equal test_events, result.data
    assert_not_empty result.headers
  end

  # Check that JSON at HTTP request body is valid
  def test_write_with_json_array_setting
    d = create_driver(config + "json_array true")
    d.run(default_tag: 'test.http') do
      test_events.each { |event|
        d.feed(event)
      }
    end

    result = @@result
    assert_equal 'application/json', result.content_type
    assert_equal test_events, result.data
    assert_not_empty result.headers
  end

  def test_write_with_single_value_format
    d = create_driver(config + %[
      <format>
        @type single_value
      </format>
    ])
    d.run(default_tag: 'test.http') do
      test_events.each { |event|
        d.feed(event)
      }
    end

    result = @@result
    assert_equal 'text/plain', result.content_type
    assert_equal (test_events.map { |e| e['message'] }), result.data
    assert_not_empty result.headers
  end

  def test_write_with_headers
    d = create_driver(config + 'headers {"test_header":"fluentd!"}')
    d.run(default_tag: 'test.http') do
      test_events.each { |event|
        d.feed(event)
      }
    end

    result = @@result
    assert_true result.headers.has_key?('test_header')
    assert_equal "fluentd!", result.headers['test_header']
  end

  def test_write_with_headers_from_placeholders
    d = create_driver(config + %[
      headers_from_placeholders {"x-test":"${$.foo.bar}-test","x-tag":"${tag}"}
      <buffer tag,$.foo.bar>
      </buffer>
    ])
    d.run(default_tag: 'test.http') do
      test_events.each { |event|
        ev = event.dup
        ev['foo'] = {'bar' => 'abcd'}
        d.feed(ev)
      }
    end

    result = @@result
    assert_equal "abcd-test", result.headers['x-test']
    assert_equal "test.http", result.headers['x-tag']
  end

  def test_write_with_retryable_response
    old_report_on_exception = Thread.report_on_exception
    Thread.report_on_exception = false # thread finished as invalid state since RetryableResponse raises.

    d = create_driver("endpoint #{base_endpoint}/503")
    assert_raise(Fluent::Plugin::HTTPOutput::RetryableResponse) do
      d.run(default_tag: 'test.http', shutdown: false) do
        test_events.each { |event|
          d.feed(event)
        }
      end
    end

    d.instance_shutdown(log: $log)
  ensure
    Thread.report_on_exception = old_report_on_exception
  end

  def test_write_with_disabled_unrecoverable
    d = create_driver(%[
      endpoint #{base_endpoint}/404
      error_response_as_unrecoverable false
    ])
    d.run(default_tag: 'test.http', shutdown: false) do
      test_events.each { |event|
        d.feed(event)
      }
    end
    assert_match(/got error response from.*404 Not Found Not Found/, d.instance.log.out.logs.join)
    d.instance_shutdown
  end

  sub_test_case 'basic auth' do
    setup do
      FileUtils.mkdir_p(TMP_DIR)
      htpd = WEBrick::HTTPAuth::Htpasswd.new(File.join(TMP_DIR, 'dot.htpasswd'))
      htpd.set_passwd(nil, 'test', 'hey')
      authenticator = WEBrick::HTTPAuth::BasicAuth.new(:UserDB => htpd, :Realm => 'test', :Logger => DEFAULT_LOGGER)
      @@auth_handler = Proc.new { |req, res| authenticator.authenticate(req, res) }
    end

    teardown do
      FileUtils.rm_rf(TMP_DIR)
    end

    def server_port
      19881
    end

    def test_basic_auth
      d = create_driver(config + %[
        <auth>
          method basic
          username test
          password hey
        </auth>
      ])
      d.run(default_tag: 'test.http') do
        test_events.each { |event|
          d.feed(event)
        }
      end

      result = @@result
      assert_equal 'POST', result.method
      assert_equal 'application/x-ndjson', result.content_type
      assert_equal test_events, result.data
      assert_not_empty result.headers
    end

    # This test includes `error_response_as_unrecoverable true` behaviour check
    def test_basic_auth_with_invalid_auth
      d = create_driver(config + %[
        <auth>
          method basic
          username ayaya
          password hello?
        </auth>
      ])
      d.instance.system_config_override(root_dir: TMP_DIR) # Backup files are generated in TMP_DIR.
      d.run(default_tag: 'test.http', shutdown: false) do
        test_events.each { |event|
          d.feed(event)
        }
      end
      assert_match(/got unrecoverable error/, d.instance.log.out.logs.join)

      d.instance_shutdown
    end
  end


  sub_test_case 'aws sigv4 auth' do
    setup do
      @@fake_aws_credentials = Aws::Credentials.new(
        'fakeaccess',
        'fakesecret',
        'fake session token'
      )
    end

    def server_port
      19883
    end

    def test_aws_sigv4_sts_role_arn
      stub(Aws::AssumeRoleCredentials).new do |credentials_provider|
        stub(credentials_provider).credentials {
          @@fake_aws_credentials
        }
        credentials_provider
      end

      d = create_driver(config + %[
          <auth>
            method aws_sigv4
            aws_service someservice
            aws_region my-region-1
            aws_role_arn arn:aws:iam::123456789012:role/MyRole
          </auth>
        ])
      d.run(default_tag: 'test.http') do
        test_events.each { |event|
          d.feed(event)
        }
      end

      result = @@result
      assert_equal 'POST', result.method
      assert_equal 'application/x-ndjson', result.content_type
      assert_equal test_events, result.data
      assert_not_empty result.headers
      assert_not_nil result.headers['authorization']
      assert_match /AWS4-HMAC-SHA256 Credential=[a-zA-Z0-9]*\/\d+\/my-region-1\/someservice\/aws4_request/, result.headers['authorization']
      assert_match /SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token/, result.headers['authorization']
      assert_equal @@fake_aws_credentials.session_token, result.headers['x-amz-security-token']
      assert_not_nil result.headers['x-amz-content-sha256']
      assert_not_empty result.headers['x-amz-content-sha256']
      assert_not_nil result.headers['x-amz-security-token']
      assert_not_empty result.headers['x-amz-security-token']
      assert_not_nil result.headers['x-amz-date']
      assert_not_empty result.headers['x-amz-date']
    end

    def test_aws_sigv4_no_role
      stub(Aws::CredentialProviderChain).new do |provider_chain|
        stub(provider_chain).resolve {
          @@fake_aws_credentials
        }
        provider_chain
      end
      d = create_driver(config + %[
          <auth>
            method aws_sigv4
            aws_service someservice
            aws_region my-region-1
          </auth>
        ])
      d.run(default_tag: 'test.http') do
        test_events.each { |event|
          d.feed(event)
        }
      end

      result = @@result
      assert_equal 'POST', result.method
      assert_equal 'application/x-ndjson', result.content_type
      assert_equal test_events, result.data
      assert_not_empty result.headers
      assert_not_nil result.headers['authorization']
      assert_match /AWS4-HMAC-SHA256 Credential=[a-zA-Z0-9]*\/\d+\/my-region-1\/someservice\/aws4_request/, result.headers['authorization']
      assert_match /SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token/, result.headers['authorization']
      assert_equal @@fake_aws_credentials.session_token, result.headers['x-amz-security-token']
      assert_not_nil result.headers['x-amz-content-sha256']
      assert_not_empty result.headers['x-amz-content-sha256']
      assert_not_nil result.headers['x-amz-security-token']
      assert_not_empty result.headers['x-amz-security-token']
      assert_not_nil result.headers['x-amz-date']
      assert_not_empty result.headers['x-amz-date']
    end
  end

  sub_test_case 'HTTPS' do
    def server_port
      19882
    end

    def server_config
      config = super
      # WEBrick supports self-generated self-signed certificate
      config[:SSLEnable] = true
      config[:SSLCertName] = [["CN", WEBrick::Utils::getservername]]
      config
    end

    def http_client(&block)
      super(use_ssl: true, verify_mode: OpenSSL::SSL::VERIFY_NONE, &block)
    end

    def test_write_with_https
      d = create_driver(%[
        endpoint https://127.0.0.1:#{server_port}/test
        tls_verify_mode none
        ssl_timeout 2s
      ])
      d.run(default_tag: 'test.http') do
        test_events.each { |event|
          d.feed(event)
        }
      end

      result = @@result
      assert_equal 'POST', result.method
      assert_equal 'application/x-ndjson', result.content_type
      assert_equal test_events, result.data
      assert_not_empty result.headers
    end
  end

  sub_test_case 'connection_reuse' do
    def server_port
      19883
    end

    def test_connection_recreation
      d = create_driver(%[
        endpoint http://127.0.0.1:#{server_port}/test
        reuse_connections true
      ])

      d.run(default_tag: 'test.http', shutdown: false) do
        d.feed(test_events[0])
      end

      data = @@result.data

      # Restart server to simulate connection loss
      @@http_server_thread.kill
      @@http_server_thread.join
      @@http_server_thread = Thread.new do
        run_http_server
      end

      d.run(default_tag: 'test.http') do
        d.feed(test_events[1])
      end

      result = @@result
      assert_equal 'POST', result.method
      assert_equal 'application/x-ndjson', result.content_type
      assert_equal test_events, data.concat(result.data)
      assert_not_empty result.headers
    end
  end
end
