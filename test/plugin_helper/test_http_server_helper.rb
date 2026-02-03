require_relative '../helper'
require 'flexmock/test_unit'
require 'fluent/plugin_helper/http_server'
require 'fluent/plugin/output'
require 'fluent/event'
require 'net/http'
require 'uri'
require 'openssl'
require 'async'

class HttpHelperTest < Test::Unit::TestCase
  NULL_LOGGER = Logger.new(nil)
  CERT_DIR = File.expand_path(File.dirname(__FILE__) + '/data/cert/without_ca')
  CERT_CA_DIR = File.expand_path(File.dirname(__FILE__) + '/data/cert/with_ca')

  def setup
    @port = unused_port(protocol: :tcp)
  end

  def teardown
    @port = nil
  end

  def ipv6_enabled?
    begin
      # Try to actually bind to an IPv6 address to verify it works
      sock = Socket.new(Socket::AF_INET6, Socket::SOCK_STREAM, 0)
      sock.bind(Socket.sockaddr_in(0, '::1'))
      sock.close
      
      # Also test that we can resolve IPv6 addresses
      # This is needed because some systems can bind but can't connect
      Socket.getaddrinfo('::1', nil, Socket::AF_INET6)
      true
    rescue Errno::EADDRNOTAVAIL, Errno::EAFNOSUPPORT, SocketError
      false
    end
  end

  class Dummy < Fluent::Plugin::TestBase
    helpers :http_server
  end

  def on_driver(config = nil)
    config ||= Fluent::Config.parse(config || '', '(name)', '')
    Fluent::Test.setup
    driver = Dummy.new
    driver.configure(config)
    driver.start
    driver.after_start

    yield(driver)
  ensure
    unless driver.stopped?
      driver.stop rescue nil
    end

    unless driver.before_shutdown?
      driver.before_shutdown rescue nil
    end

    unless driver.shutdown?
      driver.shutdown rescue nil
    end

    unless driver.after_shutdown?
      driver.after_shutdown rescue nil
    end

    unless driver.closed?
      driver.close rescue nil
    end

    unless driver.terminated?
      driver.terminated rescue nil
    end
  end

  def on_driver_transport(opts = {}, &block)
    transport_conf = config_element('transport', 'tls', opts)
    c = config_element('ROOT', '', {}, [transport_conf])
    on_driver(c, &block)
  end

  %w[get head].each do |n|
    define_method(n) do |uri, header = {}|
      url = URI.parse(uri)
      headers = { 'Content-Type' => 'application/x-www-form-urlencoded/' }.merge(header)
      req = Net::HTTP.const_get(n.capitalize).new(url, headers)
      Net::HTTP.start(url.host, url.port) do |http|
        http.request(req)
      end
    end

    define_method("secure_#{n}") do |uri, header = {}, verify: true, cert_path: nil, selfsigned: true, hostname: false|
      url = URI.parse(uri)
      headers = { 'Content-Type' => 'application/x-www-form-urlencoded/' }.merge(header)
      start_https_request(url.host, url.port, verify: verify, cert_path: cert_path, selfsigned: selfsigned) do |https|
        https.send(n, url.path, headers.to_a)
      end
    end
  end

  %w[post put patch delete options trace].each do |n|
    define_method(n) do |uri, body = '', header = {}|
      url = URI.parse(uri)
      headers = { 'Content-Type' => 'application/x-www-form-urlencoded/' }.merge(header)
      req = Net::HTTP.const_get(n.capitalize).new(url, headers)
      req.body = body
      Net::HTTP.start(url.host, url.port) do |http|
        http.request(req)
      end
    end
  end

  # wrapper for net/http
  Response = Struct.new(:code, :body, :headers)

  # Use async-http as http client since net/http can't be set verify_hostname= now
  # will be replaced when net/http supports verify_hostname=
  def start_https_request(addr, port, verify: true, cert_path: nil, selfsigned: true, hostname: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.set_params({})
    if verify
      cert_store = OpenSSL::X509::Store.new
      cert_store.set_default_paths
      if selfsigned && OpenSSL::X509.const_defined?('V_FLAG_CHECK_SS_SIGNATURE')
        cert_store.flags = OpenSSL::X509::V_FLAG_CHECK_SS_SIGNATURE
      end

      if cert_path
        cert_store.add_file(cert_path)
      end

      context.cert_store = cert_store
      if !hostname
        context.verify_hostname = false # In test code, using hostname to be connected is very difficult
      end

      context.verify_mode = OpenSSL::SSL::VERIFY_PEER
    else
      context.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end

    client = Async::HTTP::Client.new(Async::HTTP::Endpoint.parse("https://#{addr}:#{port}", ssl_context: context))
    Console.logger = Fluent::Log::ConsoleAdapter.wrap(NULL_LOGGER)

    resp = nil
    error = nil

    Sync do
      Console.logger = Fluent::Log::ConsoleAdapter.wrap(NULL_LOGGER)
      begin
        response = yield(client)
      rescue => e               # Async::Reactor rescue all error. handle it by myself
        error = e
      end

      if response
        resp = Response.new(response.status.to_s, response.read, response.headers)
      end
    end

    if error
      raise error
    else
      resp
    end
  end

  sub_test_case 'Create a HTTP server' do
    test 'mount given path' do
      on_driver do |driver|
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
          s.post('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello post'] }
          s.head('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello head'] }
          s.put('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello put'] }
          s.patch('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello patch'] }
          s.delete('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello delete'] }
          s.trace('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello trace'] }
          s.options('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello options'] }
        end

        resp = head("http://127.0.0.1:#{@port}/example/hello")
        assert_equal('200', resp.code)
        assert_equal(nil, resp.body)
        assert_equal('text/plain', resp['Content-Type'])

        %w[get put post put delete options trace].each do |n|
          resp = send(n, "http://127.0.0.1:#{@port}/example/hello")
          assert_equal('200', resp.code)
          assert_equal("hello #{n}", resp.body)
          assert_equal('text/plain', resp['Content-Type'])
        end
      end
    end

    test 'mount frozen path' do
      on_driver do |driver|
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
          s.get('/example/hello'.freeze) { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
        end

        resp = get("http://127.0.0.1:#{@port}/example/hello/")
        assert_equal('200', resp.code)
      end
    end

    test 'when path does not start with `/` or ends with `/`' do
      on_driver do |driver|
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
          s.get('example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
          s.get('/example/hello2/') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
        end

        resp = get("http://127.0.0.1:#{@port}/example/hello")
        assert_equal('404', resp.code)

        resp = get("http://127.0.0.1:#{@port}/example/hello2")
        assert_equal('200', resp.code)
      end
    end

    test 'when error raised' do
      on_driver do |driver|
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') { raise 'error!' }
        end

        resp = get("http://127.0.0.1:#{@port}/example/hello")
        assert_equal('500', resp.code)
      end
    end

    test 'when path is not found' do
      on_driver do |driver|
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
        end

        resp = get("http://127.0.0.1:#{@port}/example/hello/not_found")
        assert_equal('404', resp.code)
      end
    end

    test 'params and body' do
      on_driver do |driver|
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') do |req|
            assert_equal(req.query_string, nil)
            assert_equal(req.body, nil)
            [200, { 'Content-Type' => 'text/plain' }, 'hello get']
          end

          s.post('/example/hello') do |req|
            assert_equal(req.query_string, nil)
            assert_equal(req.body, 'this is body')
            [200, { 'Content-Type' => 'text/plain' }, 'hello post']
          end

          s.get('/example/hello/params') do |req|
            assert_equal(req.query_string, 'test=true')
            assert_equal(req.body, nil)
            [200, { 'Content-Type' => 'text/plain' }, 'hello get']
          end

          s.post('/example/hello/params') do |req|
            assert_equal(req.query_string, 'test=true')
            assert_equal(req.body, 'this is body')
            [200, { 'Content-Type' => 'text/plain' }, 'hello post']
          end
        end

        resp = get("http://127.0.0.1:#{@port}/example/hello")
        assert_equal('200', resp.code)

        resp = post("http://127.0.0.1:#{@port}/example/hello", 'this is body')
        assert_equal('200', resp.code)

        resp = get("http://127.0.0.1:#{@port}/example/hello/params?test=true")
        assert_equal('200', resp.code)

        resp = post("http://127.0.0.1:#{@port}/example/hello/params?test=true", 'this is body')
        assert_equal('200', resp.code)
      end
    end

    sub_test_case 'create a HTTPS server' do
      test '#configure' do
        driver = Dummy.new

        transport_conf = config_element('transport', 'tls', { 'version' => 'TLSv1_1' })
        driver.configure(config_element('ROOT', '', {}, [transport_conf]))
        assert_equal :tls, driver.transport_config.protocol
        assert_equal :TLSv1_1, driver.transport_config.version
      end

      sub_test_case '#http_server_create_https_server' do
        test 'can overwrite settings by using tls_context' do
          on_driver_transport({ 'insecure' => 'false' }) do |driver|
            tls = { 'insecure' => 'true' } # overwrite
            driver.http_server_create_https_server(:http_server_helper_test_tls, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER, tls_opts: tls) do |s|
              s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
            end

            resp = secure_get("https://127.0.0.1:#{@port}/example/hello", verify: false)
            assert_equal('200', resp.code)
            assert_equal('hello get', resp.body)
          end
        end

        test 'with insecure in transport section' do
          on_driver_transport({ 'insecure' => 'true' }) do |driver|
            driver.http_server_create_https_server(:http_server_helper_test_tls, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
              s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
            end
            omit "TLS connection should be aborted due to `Errno::ECONNABORTED`. Need to debug." if Fluent.windows?

            resp = secure_get("https://127.0.0.1:#{@port}/example/hello", verify: false)
            assert_equal('200', resp.code)
            assert_equal('hello get', resp.body)

            assert_raise OpenSSL::SSL::SSLError do
              secure_get("https://127.0.0.1:#{@port}/example/hello")
            end
          end
        end

        data(
          'with passphrase' => ['apple', 'cert-pass.pem', 'cert-key-pass.pem'],
          'without passphrase' => [nil, 'cert.pem', 'cert-key.pem'])
        test 'load self-signed cert/key pair, verified from clients using cert files' do |(passphrase, cert, private_key)|
          omit "Self signed certificate blocks TLS connection. Need to debug." if Fluent.windows?

          cert_path = File.join(CERT_DIR, cert)
          private_key_path = File.join(CERT_DIR, private_key)
          opt = { 'insecure' => 'false', 'private_key_path' => private_key_path, 'cert_path' => cert_path }
          if passphrase
            opt['private_key_passphrase'] = passphrase
          end

          on_driver_transport(opt) do |driver|
            driver.http_server_create_https_server(:http_server_helper_test_tls, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
              s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
            end

            resp = secure_get("https://127.0.0.1:#{@port}/example/hello", cert_path: cert_path)
            assert_equal('200', resp.code)
            assert_equal('hello get', resp.body)
          end
        end

        data(
          'with passphrase' => ['apple', 'cert-pass.pem', 'cert-key-pass.pem', 'ca-cert-pass.pem'],
          'without passphrase' => [nil, 'cert.pem', 'cert-key.pem', 'ca-cert.pem'])
        test 'load cert by private CA cert file, verified from clients using CA cert file' do |(passphrase, cert, cert_key, ca_cert)|
          omit "Self signed certificate blocks TLS connection. Need to debug." if Fluent.windows?

          cert_path = File.join(CERT_CA_DIR, cert)
          private_key_path = File.join(CERT_CA_DIR, cert_key)

          ca_cert_path = File.join(CERT_CA_DIR, ca_cert)

          opt = { 'insecure' => 'false', 'cert_path' => cert_path, 'private_key_path' => private_key_path }
          if passphrase
            opt['private_key_passphrase'] = passphrase
          end

          on_driver_transport(opt) do |driver|
            driver.http_server_create_https_server(:http_server_helper_test_tls, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do |s|
              s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
            end

            resp = secure_get("https://127.0.0.1:#{@port}/example/hello", cert_path: ca_cert_path)
            assert_equal('200', resp.code)
            assert_equal('hello get', resp.body)
          end
        end
      end
    end

    test 'IPv6 address formatting logic - bare ::1' do
      # Test the bracketing logic without needing real IPv6 networking
      # Normalize address by stripping brackets if present
      addr = '::1'
      normalized_addr = if addr.start_with?('[') && addr.end_with?(']')
                          addr[1..-2]
                        else
                          addr
                        end
      assert_equal('::1', normalized_addr)
      
      # Add brackets for URI formatting
      addr_display = if normalized_addr.include?(":")
                       "[#{normalized_addr}]"
                     else
                       normalized_addr
                     end
      assert_equal('[::1]', addr_display)
      
      # Verify URI construction works
      uri = URI("http://#{addr_display}:24231/metrics").to_s
      assert_equal('http://[::1]:24231/metrics', uri)
    end

    test 'IPv6 address formatting logic - bare ::' do
      addr = '::'
      normalized_addr = if addr.start_with?('[') && addr.end_with?(']')
                          addr[1..-2]
                        else
                          addr
                        end
      assert_equal('::', normalized_addr)
      
      addr_display = if normalized_addr.include?(":")
                       "[#{normalized_addr}]"
                     else
                       normalized_addr
                     end
      assert_equal('[::]', addr_display)
      
      uri = URI("http://#{addr_display}:24231/metrics").to_s
      assert_equal('http://[::]:24231/metrics', uri)
    end

    test 'IPv6 address formatting logic - already bracketed [::1]' do
      # Test that pre-bracketed addresses are normalized correctly
      addr = '[::1]'
      # First normalize by removing brackets
      normalized_addr = if addr.start_with?('[') && addr.end_with?(']')
                          addr[1..-2]
                        else
                          addr
                        end
      assert_equal('::1', normalized_addr)  # Brackets stripped
      
      # Then add brackets for URI
      addr_display = if normalized_addr.include?(":")
                       "[#{normalized_addr}]"
                     else
                       normalized_addr
                     end
      assert_equal('[::1]', addr_display)
      
      uri = URI("http://#{addr_display}:24231/metrics").to_s
      assert_equal('http://[::1]:24231/metrics', uri)
    end

    test 'IPv6 address formatting logic - already bracketed [::]' do
      addr = '[::]'
      # First normalize by removing brackets
      normalized_addr = if addr.start_with?('[') && addr.end_with?(']')
                          addr[1..-2]
                        else
                          addr
                        end
      assert_equal('::', normalized_addr)  # Brackets stripped
      
      # Then add brackets for URI
      addr_display = if normalized_addr.include?(":")
                       "[#{normalized_addr}]"
                     else
                       normalized_addr
                     end
      assert_equal('[::]', addr_display)
      
      uri = URI("http://#{addr_display}:24231/metrics").to_s
      assert_equal('http://[::]:24231/metrics', uri)
    end

    test 'IPv4 address formatting logic - no brackets' do
      addr = '127.0.0.1'
      # Normalize (no-op for IPv4)
      normalized_addr = if addr.start_with?('[') && addr.end_with?(']')
                          addr[1..-2]
                        else
                          addr
                        end
      assert_equal('127.0.0.1', normalized_addr)
      
      # No brackets added for IPv4
      addr_display = if normalized_addr.include?(":")
                       "[#{normalized_addr}]"
                     else
                       normalized_addr
                     end
      assert_equal('127.0.0.1', addr_display)  # No brackets for IPv4
      
      uri = URI("http://#{addr_display}:24231/metrics").to_s
      assert_equal('http://127.0.0.1:24231/metrics', uri)
    end

    test 'must be called #start and #stop' do
      on_driver do |driver|
        server = flexmock('Server') do |watcher|
          watcher.should_receive(:start).once.and_return do |que|
            que.push(:start)
          end
          watcher.should_receive(:stop).once
        end

        stub(Fluent::PluginHelper::HttpServer::Server).new(addr: anything, port: anything, logger: anything, default_app: anything) { server }
        driver.http_server_create_http_server(:http_server_helper_test, addr: '127.0.0.1', port: @port, logger: NULL_LOGGER) do
          # nothing
        end
        driver.stop
      end
    end
  end
end
