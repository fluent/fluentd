require_relative '../helper'
require 'flexmock/test_unit'
require 'fluent/plugin_helper/http_server'
require 'fluent/plugin/output'
require 'fluent/event'
require 'net/http'
require 'uri'

class HtttpHelperTest < Test::Unit::TestCase
  PORT = unused_port
  NULL_LOGGER = Logger.new(nil)

  class Dummy < Fluent::Plugin::TestBase
    helpers :http_server
  end

  def on_driver
    Fluent::Test.setup
    driver = Dummy.new
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

  %w[get head].each do |n|
    define_method(n) do |uri, header = {}|
      url = URI.parse(uri)
      req = Net::HTTP.const_get(n.capitalize).new(url, header)
      Net::HTTP.start(url.host, url.port) do |http|
        http.request(req)
      end
    end
  end

  %w[post put patch delete options trace].each do |n|
    define_method(n) do |uri, body = '', header = {}|
      url = URI.parse(uri)
      req = Net::HTTP.const_get(n.capitalize).new(url, header)
      req.body = body
      Net::HTTP.start(url.host, url.port) do |http|
        http.request(req)
      end
    end
  end

  sub_test_case 'Create a HTTP server' do
    test 'monunt given path' do
      on_driver do |driver|
        driver.create_http_server(addr: '127.0.0.1', port: PORT, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
          s.post('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello post'] }
          s.head('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello head'] }
          s.put('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello put'] }
          s.patch('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello patch'] }
          s.delete('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello delete'] }
          s.trace('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello trace'] }
          s.options('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello options'] }
        end

        resp = head("http://127.0.0.1:#{PORT}/example/hello")
        assert_equal('200', resp.code)
        assert_equal(nil, resp.body)
        assert_equal('text/plain', resp['Content-Type'])

        %w[get put post put delete options trace].each do |n|
          resp = send(n, "http://127.0.0.1:#{PORT}/example/hello")
          assert_equal('200', resp.code)
          assert_equal("hello #{n}", resp.body)
          assert_equal('text/plain', resp['Content-Type'])
        end
      end
    end

    test 'when path does not start with `/` or ends with `/`' do
      on_driver do |driver|
        driver.create_http_server(addr: '127.0.0.1', port: PORT, logger: NULL_LOGGER) do |s|
          s.get('example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
          s.get('/example/hello2/') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
        end

        resp = get("http://127.0.0.1:#{PORT}/example/hello")
        assert_equal('404', resp.code)

        resp = get("http://127.0.0.1:#{PORT}/example/hello2")
        assert_equal('200', resp.code)
      end
    end

    test 'when error raised' do
      on_driver do |driver|
        driver.create_http_server(addr: '127.0.0.1', port: PORT, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') { raise 'error!' }
        end

        resp = get("http://127.0.0.1:#{PORT}/example/hello")
        assert_equal('500', resp.code)
      end
    end

    test 'when path is not found' do
      on_driver do |driver|
        driver.create_http_server(addr: '127.0.0.1', port: PORT, logger: NULL_LOGGER) do |s|
          s.get('/example/hello') { [200, { 'Content-Type' => 'text/plain' }, 'hello get'] }
        end

        resp = get("http://127.0.0.1:#{PORT}/example/hello/not_found")
        assert_equal('404', resp.code)
      end
    end

    test 'params and body' do
      on_driver do |driver|
        driver.create_http_server(addr: '127.0.0.1', port: PORT, logger: NULL_LOGGER) do |s|
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

        resp = get("http://127.0.0.1:#{PORT}/example/hello")
        assert_equal('200', resp.code)

        resp = post("http://127.0.0.1:#{PORT}/example/hello", 'this is body')
        assert_equal('200', resp.code)

        resp = get("http://127.0.0.1:#{PORT}/example/hello/params?test=true")
        assert_equal('200', resp.code)

        resp = post("http://127.0.0.1:#{PORT}/example/hello/params?test=true", 'this is body')
        assert_equal('200', resp.code)
      end
    end

    test 'must be called #start and #stop' do
      on_driver do |driver|
        server = flexmock('Server') do |watcher|
          watcher.should_receive(:start).once.and_return do |que|
            que.push(:start)
          end
          watcher.should_receive(:stop).once
        end

        stub(Fluent::PluginHelper::HttpServer::Server).new(anything) { server }
        driver.create_http_server(addr: '127.0.0.1', port: PORT, logger: NULL_LOGGER) do
          # nothing
        end
        driver.stop
      end
    end
  end
end
