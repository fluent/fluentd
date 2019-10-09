require_relative '../../helper'
require 'flexmock/test_unit'

begin
  require 'fluent/plugin_helper/http_server/app'
  skip = false
rescue LoadError => _
  skip = true
end

unless skip
  class HtttpHelperAppTest < Test::Unit::TestCase
    NULL_LOGGER = Logger.new(nil)

    class DummyRounter
      def initialize(table = {})
        @table = table
      end

      def route!(method, path, _req)
        r = @table.fetch(method).fetch(path)
        [200, {}, r]
      end
    end

    sub_test_case '#call' do
      data(
        'GET request' => 'GET',
        'POST request' => 'POST',
        'DELETE request' => 'DELETE',
        'PUT request' => 'PUT',
        'PATCH request' => 'PATCH',
        'OPTION request' => 'OPTIONS',
        'CONNECT request' => 'CONNECT',
        'TRACE request' => 'TRACE',
      )
      test 'dispatch correct path' do |method|
        r = DummyRounter.new(method.downcase.to_sym => { '/path/' => 'hi' })
        app = Fluent::PluginHelper::HttpServer::App.new(r, NULL_LOGGER)
        m = flexmock('request', method: method, path: '/path/')
        r = app.call(m)
        assert_equal(r.body.read, 'hi')
        assert_equal(r.status, 200)
      end

      test 'dispatch correct path for head' do |method|
        r = DummyRounter.new(head: { '/path/' => 'hi' })
        app = Fluent::PluginHelper::HttpServer::App.new(r, NULL_LOGGER)
        m = flexmock('request', method: method, path: '/path')
        r = app.call(m)
        assert_equal(r.body.read, '')
        assert_equal(r.status, 200)
      end

      test 'if path does not end with `/`' do |method|
        r = DummyRounter.new(head: { '/path/' => 'hi' })
        app = Fluent::PluginHelper::HttpServer::App.new(r, NULL_LOGGER)
        m = flexmock('request', method: method, path: '/path')
        r = app.call(m)
        assert_equal(r.body.read, '')
        assert_equal(r.status, 200)
      end
    end
  end
end
