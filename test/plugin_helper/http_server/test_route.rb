require_relative '../../helper'
require 'flexmock/test_unit'

begin
  require 'fluent/plugin_helper/http_server/router'
  skip = false
rescue LoadError => _
  skip = true
end

unless skip
  class HttpHelperRouterTest < Test::Unit::TestCase
    sub_test_case '#mount' do
      test 'mount with method and path' do
        router = Fluent::PluginHelper::HttpServer::Router.new
        router.mount(:get, '/path/', ->(req) { req })
        assert_equal(router.route!(:get, '/path/', 'request'), 'request')
      end

      test 'use default app if path is not found' do
        router = Fluent::PluginHelper::HttpServer::Router.new
        req = flexmock('request', path: 'path/')
        assert_equal(router.route!(:get, '/path/', req), [404, { 'Content-Type' => 'text/plain' }, "404 Not Found\n"])
      end

      test 'default app is configurable' do
        router = Fluent::PluginHelper::HttpServer::Router.new(->(req) { req })
        assert_equal(router.route!(:get, '/path/', 'hello'), 'hello')
      end
    end
  end
end
