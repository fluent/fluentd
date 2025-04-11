require_relative '../../helper'
require 'flexmock/test_unit'
require 'fluent/plugin_helper/http_server/request'

class HttpHelperRequestTest < Test::Unit::TestCase
  def test_request
    headers = Protocol::HTTP::Headers.new({ 'Content-Type' => 'text/html', 'Content-Encoding' => 'gzip' })
    req = flexmock('request', path: '/path?foo=42', headers: headers)

    request = Fluent::PluginHelper::HttpServer::Request.new(req)

    assert_equal('/path', request.path)
    assert_equal('foo=42', request.query_string)
    assert_equal({'foo'=>['42']}, request.query)
    assert_equal('text/html', request.headers['content-type'])
    assert_equal(['gzip'], request.headers['content-encoding'])
  end
end
