require_relative '../helper'
require 'fluent/plugin/socket_util'

class SocketUtilTcpHandlerTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @server = TCPServer.new('127.0.0.1', 0)
    @client = TCPSocket.new('127.0.0.1', @server.addr[1])
    @io = @server.accept
  end

  def teardown
    [@io, @client, @server].compact.each(&:close)
  end

  data("delimiter split across reads" => ["\r\n", ["hello\r", "\nworld\r\n"], ["hello", "world"]],
       "delimiter split across three reads" => ["\r\n\r\n", ["a\r", "\n\r", "\n"], ["a"]],
       "many messages in one read" => ["\r\n", ["a\r\nb\r\nc", "c\r\n"], ["a", "b", "cc"]],
       "one byte per read" => ["\r\n", "ab\r\ncd\r\n".chars, ["ab", "cd"]],
       "partial delimiter in a message" => ["\r\n", ["a\r", "b\r", "\n"], ["a\rb"]])
  test "on_read splits data by the delimiter" do |(delimiter, reads, expected)|
    messages = []
    handler = Fluent::SocketUtil::TcpHandler.new(@io, $log, delimiter, ->(msg, _addr) { messages << msg })
    reads.each { |data| handler.on_read(data.b) }
    assert_equal(expected, messages)
  end
end
