require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class ApacheParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin.new_parser('apache'))
  end

  data('parse' => :parse, 'call' => :call)
  def test_call(method_name)
    m = @parser.instance.method(method_name)
    m.call('192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777') { |time, record|
      assert_equal(event_time('28/Feb/2013:12:00:00 +0900', format: '%d/%b/%Y:%H:%M:%S %z'), time)
      assert_equal({
                     'user'    => '-',
                     'method'  => 'GET',
                     'code'    => '200',
                     'size'    => '777',
                     'host'    => '192.168.0.1',
                     'path'    => '/'
                   }, record)
    }
  end

  def test_parse_with_keep_time_key
    parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin.new_parser('apache'))
    parser.instance.configure(
                              'time_format'=>"%d/%b/%Y:%H:%M:%S %z",
                              'keep_time_key'=>'true',
                              )
    text = '192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777'
    parser.instance.parse(text) do |time, record|
      assert_equal "28/Feb/2013:12:00:00 +0900", record['time']
    end
  end
end
