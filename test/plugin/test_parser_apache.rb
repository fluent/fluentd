require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser_apache'

class ApacheParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = {})
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::ApacheParser.new).configure(conf)
  end

  data('parse' => :parse, 'call' => :call)
  def test_call(method_name)
    d = create_driver
    m = d.instance.method(method_name)
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
    conf = {
      'time_format' => "%d/%b/%Y:%H:%M:%S %z",
      'keep_time_key' => 'true',
    }
    d = create_driver(conf)
    text = '192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777'
    d.instance.parse(text) do |_time, record|
      assert_equal "28/Feb/2013:12:00:00 +0900", record['time']
    end
  end
end
