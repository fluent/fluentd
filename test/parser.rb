require 'helper'
require 'fluent/test'
require 'fluent/parser'

module ParserTest
  class LabeledTSVParserTest < Test::Unit::TestCase
    include Fluent

    def test_config_params
      parser = Fluent::TextParser::LabeledTSVParser.new

      assert_equal "\t", parser.delimiter
      assert_equal  ":", parser.label_delimiter

      parser.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )

      assert_equal ",", parser.delimiter
      assert_equal "=", parser.label_delimiter
    end

    def test_call
      parser = Fluent::TextParser::LabeledTSVParser.new
      time, record = parser.call("time:[28/Feb/2013:12:00:00 +0900]\thost:192.168.0.1\treq:GET /list HTTP/1.1")

      assert_equal({
        'time' => '[28/Feb/2013:12:00:00 +0900]',
        'host' => '192.168.0.1',
        'req'  => 'GET /list HTTP/1.1',
      }, record)
    end

    def test_call_with_customized_delimiter
      parser = Fluent::TextParser::LabeledTSVParser.new
      parser.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )
      time, record = parser.call('time=[28/Feb/2013:12:00:00 +0900],host=192.168.0.1,req=GET /list HTTP/1.1')

      assert_equal({
        'time' => '[28/Feb/2013:12:00:00 +0900]',
        'host' => '192.168.0.1',
        'req'  => 'GET /list HTTP/1.1',
      }, record)
    end
  end
end
