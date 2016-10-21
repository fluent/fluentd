require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

module ParserTest
  class BaseParserTest < ::Test::Unit::TestCase
    def setup
      Fluent::Test.setup
    end

    def create_driver(conf={})
      Fluent::Test::Driver::Parser.new(Fluent::Plugin::Parser).configure(conf)
    end

    def test_init
      d = create_driver
      assert_true d.instance.estimate_current_event
    end

    def test_configure_against_string_literal
      d = create_driver('keep_time_key true')
      assert_true d.instance.keep_time_key
    end

    def test_parse
      d = create_driver
      assert_raise NotImplementedError do
        d.instance.parse('')
      end
    end
  end
end
