require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_dummy'

class DummyTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::InputTestDriver.new(Fluent::DummyInput).configure(conf)
  end

  sub_test_case 'configure' do
    test 'required parameters' do
      assert_raise_message("'tag' parameter is required") do
        create_driver('')
      end
    end

    test 'tag' do
      d = create_driver(%[
        tag dummy
      ])
      assert_equal "dummy", d.instance.tag
    end

    config = %[
      tag dummy
    ]

    test 'auto_increment_key' do
      d = create_driver(config + %[
        auto_increment_key id
      ])
      assert_equal "id", d.instance.auto_increment_key
    end

    test 'rate' do
      d = create_driver(config + %[
        rate 10
      ])
      assert_equal 10, d.instance.rate
    end

    test 'dummy' do
      # hash is okay
      d = create_driver(config + %[dummy {"foo":"bar"}])
      assert_equal [{"foo"=>"bar"}], d.instance.dummy

      # array of hash is okay
      d = create_driver(config + %[dummy [{"foo":"bar"}]])
      assert_equal [{"foo"=>"bar"}], d.instance.dummy

      assert_raise_message(/JSON::ParserError|got incomplete JSON/) do
        create_driver(config + %[dummy "foo"])
      end

      assert_raise_message(/is not a hash/) do
        create_driver(config + %[dummy ["foo"]])
      end
    end
  end

  sub_test_case "emit" do
    config = %[
      tag dummy
      rate 10
      dummy {"foo":"bar"}
    ]

    test 'simple' do
      d = create_driver(config)
      d.run {
        # d.run sleeps 0.5 sec
      }
      emits = d.emits
      emits.each do |tag, time, record|
        assert_equal("dummy", tag)
        assert_equal({"foo"=>"bar"}, record)
      end
    end

    test 'with auto_increment_key' do
      d = create_driver(config + %[auto_increment_key id])
      d.run {
        # d.run sleeps 0.5 sec
      }
      emits = d.emits
      emits.each_with_index do |(tag, time, record), i|
        assert_equal("dummy", tag)
        assert_equal({"foo"=>"bar", "id"=>i}, record)
      end
    end
  end
end
