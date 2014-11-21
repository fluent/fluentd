require_relative '../helper'
require 'fluent/test'

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
        create_driver(%[
          rate 10
          dummy {"foo":"bar"}
        ])
      end

      assert_raise_message("'rate' parameter is required") do
        create_driver(%[
          tag dummy
          dummy {"foo":"bar"}
        ])
      end

      assert_raise_message("'dummy' parameter is required") do
        create_driver(%[
          tag dummy
          rate 10
        ])
      end
    end

    test 'optional prameters' do
      config = %[
        tag dummy
        rate 10
        dummy {"foo":"bar"}
      ]
      d = create_driver(config + %[
        auto_increment_key id
      ])
      assert_equal "id", d.instance.auto_increment_key
    end

    test 'format of dummy' do
      config = %[
        tag dummy
        rate 10
      ]

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
    CONFIG = %[
      tag dummy
      rate 10
      dummy {"foo":"bar"}
    ]

    test 'simple' do
      d = create_driver(CONFIG)
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
      d = create_driver(CONFIG + %[auto_increment_key id])
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
