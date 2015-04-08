require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_dummy'
require 'fileutils'

class DummyTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf, system_opts={})
    Fluent::Test::Driver::Input.new(Fluent::Plugin::DummyInput, system_opts).configure(conf)
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
      d.expected_emits_length = 5
      d.run

      emits = d.emits
      emits.each do |tag, time, record|
        assert_equal("dummy", tag)
        assert_equal({"foo"=>"bar"}, record)
      end
    end

    test 'with auto_increment_key' do
      d = create_driver(config + %[auto_increment_key id])
      d.expected_emits_length = 5
      d.run

      emits = d.emits
      emits.each_with_index do |(tag, time, record), i|
        assert_equal("dummy", tag)
        assert_equal({"foo"=>"bar", "id"=>i}, record)
      end
    end
  end

  TEST_PLUGIN_STORAGE_PATH = File.join( File.dirname(File.dirname(__FILE__)), 'tmp', 'in_dummy', 'store' )
  FileUtils.mkdir_p TEST_PLUGIN_STORAGE_PATH

  sub_test_case "doesn't suspend internal counters in default" do
    config1 = %[
      @id test-01
      tag dummy
      rate 10
      dummy [{"x": 1, "y": "1"}, {"x": 2, "y": "2"}, {"x": 3, "y": "3"}]
      auto_increment_key id
    ]
    test "value of auto increment key is not suspended after stop-and-start" do
      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))

      d1 = create_driver(config1, plugin_storage_path: TEST_PLUGIN_STORAGE_PATH)
      d1.expected_emits_length = 4
      d1.run

      first_id1 = d1.emits.first[2]['id']
      assert_equal 0, first_id1

      last_id1 = d1.emits.last[2]['id']
      assert { last_id1 > 0 }

      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))

      d2 = create_driver(config1, plugin_storage_path: TEST_PLUGIN_STORAGE_PATH)
      d2.expected_emits_length = 4
      d2.run

      first_id2 = d2.emits.first[2]['id']
      assert_equal 0, first_id2

      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))
    end
  end

  sub_test_case "suspend internal counters if suspend is true" do
    setup do
      FileUtils.rm_rf(TEST_PLUGIN_STORAGE_PATH)
    end

    config2 = %[
      @id test-02
      tag dummy
      rate 2
      dummy [{"x": 1, "y": "1"}, {"x": 2, "y": "2"}, {"x": 3, "y": "3"}]
      auto_increment_key id
      suspend true
    ]
    test "value of auto increment key is suspended after stop-and-start" do
      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))

      d1 = create_driver(config2, plugin_storage_path: TEST_PLUGIN_STORAGE_PATH)

      d1.expected_emits_length = 4
      d1.run

      first_id1 = d1.emits.first[2]['id']
      assert_equal 0, first_id1

      last_id1 = d1.emits.last[2]['id']
      assert { last_id1 > 0 }

      assert File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))

      d2 = create_driver(config2, plugin_storage_path: TEST_PLUGIN_STORAGE_PATH)
      d2.expected_emits_length = 4
      d2.run

      first_id2 = d2.emits.first[2]['id']
      assert_equal last_id1 + 1, first_id2

      assert File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))
    end
  end
end
