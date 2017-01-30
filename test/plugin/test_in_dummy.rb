require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_dummy'
require 'fileutils'

class DummyTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::DummyInput).configure(conf)
  end

  sub_test_case 'configure' do
    test 'required parameters' do
      assert_raise_message("'tag' parameter is required") do
        Fluent::Plugin::DummyInput.new.configure(config_element('ROOT',''))
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
      d.run(timeout: 0.5)

      d.events.each do |tag, time, record|
        assert_equal("dummy", tag)
        assert_equal({"foo"=>"bar"}, record)
        assert(time.is_a?(Fluent::EventTime))
      end
    end

    test 'with auto_increment_key' do
      d = create_driver(config + %[auto_increment_key id])
      d.run(timeout: 0.5)

      d.events.each_with_index do |(tag, _time, record), i|
        assert_equal("dummy", tag)
        assert_equal({"foo"=>"bar", "id"=>i}, record)
      end
    end
  end

  TEST_PLUGIN_STORAGE_PATH = File.join( File.dirname(File.dirname(__FILE__)), 'tmp', 'in_dummy', 'store' )
  FileUtils.mkdir_p TEST_PLUGIN_STORAGE_PATH

  sub_test_case "doesn't suspend internal counters in default" do
    config1 = {
      'tag' => 'dummy',
      'rate' => '2',
      'dummy' => '[{"x": 1, "y": "1"}, {"x": 2, "y": "2"}, {"x": 3, "y": "3"}]',
      'auto_increment_key' => 'id',
      'suspend' => false,
    }
    conf1 = config_element('ROOT', '', config1, [])
    test "value of auto increment key is not suspended after stop-and-start" do
      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))

      d1 = create_driver(conf1)
      d1.run(timeout: 0.5) do
        d1.instance.emit(4)
      end

      events = d1.events.sort{|a,b| a[2]['id'] <=> b[2]['id'] }

      first_id1 = events.first[2]['id']
      assert_equal 0, first_id1

      last_id1 = events.last[2]['id']
      assert { last_id1 > 0 }

      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))

      d2 = create_driver(conf1)
      d2.run(timeout: 0.5) do
        d2.instance.emit(4)
      end

      events = d2.events.sort{|a,b| a[2]['id'] <=> b[2]['id'] }

      first_id2 = events.first[2]['id']
      assert_equal 0, first_id2

      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))
    end
  end

  sub_test_case "suspend internal counters if suspend is true" do
    setup do
      FileUtils.rm_rf(TEST_PLUGIN_STORAGE_PATH)
      FileUtils.mkdir_p(File.join(TEST_PLUGIN_STORAGE_PATH, 'json'))
      FileUtils.chmod_R(0755, File.join(TEST_PLUGIN_STORAGE_PATH, 'json'))
    end

    config2 = {
      '@id' => 'test-02',
      'tag' => 'dummy',
      'rate' => '2',
      'dummy' => '[{"x": 1, "y": "1"}, {"x": 2, "y": "2"}, {"x": 3, "y": "3"}]',
      'auto_increment_key' => 'id',
      'suspend' => true,
    }
    conf2 = config_element('ROOT', '', config2, [
              config_element(
                'storage', '',
                {'@type' => 'local',
                 '@id' => 'test-02',
                 'path' => File.join(TEST_PLUGIN_STORAGE_PATH,
                                     'json', 'test-02.json'),
                 'persistent' => true,
                })
            ])
    test "value of auto increment key is suspended after stop-and-start" do
      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))

      d1 = create_driver(conf2)
      d1.run(timeout: 0.5) do
        d1.instance.emit(4)
      end

      first_id1 = d1.events.first[2]['id']
      assert_equal 0, first_id1

      last_id1 = d1.events.last[2]['id']
      assert { last_id1 > 0 }

      assert File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))

      d2 = create_driver(conf2)
      d2.run(timeout: 0.5) do
        d2.instance.emit(4)
      end
      d2.events

      first_id2 = d2.events.first[2]['id']
      assert_equal last_id1 + 1, first_id2

      assert File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))
    end
  end
end
