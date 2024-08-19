# frozen_string_literal: true

require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_sample'
require 'fileutils'

class SampleTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::SampleInput).configure(conf)
  end

  sub_test_case 'configure' do
    test 'required parameters' do
      assert_raise_message("'tag' parameter is required") do
        Fluent::Plugin::SampleInput.new.configure(config_element('ROOT',''))
      end
    end

    test 'tag' do
      d = create_driver(%[
        tag sample
      ])
      assert_equal "sample", d.instance.tag
    end

    config = %[
      tag sample
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

    test 'sample' do
      # hash is okay
      d = create_driver(config + %[sample {"foo":"bar"}])
      assert_equal [{"foo"=>"bar"}], d.instance.sample

      # array of hash is okay
      d = create_driver(config + %[sample [{"foo":"bar"}]])
      assert_equal [{"foo"=>"bar"}], d.instance.sample

      assert_raise_message(/JSON::ParserError|got incomplete JSON/) do
        create_driver(config + %[sample "foo"])
      end

      assert_raise_message(/is not a hash/) do
        create_driver(config + %[sample ["foo"]])
      end
    end
  end

  sub_test_case "emit" do
    config = %[
      tag sample
      rate 10
      sample {"foo":"bar"}
    ]

    test 'simple' do
      d = create_driver(config)
      d.run(timeout: 0.5)

      d.events.each do |tag, time, record|
        assert_equal("sample", tag)
        assert_equal({"foo"=>"bar"}, record)
        assert(time.is_a?(Fluent::EventTime))
      end
    end

    test 'with auto_increment_key' do
      d = create_driver(config + %[auto_increment_key id])
      d.run(timeout: 0.5)

      d.events.each_with_index do |(tag, _time, record), i|
        assert_equal("sample", tag)
        assert_equal({"foo"=>"bar", "id"=>i}, record)
      end
    end
  end

  TEST_PLUGIN_STORAGE_PATH = File.join( File.dirname(File.dirname(__FILE__)), 'tmp', 'in_sample', 'store' )
  FileUtils.mkdir_p TEST_PLUGIN_STORAGE_PATH

  sub_test_case 'when sample plugin has storage which is not specified the path'  do
    config1 = {
      'tag' => 'sample',
      'rate' => '0',
      'sample' => '[{"x": 1, "y": "1"}, {"x": 2, "y": "2"}, {"x": 3, "y": "3"}]',
      'auto_increment_key' => 'id',
    }
    conf1 = config_element('ROOT', '', config1, [])
    test "value of auto increment key is not kept after stop-and-start" do
      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))

      d1 = create_driver(conf1)
      d1.run(timeout: 0.5) do
        d1.instance.emit(2)
      end

      events = d1.events.sort{|a,b| a[2]['id'] <=> b[2]['id'] }

      first_id1 = events.first[2]['id']
      assert_equal 0, first_id1

      last_id1 = events.last[2]['id']
      assert { last_id1 > 0 }

      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))

      d2 = create_driver(conf1)
      d2.run(timeout: 0.5) do
        d2.instance.emit(2)
      end

      events = d2.events.sort{|a,b| a[2]['id'] <=> b[2]['id'] }

      first_id2 = events.first[2]['id']
      assert_equal 0, first_id2

      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-01.json'))
    end
  end

  sub_test_case 'when sample plugin has storage which is specified the path'  do
    setup do
      FileUtils.rm_rf(TEST_PLUGIN_STORAGE_PATH)
      FileUtils.mkdir_p(File.join(TEST_PLUGIN_STORAGE_PATH, 'json'))
      FileUtils.chmod_R(0755, File.join(TEST_PLUGIN_STORAGE_PATH, 'json'))
    end

    config2 = {
      '@id' => 'test-02',
      'tag' => 'sample',
      'rate' => '0',
      'sample' => '[{"x": 1, "y": "1"}, {"x": 2, "y": "2"}, {"x": 3, "y": "3"}]',
      'auto_increment_key' => 'id',
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
    test "value of auto increment key is kept after stop-and-start" do
      assert !File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))

      d1 = create_driver(conf2)
      d1.run(timeout: 1) do
        d1.instance.emit(2)
      end

      first_id1 = d1.events.first[2]['id']
      assert_equal 0, first_id1

      last_id1 = d1.events.last[2]['id']
      assert { last_id1 > 0 }

      assert File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))

      d2 = create_driver(conf2)
      d2.run(timeout: 1) do
        d2.instance.emit(2)
      end
      d2.events

      first_id2 = d2.events.first[2]['id']
      assert_equal last_id1 + 1, first_id2

      assert File.exist?(File.join(TEST_PLUGIN_STORAGE_PATH, 'json', 'test-02.json'))
    end
  end
end
