require_relative '../helper'
require 'fluent/test'
require 'fluent/storage/json'

require 'fileutils'

module Fluent::Storage
  class JSONStorageTest < Test::Unit::TestCase
    sub_test_case Fluent::Storage::JSON do
      setup do
        @storage_path = File.join(File.dirname(File.dirname(__FILE__)), 'tmp', 'storage')
        FileUtils.rm_rf(@storage_path, secure: true) if Dir.exist?(@storage_path)
        FileUtils.mkdir_p(@storage_path)

        @storage = Fluent::Storage::JSON.new
      end

      sub_test_case '#put' do
        test "#put handle keys both symbols and strings as same" do
          obj = "data1"
          @storage.put(:key1, obj)
          assert_equal obj.object_id, @storage.instance_eval{ @store[:key1] }.object_id

          @storage.put("key1", obj)
          assert_equal obj.object_id, @storage.instance_eval{ @store[:key1] }.object_id
        end
      end

      sub_test_case '#get' do
        test "return nil for unknown keys" do
          assert_nil @storage.get(:key1)

          @storage.put(:key1, "data1")
          assert_equal "data1", @storage.get(:key1)

          assert_nil @storage.get(:key2)
        end

        test "get handle keys both symbols and strings as same" do
          obj = "data1"
          @storage.put(:key1, obj)
          assert_equal obj.object_id, @storage.get(:key1).object_id
          assert_equal obj.object_id, @storage.get("key1").object_id

          obj = "data2"
          @storage.put("key1", obj)
          assert_equal obj.object_id, @storage.get(:key1).object_id
          assert_equal obj.object_id, @storage.get("key1").object_id
        end
      end

      sub_test_case '#fetch' do
        test "fetch works as well as Hash#fetch" do
          assert_equal "data", @storage.fetch(:key1, "data")

          @storage.put(:key1, "data1")
          assert_equal "data1", @storage.fetch(:key1, "data")
        end
      end

      def e(name, arg = '', attrs = {}, elements = [])
        attrs_str_keys = {}
        attrs.each{|key, value| attrs_str_keys[key.to_s] = value }
        Fluent::Config::Element.new(name, arg, attrs_str_keys, elements)
      end

      sub_test_case '#save' do
        test "stored data is saved as plain-text JSON data on the disk" do
          test_path = File.join(@storage_path, 'save_test_1.json')
          assert !(File.exist?(test_path))

          @storage.configure(e('ROOT', '', {}, [e('storage', '', {'path' => test_path, 'pretty_print' => 'false'}, [])]))
          assert_equal test_path, @storage.storage.path
          assert_equal test_path + '.tmp', @storage.instance_eval{ @tmp_path }
          assert_equal false, @storage.storage.pretty_print

          assert_equal({}, @storage.instance_eval{ @store })

          t = Time.now

          @storage.put(:key1, 'data1')
          @storage.put(:key2, {k1: 'v1', k2: 'v2', k3: [1,2,3]})
          @storage.put(:key3, ['a', 'b', 'c', 'd'])
          @storage.put(:key4, t.to_i)

          @storage.save

          assert !(File.exist?(test_path + '.tmp'))
          assert File.exist?(test_path)

          content = open(test_path){|f| ::JSON.parse(f.read, symbolize_names: true)}
          expected = {key1: 'data1', key2: {k1: 'v1', k2: 'v2', k3: [1,2,3]}, key3: ['a', 'b', 'c', 'd'], key4: t.to_i}
          assert_equal expected, content
        end
      end

      sub_test_case '#load' do
        test "saved data is successfully loaded with symbolized keys" do
          test_path = File.join(@storage_path, 'load_test_1.json')
          assert !(File.exist?(test_path))

          t = Time.now

          expected = {key1: 'data1', key2: {k1: 'v1', k2: 'v2', k3: [1,2,3]}, key3: ['a', 'b', 'c', 'd'], key4: t.to_i}
          open(test_path, 'w:utf-8') do |f|
            f.write expected.to_json
          end
          assert File.exist?(test_path)

          @storage.configure(e('ROOT', '', {}, [e('storage', '', {'path' => test_path, 'pretty_print' => 'false'}, [])]))
          @storage.load

          assert_equal expected, @storage.instance_eval{ @store }

          assert_equal 'data1', @storage.get(:key1)
          assert_equal({k1: 'v1', k2: 'v2', k3: [1,2,3]}, @storage.get(:key2))
          assert_equal ['a', 'b', 'c', 'd'], @storage.get(:key3)
          assert_equal t.to_i, @storage.get(:key4)
        end
      end
    end
  end
end
