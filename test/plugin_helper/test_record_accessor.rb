require_relative '../helper'
require 'fluent/plugin_helper/record_accessor'
require 'fluent/plugin/base'

require 'time'

class RecordAccessorHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :record_accessor
  end

  sub_test_case 'parse nested key expression' do
    data('normal' => 'key1',
         'space' => 'ke y2',
         'dot key' => 'this.is.key3')
    test 'parse single key' do |param|
      result = Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter(param)
      assert_equal param, result
    end

    test "nested bracket keys with dot" do
      result = Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter("$['key1']['this.is.key3']")
      assert_equal ['key1', 'this.is.key3'], result
    end

    data('dot' => '$.key1.key2[0]',
         'bracket' => "$['key1']['key2'][0]",
         'bracket w/ double quotes' => '$["key1"]["key2"][0]')
    test "nested keys ['key1', 'key2', 0]" do |param|
      result = Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter(param)
      assert_equal ['key1', 'key2', 0], result
    end

    data('bracket' => "$['key1'][0]['ke y2']",
         'bracket w/ double quotes' => '$["key1"][0]["ke y2"]')
    test "nested keys ['key1', 0, 'ke y2']" do |param|
      result = Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter(param)
      assert_equal ['key1', 0, 'ke y2'], result
    end

    data('dot' => '$.[0].key1.[1].key2',
         'bracket' => "$[0]['key1'][1]['key2']",
         'bracket w/ double quotes' => '$[0]["key1"][1]["key2"]')
    test "nested keys [0, 'key1', 1, 'key2']" do |param|
      result = Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter(param)
      assert_equal [0, 'key1', 1, 'key2'], result
    end

    data("missing ']'" => "$['key1'",
         "missing array index with dot" => "$.hello[]",
         "missing array index with braket" => "$[]",
         "more chars" => "$.key1[0]foo",
         "whitespace char included key in dot notation" => "$.key[0].ke y",
         "empty keys with dot" => "$.",
         "empty keys with bracket" => "$[",
         "mismatched quotes1" => "$['key1']['key2\"]",
         "mismatched quotes2" => '$["key1"]["key2\']')
    test 'invalid syntax' do |param|
      assert_raise Fluent::ConfigError do
        Fluent::PluginHelper::RecordAccessor::Accessor.parse_parameter(param)
      end
    end
  end

  sub_test_case 'attr_reader :keys' do
    setup do
      @d = Dummy.new
    end

    data('normal' => 'key1',
         'space' => 'ke y2',
         'dot key' => 'this.is.key3')
    test 'access single key' do |param|
      accessor = @d.record_accessor_create(param)
      assert_equal param, accessor.keys
    end

    test "nested bracket keys with dot" do
      accessor = @d.record_accessor_create("$['key1']['this.is.key3']")
      assert_equal ['key1','this.is.key3'], accessor.keys
    end

    data('dot' => '$.key1.key2[0]',
         'bracket' => "$['key1']['key2'][0]",
         'bracket w/ double quotes' => '$["key1"]["key2"][0]')
    test "nested keys ['key1', 'key2', 0]" do |param|
      accessor = @d.record_accessor_create(param)
      assert_equal ['key1', 'key2', 0], accessor.keys
    end

    data('bracket' => "$['key1'][0]['ke y2']",
         'bracket w/ double quotes' => '$["key1"][0]["ke y2"]')
    test "nested keys ['key1', 0, 'ke y2']" do |param|
      accessor = @d.record_accessor_create(param)
      assert_equal ['key1', 0, 'ke y2'], accessor.keys
    end
  end

  sub_test_case Fluent::PluginHelper::RecordAccessor::Accessor do
    setup do
      @d = Dummy.new
    end

    data('normal' => 'key1',
         'space' => 'ke y2',
         'dot key' => 'this.is.key3')
    test 'access single key' do |param|
      r = {'key1' => 'v1', 'ke y2' => 'v2', 'this.is.key3' => 'v3'}
      accessor = @d.record_accessor_create(param)
      assert_equal r[param], accessor.call(r)
    end

    test "access single dot key using bracket style" do
      r = {'key1' => 'v1', 'ke y2' => 'v2', 'this.is.key3' => 'v3'}
      accessor = @d.record_accessor_create('$["this.is.key3"]')
      assert_equal 'v3', accessor.call(r)
    end

    test "nested bracket keys with dot" do
      r = {'key1' => {'this.is.key3' => 'value'}}
      accessor = @d.record_accessor_create("$['key1']['this.is.key3']")
      assert_equal 'value', accessor.call(r)
    end

    data('dot' => '$.key1.key2[0]',
         'bracket' => "$['key1']['key2'][0]",
         'bracket w/ double quotes' => '$["key1"]["key2"][0]')
    test "nested keys ['key1', 'key2', 0]" do |param|
      r = {'key1' => {'key2' => [1, 2, 3]}}
      accessor = @d.record_accessor_create(param)
      assert_equal 1, accessor.call(r)
    end

    data('bracket' => "$['key1'][0]['ke y2']",
         'bracket w/ double quotes' => '$["key1"][0]["ke y2"]')
    test "nested keys ['key1', 0, 'ke y2']" do |param|
      r = {'key1' => [{'ke y2' => "value"}]}
      accessor = @d.record_accessor_create(param)
      assert_equal 'value', accessor.call(r)
    end

    data("missing ']'" => "$['key1'",
         "missing array index with dot" => "$.hello[]",
         "missing array index with braket" => "$['hello'][]",
         "whitespace char included key in dot notation" => "$.key[0].ke y",
         "more chars" => "$.key1[0]foo",
         "empty keys with dot" => "$.",
         "empty keys with bracket" => "$[",
         "mismatched quotes1" => "$['key1']['key2\"]",
         "mismatched quotes2" => '$["key1"]["key2\']')
    test 'invalid syntax' do |param|
      assert_raise Fluent::ConfigError do
        @d.record_accessor_create(param)
      end
    end
  end

  sub_test_case 'Fluent::PluginHelper::RecordAccessor::Accessor#delete' do
    setup do
      @d = Dummy.new
    end

    data('normal' => 'key1',
         'space' => 'ke y2',
         'dot key' => 'this.is.key3')
    test 'delete top key' do |param|
      r = {'key1' => 'v1', 'ke y2' => 'v2', 'this.is.key3' => 'v3'}
      accessor = @d.record_accessor_create(param)
      accessor.delete(r)
      assert_not_include(r, param)
    end

    test "delete top key using bracket style" do
      r = {'key1' => 'v1', 'ke y2' => 'v2', 'this.is.key3' => 'v3'}
      accessor = @d.record_accessor_create('$["this.is.key3"]')
      accessor.delete(r)
      assert_not_include(r, 'this.is.key3')
    end

    data('bracket' => "$['key1'][0]['ke y2']",
         'bracket w/ double quotes' => '$["key1"][0]["ke y2"]')
    test "delete nested keys ['key1', 0, 'ke y2']" do |param|
      r = {'key1' => [{'ke y2' => "value"}]}
      accessor = @d.record_accessor_create(param)
      accessor.delete(r)
      assert_not_include(r['key1'][0], 'ke y2')
    end

    test "don't raise an error when unexpected record is coming" do
      r = {'key1' => [{'key3' => "value"}]}
      accessor = @d.record_accessor_create("$['key1']['key2']['key3']")
      assert_nothing_raised do
        assert_nil accessor.delete(r)
      end
    end
  end
end
