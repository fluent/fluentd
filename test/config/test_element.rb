require_relative '../helper'
require 'fluent/config/element'
require 'fluent/config/configure_proxy'
require 'fluent/configurable'
require 'pp'

class TestConfigElement < ::Test::Unit::TestCase
  def element(name = 'ROOT', arg = '', attrs = {}, elements = [], unused = nil)
    Fluent::Config::Element.new(name, arg, attrs, elements, unused)
  end

  sub_test_case '#elements=' do
    test 'elements can be set by others' do
      e = element()
      assert_equal [], e.elements

      e1 = element('e1')
      e2 = element('e2')
      e.elements = [e1, e2]
      assert_equal [e1, e2], e.elements
    end
  end

  sub_test_case '#elements' do
    setup do
      @c1 = element('source')
      @c2 = element('source', 'yay')
      @c3 = element('match', 'test.**')
      @c4 = element('label', '@mytest', {}, [ element('filter', '**'), element('match', '**') ])
      children = [@c1, @c2, @c3, @c4]
      @e = Fluent::Config::Element.new('ROOT', '', {}, children)
    end

    test 'returns all elements without arguments' do
      assert_equal [@c1, @c2, @c3, @c4], @e.elements
    end

    test 'returns elements with specified names' do
      assert_equal [@c1, @c2, @c3], @e.elements('source', 'match')
      assert_equal [@c3, @c4], @e.elements('match', 'label')
    end

    test 'returns elements with specified name, and arg if specified' do
      assert_equal [@c1, @c2], @e.elements(name: 'source')
      assert_equal [@c2], @e.elements(name: 'source', arg: 'yay')
    end

    test 'keyword argument name/arg and names are exclusive' do
      assert_raise ArgumentError do
        @e.elements('source', name: 'match')
      end
      assert_raise ArgumentError do
        @e.elements('source', 'match', name: 'label', arg: '@mytest')
      end
    end

    test 'specifying only arg without name is invalid' do
      assert_raise ArgumentError do
        @e.elements(arg: '@mytest')
      end
    end
  end

  sub_test_case '#initialize' do
    test 'creates object with blank attrs and elements' do
      e = element('ROOT', '', {}, [])
      assert_equal([], e.elements)
    end

    test 'creates object which contains attrs and elements' do
      e = element('ROOT', '', {"k1" => "v1", "k2" => "v2"}, [
                    element('test', 'mydata', {'k3' => 'v3'}, [])
                  ])
      assert_equal('ROOT', e.name)
      assert_equal('',  e.arg)
      assert_equal('v1', e["k1"])
      assert_equal('v2', e["k2"])
      assert_equal(1, e.elements.size)
      e.each_element('test') do |el|
        assert_equal('test', el.name)
        assert_equal('mydata', el.arg)
        assert_equal('v3', el["k3"])
      end
    end

    test 'creates object which contains attrs, elements and unused' do
      e = element('ROOT', '', {"k1" => "v1", "k2" => "v2", "k4" => "v4"}, [
                    element('test', 'mydata', {'k3' => 'v3'}, [])
                  ], "k3")
      assert_equal("k3", e.unused)
      assert_equal('ROOT', e.name)
      assert_equal('',  e.arg)
      assert_equal('v1', e["k1"])
      assert_equal('v2', e["k2"])
      assert_equal('v4', e["k4"])
      assert_equal(1, e.elements.size)
      e.each_element('test') do |el|
        assert_equal('test', el.name)
        assert_equal('mydata', el.arg)
        assert_equal('v3', el["k3"])
      end
      assert_equal("k3", e.unused)
    end
  end

  sub_test_case "@unused" do
    sub_test_case '#[] has side effect for @unused' do
      test 'without unused argument' do
        e = element('ROOT', '', {"k1" => "v1", "k2" => "v2", "k4" => "v4"}, [
                      element('test', 'mydata', {'k3' => 'v3'}, [])
                    ])
        assert_equal(["k1", "k2", "k4"], e.unused)
        assert_equal('v1', e["k1"])
        assert_equal(["k2", "k4"], e.unused)
        assert_equal('v2', e["k2"])
        assert_equal(["k4"], e.unused)
        assert_equal('v4', e["k4"])
        assert_equal([], e.unused)
      end

      test 'with unused argument' do
        e = element('ROOT', '', {"k1" => "v1", "k2" => "v2", "k4" => "v4"}, [
                      element('test', 'mydata', {'k3' => 'v3'}, [])
                    ], ["k4"])
        assert_equal(["k4"], e.unused)
        assert_equal('v1', e["k1"])
        assert_equal(["k4"], e.unused)
        assert_equal('v2', e["k2"])
        assert_equal(["k4"], e.unused)
        assert_equal('v4', e["k4"])
        # only consume for "k4"
        assert_equal([], e.unused)
      end
    end
  end

  sub_test_case '#add_element' do
    test 'elements can be set by #add_element' do
      e = element()
      assert_equal [], e.elements

      e.add_element('e1', '')
      e.add_element('e2', '')
      assert_equal [element('e1', ''), element('e2', '')], e.elements
    end
  end

  sub_test_case '#==' do
    sub_test_case 'compare with two element objects' do
      test 'equal' do
        e1 = element('ROOT', '', {}, [])
        e2 = element('ROOT', '', {}, [])
        assert_true(e1 == e2)
      end

      data("differ args" => [Fluent::Config::Element.new('ROOT', '', {}, []),
                             Fluent::Config::Element.new('ROOT', 'mydata', {}, [])],
           "differ keys" => [Fluent::Config::Element.new('ROOT', 'mydata', {}, []),
                             Fluent::Config::Element.new('ROOT', 'mydata', {"k1" => "v1"}, [])],
           "differ elemnts" =>
           [Fluent::Config::Element.new('ROOT', 'mydata', {"k1" => "v1"}, []),
            Fluent::Config::Element.new('ROOT', 'mydata', {"k1" => "v1"}, [
              Fluent::Config::Element.new('test', 'mydata', {'k3' => 'v3'}, [])
            ])])
      test 'not equal' do |data|
        e1, e2 = data
        assert_false(e1 == e2)
      end
    end
  end

  sub_test_case '#+' do
    test 'can merge 2 elements: object side is primary' do
      e1 = element('ROOT', 'mydata', {"k1" => "v1"}, [])
      e2 = element('ROOT', 'mydata2', {"k1" => "ignored", "k2" => "v2"}, [
                     element('test', 'ext', {'k3' => 'v3'}, [])
                   ])
      e = e1 + e2

      assert_equal('ROOT', e.name)
      assert_equal('mydata', e.arg)
      assert_equal('v1', e['k1'])
      assert_equal('v2', e['k2'])
      assert_equal(1, e.elements.size)
      e.each_element('test') do |el|
        assert_equal('test', el.name)
        assert_equal('ext', el.arg)
        assert_equal('v3', el["k3"])
      end
    end
  end

  sub_test_case '#check_not_fetched' do
    sub_test_case 'without unused' do
      test 'can get attribute keys and original Config::Element' do
        e = element('ROOT', 'mydata', {"k1" => "v1"}, [])
        e.check_not_fetched { |key, elem|
          assert_equal("k1", key)
          assert_equal(e, elem)
        }
      end
    end

    sub_test_case 'with unused' do
      test 'can get unused marked attribute keys and original Config::Element' do
        e = element('ROOT', 'mydata', {"k1" => "v1", "k2" => "unused", "k3" => "k3"})
        e.unused = "k2"
        e.check_not_fetched { |key, elem|
          assert_equal("k2", key)
          assert_equal(e, elem)
        }
      end
    end
  end

  sub_test_case '#has_key?' do
    test 'can get boolean with key name' do
      e = element('ROOT', 'mydata', {"k1" => "v1"}, [])
      assert_true(e.has_key?("k1"))
      assert_false(e.has_key?("noexistent"))
    end
  end


  sub_test_case '#to_s' do
    data("without v1_config" => [false, <<-CONF
<ROOT>
  k1 v1
  k2 "stringVal"
  <test ext>
    k2 v2
  </test>
</ROOT>
CONF
                                ],
         "with v1_config" => [true, <<-CONF
<ROOT>
  k1 v1
  k2 "stringVal"
  <test ext>
    k2 v2
  </test>
</ROOT>
CONF
                             ],
        )
    test 'dump config element with #to_s' do |data|
      v1_config, expected = data
      e = element('ROOT', '', {'k1' => 'v1', "k2" =>"\"stringVal\""}, [
                    element('test', 'ext', {'k2' => 'v2'}, [])
                  ])
      e.v1_config = v1_config
      dump = expected
      assert_not_equal(e.inspect, e.to_s)
      assert_equal(dump, e.to_s)
    end
  end

  sub_test_case '#inspect' do
    test 'dump config element with #inspect' do
      e = element('ROOT', '', {'k1' => 'v1'}, [
                   element('test', 'ext', {'k2' => 'v2'}, [])
                  ])
      dump = <<-CONF
name:ROOT, arg:, {\"k1\"=>\"v1\"}, [name:test, arg:ext, {\"k2\"=>\"v2\"}, []]
CONF
      assert_not_equal(e.to_s, e.inspect)
      assert_equal(dump.chomp, e.inspect)
    end
  end

  sub_test_case 'for sections which has secret parameter' do
    setup do
      @type_lookup = ->(type){ Fluent::Configurable.lookup_type(type) }
      p1 = Fluent::Config::ConfigureProxy.new(:match, type_lookup: @type_lookup)
      p1.config_param :str1, :string
      p1.config_param :str2, :string, secret: true
      p1.config_param :enum1, :enum, list: [:a, :b, :c]
      p1.config_param :enum2, :enum, list: [:a, :b, :c], secret: true
      p1.config_param :bool1, :bool
      p1.config_param :bool2, :bool, secret: true
      p1.config_param :int1, :integer
      p1.config_param :int2, :integer, secret: true
      p1.config_param :float1, :float
      p1.config_param :float2, :float, secret: true
      p2 = Fluent::Config::ConfigureProxy.new(:match, type_lookup: @type_lookup)
      p2.config_param :size1, :size
      p2.config_param :size2, :size, secret: true
      p2.config_param :time1, :time
      p2.config_param :time2, :time, secret: true
      p2.config_param :array1, :array
      p2.config_param :array2, :array, secret: true
      p2.config_param :hash1, :hash
      p2.config_param :hash2, :hash, secret: true
      p1.config_section :mysection do
        config_param :str1, :string
        config_param :str2, :string, secret: true
        config_param :enum1, :enum, list: [:a, :b, :c]
        config_param :enum2, :enum, list: [:a, :b, :c], secret: true
        config_param :bool1, :bool
        config_param :bool2, :bool, secret: true
        config_param :int1, :integer
        config_param :int2, :integer, secret: true
        config_param :float1, :float
        config_param :float2, :float, secret: true
        config_param :size1, :size
        config_param :size2, :size, secret: true
        config_param :time1, :time
        config_param :time2, :time, secret: true
        config_param :array1, :array
        config_param :array2, :array, secret: true
        config_param :hash1, :hash
        config_param :hash2, :hash, secret: true
      end
      params = {
        'str1' => 'aaa', 'str2' => 'bbb', 'enum1' => 'a', 'enum2' => 'b', 'bool1' => 'true', 'bool2' => 'yes',
        'int1' => '1', 'int2' => '2', 'float1' => '1.0', 'float2' => '0.5', 'size1' => '1k', 'size2' => '1m',
        'time1' => '5m', 'time2' => '3h', 'array1' => 'a,b,c', 'array2' => 'd,e,f',
        'hash1' => 'a:1,b:2', 'hash2' => 'a:2,b:4',
        'unknown1' => 'yay', 'unknown2' => 'boo',
      }
      e2 = Fluent::Config::Element.new('mysection', '', params.dup, [])
      e2.corresponding_proxies << p1.sections.values.first
      @e = Fluent::Config::Element.new('match', '**', params, [e2])
      @e.corresponding_proxies << p1
      @e.corresponding_proxies << p2
    end

    sub_test_case '#to_masked_element' do
      test 'returns a new element object which has masked values for secret parameters and elements' do
        e = @e.to_masked_element
        assert_equal 'aaa',    e['str1']
        assert_equal 'xxxxxx', e['str2']
        assert_equal 'a',      e['enum1']
        assert_equal 'xxxxxx', e['enum2']
        assert_equal 'true',   e['bool1']
        assert_equal 'xxxxxx', e['bool2']
        assert_equal '1',      e['int1']
        assert_equal 'xxxxxx', e['int2']
        assert_equal '1.0',    e['float1']
        assert_equal 'xxxxxx', e['float2']
        assert_equal '1k',     e['size1']
        assert_equal 'xxxxxx', e['size2']
        assert_equal '5m',     e['time1']
        assert_equal 'xxxxxx', e['time2']
        assert_equal 'a,b,c',  e['array1']
        assert_equal 'xxxxxx', e['array2']
        assert_equal 'a:1,b:2', e['hash1']
        assert_equal 'xxxxxx',  e['hash2']
        assert_equal 'yay', e['unknown1']
        assert_equal 'boo', e['unknown2']
        e2 = e.elements.first
        assert_equal 'aaa',    e2['str1']
        assert_equal 'xxxxxx', e2['str2']
        assert_equal 'a',      e2['enum1']
        assert_equal 'xxxxxx', e2['enum2']
        assert_equal 'true',   e2['bool1']
        assert_equal 'xxxxxx', e2['bool2']
        assert_equal '1',      e2['int1']
        assert_equal 'xxxxxx', e2['int2']
        assert_equal '1.0',    e2['float1']
        assert_equal 'xxxxxx', e2['float2']
        assert_equal '1k',     e2['size1']
        assert_equal 'xxxxxx', e2['size2']
        assert_equal '5m',     e2['time1']
        assert_equal 'xxxxxx', e2['time2']
        assert_equal 'a,b,c',  e2['array1']
        assert_equal 'xxxxxx', e2['array2']
        assert_equal 'a:1,b:2', e2['hash1']
        assert_equal 'xxxxxx',  e2['hash2']
        assert_equal 'yay', e2['unknown1']
        assert_equal 'boo', e2['unknown2']
      end
    end

    sub_test_case '#secret_param?' do
      test 'returns boolean which shows values of given key will be masked' do
        assert !@e.secret_param?('str1')
        assert @e.secret_param?('str2')
        assert !@e.elements.first.secret_param?('str1')
        assert @e.elements.first.secret_param?('str2')
      end
    end

    sub_test_case '#param_type' do
      test 'returns parameter type which are registered in corresponding proxy' do
        assert_equal :string, @e.param_type('str1')
        assert_equal :string, @e.param_type('str2')
        assert_equal :enum, @e.param_type('enum1')
        assert_equal :enum, @e.param_type('enum2')
        assert_nil @e.param_type('unknown1')
        assert_nil @e.param_type('unknown2')
      end
    end

    # sub_test_case '#dump_value'
    sub_test_case '#dump_value' do
      test 'dumps parameter_name and values with leading indentation' do
        assert_equal "str1 aaa\n", @e.dump_value("str1", @e["str1"], "")
        assert_equal "str2 xxxxxx\n", @e.dump_value("str2", @e["str2"], "")
      end
    end
  end

  sub_test_case '#set_target_worker' do
    test 'set target_worker_id recursively' do
      e = element('label', '@mytest', {}, [ element('filter', '**'), element('match', '**', {}, [ element('store'), element('store') ]) ])
      e.set_target_worker_id(1)
      assert_equal 1, e.target_worker_id
      assert_equal 1, e.elements[0].target_worker_id
      assert_equal 1, e.elements[1].target_worker_id
      assert_equal 1, e.elements[1].elements[0].target_worker_id
      assert_equal 1, e.elements[1].elements[1].target_worker_id
    end
  end

  sub_test_case '#for_every_workers?' do
    test 'has target_worker_id' do
      e = element()
      e.set_target_worker_id(1)
      assert_false e.for_every_workers?
    end

    test "doesn't have target_worker_id" do
      e = element()
      assert e.for_every_workers?
    end
  end

  sub_test_case '#for_this_workers?' do
    test 'target_worker_id == current worker_id' do
      e = element()
      e.set_target_worker_id(0)
      assert e.for_this_worker?
    end

    test 'target_worker_id != current worker_id' do
      e = element()
      e.set_target_worker_id(1)
      assert_false e.for_this_worker?
    end

    test "doesn't have target_worker_id" do
      e = element()
      assert_false e.for_this_worker?
    end
  end

  sub_test_case '#for_another_worker?' do
    test 'target_worker_id == current worker_id' do
      e = element()
      e.set_target_worker_id(0)
      assert_false e.for_another_worker?
    end

    test 'target_worker_id != current worker_id' do
      e = element()
      e.set_target_worker_id(1)
      assert e.for_another_worker?
    end

    test "doesn't have target_worker_id" do
      e = element()
      assert_false e.for_another_worker?
    end
  end

  sub_test_case '#pretty_print' do
    test 'prints inspect to pp object' do
      q = PP.new
      e = element()
      e.pretty_print(q)
      assert_equal e.inspect, q.output
    end
  end
end
