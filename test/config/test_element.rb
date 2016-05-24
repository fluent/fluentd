require_relative '../helper'
require 'fluent/config/element'

class TestConfigElement < ::Test::Unit::TestCase
  def element(name = 'ROOT', arg = '', attrs = {}, elements = [], unused = nil)
    Fluent::Config::Element.new(name, arg, attrs, elements, unused)
  end

  sub_test_case 'Most test' do
    test 'should be written' do
      pend 'TODO'
    end
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
end
