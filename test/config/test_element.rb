require 'helper'
require 'fluent/config/element'

class TestConfigElement < ::Test::Unit::TestCase
  def element(name = 'ROOT', arg = '', attrs = {}, elements = [])
    Fluent::Config::Element.new(name, arg, attrs, elements)
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
end
