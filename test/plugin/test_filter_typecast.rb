require_relative '../helper'
require 'fluent/plugin/filter_typecast'

class DupFilterTest < Test::Unit::TestCase
  include Fluent

  setup do
    Fluent::Test.setup
    @time = Fluent::Engine.now
  end

  def create_driver(conf = '')
    Test::FilterTestDriver.new(TypecastFilter).configure(conf, true)
  end

  def filter(d, msg)
    d.run { d.filter(msg, @time) }
    d.filtered_as_array.first[2]
  end

  sub_test_case 'configure' do
    def test_types
      d = create_driver(%[
        types string:string,integer:integer,float:float,bool:bool,time:time,array:array
      ])
      types = d.instance.instance_variable_get(:@type_converters)
      converters = ::Fluent::TextParser::TypeConverter::Converters
      assert_equal converters['string'],  types['string']
      assert_equal converters['integer'], types['integer']
      assert_equal converters['float'],   types['float']
      assert_equal converters['bool'],    types['bool']
    end
  end

  sub_test_case 'test_type_cast' do
    def test_types
      d = create_driver(%[
        types string:string,integer:integer,float:float,bool:bool,time:time,array:array
      ])
      msg = {
        'integer' => '1',
        'string'  => 'foo',
        'time'    => '2013-02-12 22:01:15 UTC',
        'bool'    => 'true',
        'array'   => 'a,b,c',
        'other'   => 'other',
      }
      filtered = filter(d, msg)
      assert_equal 1, filtered['integer']
      assert_equal 'foo', filtered['string']
      assert_equal 1360706475, filtered['time']
      assert_equal true, filtered['bool']
      assert_equal ['a', 'b', 'c'], filtered['array']
      assert_equal 'other', filtered['other']
    end

    def test_time_format
      d = create_driver(%[types time:time:%d/%b/%Y:%H:%M:%S %z])
      msg = { 'time' => '12/Dec/2014:19:37:37 +0900' }
      filtered = filter(d, msg)
      assert_equal 1418380657, filtered['time']
    end

    def test_array_delimiter
      d = create_driver(%[types array:array:.])
      msg = { 'array' => 'a.b.c' }
      filtered = filter(d, msg)
      assert_equal ['a','b','c'], filtered['array']
    end
  end
end
