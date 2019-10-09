require 'helper'
require 'fluent/config/types'

class TestConfigTypes < ::Test::Unit::TestCase
  include Fluent

  sub_test_case 'Config.size_value' do
    test 'normal case' do
      assert_equal(2048, Config.size_value("2k"))
      assert_equal(2048, Config.size_value("2K"))
      assert_equal(3145728, Config.size_value("3m"))
      assert_equal(3145728, Config.size_value("3M"))
      assert_equal(4294967296, Config.size_value("4g"))
      assert_equal(4294967296, Config.size_value("4G"))
      assert_equal(5497558138880, Config.size_value("5t"))
      assert_equal(5497558138880, Config.size_value("5T"))
      assert_equal(6, Config.size_value("6"))
    end

    test 'not assumed case' do
      assert_equal(6, Config.size_value(6))
      assert_equal(0, Config.size_value("hoge"))
      assert_equal(0, Config.size_value(""))
      assert_equal(0, Config.size_value(nil))
    end
  end

  sub_test_case 'Config.time_value' do
    test 'normal case' do
      assert_equal(10, Config.time_value("10s"))
      assert_equal(10, Config.time_value("10sec"))
      assert_equal(120, Config.time_value("2m"))
      assert_equal(10800, Config.time_value("3h"))
      assert_equal(345600, Config.time_value("4d"))
    end

    test 'not assumed case' do
      assert_equal(4.0, Config.time_value(4))
      assert_equal(0.4, Config.time_value(0.4))
      assert_equal(0.0, Config.time_value("hoge"))
      assert_equal(0.0, Config.time_value(""))
      assert_equal(0.0, Config.time_value(nil))
    end
  end

  sub_test_case 'Config.bool_value' do
    test 'normal case' do
      assert_true Config.bool_value("true")
      assert_true Config.bool_value("yes")
      assert_true Config.bool_value("")
      assert_false Config.bool_value("false")
      assert_false Config.bool_value("no")
    end

    test 'not assumed case' do
      assert_true Config.bool_value(true)
      assert_false Config.bool_value(false)
      assert_nil Config.bool_value("hoge")
      assert_nil Config.bool_value(nil)
      assert_nil Config.bool_value(10)
    end
  end

  sub_test_case 'Config.regexp_value' do
    data("empty" => [//, "//"],
         "plain" => [/regexp/, "/regexp/"],
         "zero width" => [/^$/, "/^$/"],
         "character classes" => [/[a-z]/, "/[a-z]/"],
         "meta charactersx" => [/.+.*?\d\w\s\S/, '/.+.*?\d\w\s\S/'])
    test 'normal case' do |(expected, str)|
      assert_equal(expected, Config.regexp_value(str))
    end

    data("empty" => [//, ""],
         "plain" => [/regexp/, "regexp"],
         "zero width" => [/^$/, "^$"],
         "character classes" => [/[a-z]/, "[a-z]"],
         "meta charactersx" => [/.+.*?\d\w\s\S/, '.+.*?\d\w\s\S'])
    test 'w/o slashes' do |(expected, str)|
      assert_equal(expected, Config.regexp_value(str))
    end

    data("missing right slash" => "/regexp",
         "too many options" => "/regexp/imx",)
    test 'invalid regexp' do |(str)|
      assert_raise(Fluent::ConfigError.new("invalid regexp: missing right slash: #{str}")) do
        Config.regexp_value(str)
      end
    end
  end

  sub_test_case 'type converters for config_param definitions' do
    test 'string' do
      assert_equal 'test', Config::STRING_TYPE.call('test', {})
      assert_equal '1', Config::STRING_TYPE.call('1', {})
      assert_equal '   ', Config::STRING_TYPE.call('   ', {})
      assert_equal Encoding::UTF_8, Config::STRING_TYPE.call('test', {}).encoding
    end

    data('latin' => 'Märch',
         'ascii' => 'ascii',
         'space' => '     ',
         'number' => '1',
         'Hiragana' => 'あいうえお')
    test 'string w/ binary' do |str|
      actual = Config::STRING_TYPE.call(str.b, {})
      assert_equal str, actual
      assert_equal Encoding::UTF_8, actual.encoding
    end

    test 'enum' do
      assert_equal :val, Config::ENUM_TYPE.call('val', {list: [:val, :value, :v]})
      assert_equal :v, Config::ENUM_TYPE.call('v', {list: [:val, :value, :v]})
      assert_equal :value, Config::ENUM_TYPE.call('value', {list: [:val, :value, :v]})
      assert_raises(Fluent::ConfigError.new("valid options are val,value,v but got x")){ Config::ENUM_TYPE.call('x', {list: [:val, :value, :v]}) }
      assert_raises(RuntimeError.new("Plugin BUG: config type 'enum' requires :list of symbols")){ Config::ENUM_TYPE.call('val', {}) }
      assert_raises(RuntimeError.new("Plugin BUG: config type 'enum' requires :list of symbols")){ Config::ENUM_TYPE.call('val', {list: ["val", "value", "v"]}) }
    end

    test 'integer' do
      assert_equal 1, Config::INTEGER_TYPE.call('1', {})
      assert_equal 1, Config::INTEGER_TYPE.call('1.0', {})
      assert_equal 1000, Config::INTEGER_TYPE.call('1_000', {})
      assert_equal 1, Config::INTEGER_TYPE.call('1x', {})
    end

    test 'float' do
      assert_equal 1.0, Config::FLOAT_TYPE.call('1', {})
      assert_equal 1.0, Config::FLOAT_TYPE.call('1.0', {})
      assert_equal 1.0, Config::FLOAT_TYPE.call('1.00', {})
      assert_equal 1.0, Config::FLOAT_TYPE.call('1e0', {})
    end

    test 'size' do
      assert_equal 1000, Config::SIZE_TYPE.call('1000', {})
      assert_equal 1024, Config::SIZE_TYPE.call('1k', {})
      assert_equal 1024*1024, Config::SIZE_TYPE.call('1m', {})
    end

    test 'bool' do
      assert_equal true, Config::BOOL_TYPE.call('true', {})
      assert_equal true, Config::BOOL_TYPE.call('yes', {})
      assert_equal false, Config::BOOL_TYPE.call('no', {})
      assert_equal false, Config::BOOL_TYPE.call('false', {})

      assert_equal nil, Config::BOOL_TYPE.call('TRUE', {})
      assert_equal nil, Config::BOOL_TYPE.call('True', {})
      assert_equal nil, Config::BOOL_TYPE.call('Yes', {})
      assert_equal nil, Config::BOOL_TYPE.call('No', {})

      assert_equal true, Config::BOOL_TYPE.call('', {})
      assert_equal nil, Config::BOOL_TYPE.call('unexpected_string', {})
    end

    test 'time' do
      assert_equal 0, Config::TIME_TYPE.call('0', {})
      assert_equal 1.0, Config::TIME_TYPE.call('1', {})
      assert_equal 1.01, Config::TIME_TYPE.call('1.01', {})
      assert_equal 1, Config::TIME_TYPE.call('1s', {})
      assert_equal 60, Config::TIME_TYPE.call('1m', {})
      assert_equal 3600, Config::TIME_TYPE.call('1h', {})
      assert_equal 86400, Config::TIME_TYPE.call('1d', {})
    end

    data("empty" => [//, "//"],
         "plain" => [/regexp/, "/regexp/"],
         "zero width" => [/^$/, "/^$/"],
         "character classes" => [/[a-z]/, "/[a-z]/"],
         "meta charactersx" => [/.+.*?\d\w\s\S/, '/.+.*?\d\w\s\S/'])
    test 'regexp' do |(expected, str)|
      assert_equal(expected, Config::REGEXP_TYPE.call(str, {}))
    end

    test 'hash' do
      assert_equal({"x"=>"v","k"=>1}, Config::HASH_TYPE.call('{"x":"v","k":1}', {}))
      assert_equal({"x"=>"v","k"=>"1"}, Config::HASH_TYPE.call('x:v,k:1', {}))
      assert_equal({"x"=>"v","k"=>"1"}, Config::HASH_TYPE.call('x:v, k:1', {}))
      assert_equal({"x"=>"v","k"=>"1"}, Config::HASH_TYPE.call(' x:v, k:1 ', {}))
      assert_equal({"x"=>"v","k"=>"1"}, Config::HASH_TYPE.call('x:v , k:1 ', {}))

      assert_equal({"x"=>"v:v","k"=>"1"}, Config::HASH_TYPE.call('x:v:v, k:1', {}))

      assert_equal({x: "v", k: 1},   Config::HASH_TYPE.call('{"x":"v","k":1}', {symbolize_keys: true}))
      assert_equal({x: "v", k: "1"}, Config::HASH_TYPE.call('x:v,k:1',         {symbolize_keys: true, value_type: :string}))
      assert_equal({x: "v", k: "1"}, Config::HASH_TYPE.call('{"x":"v","k":1}', {symbolize_keys: true, value_type: :string}))
      assert_equal({x: 0, k: 1},     Config::HASH_TYPE.call('x:0,k:1',         {symbolize_keys: true, value_type: :integer}))

      assert_equal({"x"=>1,"y"=>60,"z"=>3600}, Config::HASH_TYPE.call('{"x":"1s","y":"1m","z":"1h"}', {value_type: :time}))
      assert_equal({"x"=>1,"y"=>60,"z"=>3600}, Config::HASH_TYPE.call('x:1s,y:1m,z:1h',               {value_type: :time}))

      assert_raise(RuntimeError.new("unknown type in REFORMAT: foo")){ Config::HASH_TYPE.call("x:1,y:2", {value_type: :foo}) }
    end

    data('latin' => ['3:Märch', {"3"=>"Märch"}],
         'ascii' => ['ascii:ascii', {"ascii"=>"ascii"}],
         'number' => ['number:1', {"number"=>"1"}],
         'Hiragana' => ['hiragana:あいうえお', {"hiragana"=>"あいうえお"}])
    test 'hash w/ binary' do |(target, expected)|
      assert_equal(expected, Config::HASH_TYPE.call(target.b, { value_type: :string }))
    end

    test 'array' do
      assert_equal(["1","2",1], Config::ARRAY_TYPE.call('["1","2",1]', {}))
      assert_equal(["1","2","1"], Config::ARRAY_TYPE.call('1,2,1', {}))

      assert_equal(["a","b","c"], Config::ARRAY_TYPE.call('["a","b","c"]', {}))
      assert_equal(["a","b","c"], Config::ARRAY_TYPE.call('a,b,c', {}))
      assert_equal(["a","b","c"], Config::ARRAY_TYPE.call('a, b, c', {}))
      assert_equal(["a","b","c"], Config::ARRAY_TYPE.call('a , b , c', {}))

      assert_equal(["a a","b,b"," c "], Config::ARRAY_TYPE.call('["a a","b,b"," c "]', {}))

      assert_equal(["a a","b","c"], Config::ARRAY_TYPE.call('a a,b,c', {}))

      assert_equal([1,2,1], Config::ARRAY_TYPE.call('[1,2,1]', {}))
      assert_equal([1,2,1], Config::ARRAY_TYPE.call('["1","2","1"]', {value_type: :integer}))
      assert_equal([1,2,1], Config::ARRAY_TYPE.call('1,2,1', {value_type: :integer}))

      array_options = {
        default: [],
      }
      assert_equal(["1","2"], Config::ARRAY_TYPE.call('["1","2"]', array_options))
      assert_equal(["3"], Config::ARRAY_TYPE.call('["3"]', array_options))

      assert_raise(RuntimeError.new("unknown type in REFORMAT: foo")){ Config::ARRAY_TYPE.call("1,2", {value_type: :foo}) }
    end
  end
end
