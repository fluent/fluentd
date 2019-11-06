require 'helper'
require 'fluent/config/types'

class TestConfigTypes < ::Test::Unit::TestCase
  include Fluent

  sub_test_case 'Config.size_value' do
    data("2k" => [2048, "2k"],
         "2K" => [2048, "2K"],
         "3m" => [3145728, "3m"],
         "3M" => [3145728, "3M"],
         "4g" => [4294967296, "4g"],
         "4G" => [4294967296, "4G"],
         "5t" => [5497558138880, "5t"],
         "5T" => [5497558138880, "5T"],
         "6"  => [6, "6"])
    test 'normal case' do |(expected, val)|
      assert_equal(expected, Config.size_value(val))
      assert_equal(expected, Config.size_value(val, { strict: true }))
    end

    data("integer" => [6, 6],
         "hoge" => [0, "hoge"],
         "empty" => [0, ""],
         "nil" => [0, nil])
    test 'not assumed case' do |(expected, val)|
      assert_equal(expected, Config.size_value(val))
    end

    data("integer" => [6, 6],
         "hoge" => [ArgumentError.new("invalid value for Integer(): \"hoge\""), "hoge"],
         "empty" => [ArgumentError.new("invalid value for Integer(): \"\""), ""],
         "nil" => [TypeError.new("can't convert nil into Integer"), nil])
    test 'not assumed case with strict' do |(expected, val)|
      if expected.kind_of? Exception
        assert_raise(expected) do
          Config.size_value(val, { strict: true })
        end
      else
        assert_equal(expected, Config.size_value(val, { strict: true }))
      end
    end
  end

  sub_test_case 'Config.time_value' do
    data("10s" => [10, "10s"],
         "10sec" => [10, "10sec"],
         "2m" => [120, "2m"],
         "3h" => [10800, "3h"],
         "4d" => [345600, "4d"])
    test 'normal case' do |(expected, val)|
      assert_equal(expected, Config.time_value(val))
      assert_equal(expected, Config.time_value(val, { strict: true }))
    end

    data("integer" => [4.0, 4],
         "float" => [0.4, 0.4],
         "hoge" => [0.0, "hoge"],
         "empty" => [0.0, ""],
         "nil" => [0.0, nil])
    test 'not assumed case' do |(expected, val)|
      assert_equal(expected, Config.time_value(val))
    end

    data("integer" => [6, 6],
         "hoge" => [ArgumentError.new("invalid value for Float(): \"hoge\""), "hoge"],
         "empty" => [ArgumentError.new("invalid value for Float(): \"\""), ""],
         "nil" => [TypeError.new("can't convert nil into Float"), nil])
    test 'not assumed case with strict' do |(expected, val)|
      if expected.kind_of? Exception
        assert_raise(expected) do
          Config.time_value(val, { strict: true })
        end
      else
        assert_equal(expected, Config.time_value(val, { strict: true }))
      end
    end
  end

  sub_test_case 'Config.bool_value' do
    data("true" => [true, "true"],
         "yes" => [true, "yes"],
         "empty" => [true, ""],
         "false" => [false, "false"],
         "no" => [false, "no"])
    test 'normal case' do |(expected, val)|
      assert_equal(expected, Config.bool_value(val))
    end

    data("true" =>  [true,  true],
         "false" => [false, false],
         "hoge" => [nil, "hoge"],
         "nil" => [nil, nil],
         "integer" => [nil, 10])
    test 'not assumed case' do |(expected, val)|
      assert_equal(expected, Config.bool_value(val))
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
    data("test" => ['test', 'test'],
         "1" =>  ['1',    '1'],
         "spaces" =>  ['   ',  '   '])
    test 'string' do |(expected, val)|
      assert_equal expected, Config::STRING_TYPE.call(val, {})
      assert_equal Encoding::UTF_8, Config::STRING_TYPE.call(val, {}).encoding
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

    data("val" => [:val, 'val'],
         "v" => [:v, 'v'],
         "value" => [:value, 'value'])
    test 'enum' do |(expected, val, list)|
      assert_equal expected, Config::ENUM_TYPE.call(val, {list: [:val, :value, :v]})
    end

    test 'enum: pick unknown choice' do
      assert_raises(Fluent::ConfigError.new("valid options are val,value,v but got x")) do
        Config::ENUM_TYPE.call('x', {list: [:val, :value, :v]})
      end
    end

    data("empty list"  => {},
         "string list" => {list: ["val", "value", "v"]})
    test 'enum: invalid choices' do | list |
      assert_raises(RuntimeError.new("Plugin BUG: config type 'enum' requires :list of symbols")) do
        Config::ENUM_TYPE.call('val', list)
      end
    end

    data("1" => [1, '1'],
         "1.0" => [1, '1.0'],
         "1_000" => [1000, '1_000'],
         "1x" => [1, '1x'])
    test 'integer' do |(expected, val)|
      assert_equal expected, Config::INTEGER_TYPE.call(val, {})
    end

    data("integer" => [6, 6],
         "hoge" => [0, "hoge"],
         "empty" => [0, ""],
         "nil" => [0, nil])
    test 'integer: not assumed case' do |(expected, val)|
      assert_equal expected, Config::INTEGER_TYPE.call(val, {})
    end

    data("integer" => [6, 6],
         "hoge" => [ArgumentError.new("invalid value for Integer(): \"hoge\""), "hoge"],
         "empty" => [ArgumentError.new("invalid value for Integer(): \"\""), ""],
         "nil" => [TypeError.new("can't convert nil into Integer"), nil])
    test 'integer: not assumed case with strict' do |(expected, val)|
      if expected.kind_of? Exception
        assert_raise(expected) do
          Config::INTEGER_TYPE.call(val, { strict: true })
        end
      else
        assert_equal expected, Config::INTEGER_TYPE.call(val, { strict: true })
      end
    end

    data("1" => [1.0, '1'],
         "1.0" => [1.0, '1.0'],
         "1.00" => [1.0, '1.00'],
         "1e0" => [1.0, '1e0'])
    test 'float' do |(expected, val)|
      assert_equal expected, Config::FLOAT_TYPE.call(val, {})
    end

    data("integer" => [6, 6],
         "hoge" => [0, "hoge"],
         "empty" => [0, ""],
         "nil" => [0, nil])
    test 'float: not assumed case' do |(expected, val)|
      assert_equal expected, Config::FLOAT_TYPE.call(val, {})
    end

    data("integer" => [6, 6],
         "hoge" => [ArgumentError.new("invalid value for Float(): \"hoge\""), "hoge"],
         "empty" => [ArgumentError.new("invalid value for Float(): \"\""), ""],
         "nil" => [TypeError.new("can't convert nil into Float"), nil])
    test 'float: not assumed case with strict' do |(expected, val)|
      if expected.kind_of? Exception
        assert_raise(expected) do
          Config::FLOAT_TYPE.call(val, { strict: true })
        end
      else
        assert_equal expected, Config::FLOAT_TYPE.call(val, { strict: true })
      end
    end

    data("1000" => [1000, '1000'],
         "1k" => [1024, '1k'],
         "1m" => [1024*1024, '1m'])
    test 'size' do |(expected, val)|
      assert_equal expected, Config::SIZE_TYPE.call(val, {})
    end

    data("true" => [true, 'true'],
         "yes" => [true, 'yes'],
         "no" => [false, 'no'],
         "false" => [false, 'false'],
         "TRUE" => [nil, 'TRUE'],
         "True" => [nil, 'True'],
         "Yes" => [nil, 'Yes'],
         "No" => [nil, 'No'],
         "empty" => [true, ''],
         "unexpected_string" => [nil, 'unexpected_string'])
    test 'bool' do |(expected, val)|
      assert_equal expected, Config::BOOL_TYPE.call(val, {})
    end

    data("0" => [0, '0'],
         "1" => [1.0, '1'],
         "1.01" => [1.01,  '1.01'],
         "1s" => [1, '1s'],
         "1," => [60, '1m'],
         "1h" => [3600,  '1h'],
         "1d" => [86400, '1d'])
    test 'time' do |(expected, val)|
      assert_equal expected, Config::TIME_TYPE.call(val, {})
    end

    data("empty" => [//, "//"],
         "plain" => [/regexp/, "/regexp/"],
         "zero width" => [/^$/, "/^$/"],
         "character classes" => [/[a-z]/, "/[a-z]/"],
         "meta charactersx" => [/.+.*?\d\w\s\S/, '/.+.*?\d\w\s\S/'])
    test 'regexp' do |(expected, str)|
      assert_equal(expected, Config::REGEXP_TYPE.call(str, {}))
    end

    data("string and integer" => [{"x"=>"v","k"=>1}, '{"x":"v","k":1}', {}],
         "strings" => [{"x"=>"v","k"=>"1"}, 'x:v,k:1', {}],
         "w/ space" => [{"x"=>"v","k"=>"1"}, 'x:v, k:1', {}],
         "heading space" => [{"x"=>"v","k"=>"1"}, ' x:v, k:1 ', {}],
         "trailing space" => [{"x"=>"v","k"=>"1"}, 'x:v , k:1 ', {}],
         "multiple colons" => [{"x"=>"v:v","k"=>"1"}, 'x:v:v, k:1', {}],
         "symbolize keys" => [{x: "v", k: 1}, '{"x":"v","k":1}', {symbolize_keys: true}],
         "value_type: :string" => [{x: "v", k: "1"}, 'x:v,k:1', {symbolize_keys: true, value_type: :string}],
         "value_type: :string 2" => [{x: "v", k: "1"}, '{"x":"v","k":1}', {symbolize_keys: true, value_type: :string}],
         "value_type: :integer" => [{x: 0, k: 1}, 'x:0,k:1', {symbolize_keys: true, value_type: :integer}],
         "time 1" => [{"x"=>1,"y"=>60,"z"=>3600}, '{"x":"1s","y":"1m","z":"1h"}', {value_type: :time}],
         "time 2" => [{"x"=>1,"y"=>60,"z"=>3600}, 'x:1s,y:1m,z:1h', {value_type: :time}])
    test 'hash' do |(expected, val, opts)|
      assert_equal(expected, Config::HASH_TYPE.call(val, opts))
    end

    test 'hash w/ unknown type' do
      assert_raise(RuntimeError.new("unknown type in REFORMAT: foo")) do
        Config::HASH_TYPE.call("x:1,y:2", {value_type: :foo})
      end
    end

    test 'hash w/ strict option' do
      assert_raise(ArgumentError.new("invalid value for Integer(): \"hoge\"")) do
        Config::HASH_TYPE.call("x:1,y:hoge", {value_type: :integer, strict: true})
      end
    end

    data('latin' => ['3:Märch', {"3"=>"Märch"}],
         'ascii' => ['ascii:ascii', {"ascii"=>"ascii"}],
         'number' => ['number:1', {"number"=>"1"}],
         'Hiragana' => ['hiragana:あいうえお', {"hiragana"=>"あいうえお"}])
    test 'hash w/ binary' do |(target, expected)|
      assert_equal(expected, Config::HASH_TYPE.call(target.b, { value_type: :string }))
    end

    data("strings and integer" => [["1","2",1],   '["1","2",1]', {}],
         "number strings" => [["1","2","1"], '1,2,1', {}],
         "alphabets" => [["a","b","c"], '["a","b","c"]', {}],
         "alphabets w/o quote" => [["a","b","c"], 'a,b,c', {}],
         "w/ spaces" => [["a","b","c"], 'a, b, c', {}],
         "w/ space before comma" => [["a","b","c"], 'a , b , c', {}],
         "comma or space w/ qupte" => [["a a","b,b"," c "], '["a a","b,b"," c "]', {}],
         "space in a value w/o qupte" => [["a a","b","c"], 'a a,b,c', {}],
         "integers" => [[1,2,1], '[1,2,1]', {}],
         "value_type: :integer w/ quote" => [[1,2,1], '["1","2","1"]', {value_type: :integer}],
         "value_type: :integer w/o quote" => [[1,2,1], '1,2,1', {value_type: :integer}])
    test 'array' do |(expected, val, opts)|
      assert_equal(expected, Config::ARRAY_TYPE.call(val, opts))
    end

    data('["1","2"]' => [["1","2"], '["1","2"]'],
         '["3"]' => [["3"], '["3"]'])
    test 'array w/ default values' do |(expected, val)|
      array_options = {
        default: [],
      }
      assert_equal(expected, Config::ARRAY_TYPE.call(val, array_options))
    end

    test 'array w/ unknown type' do
      assert_raise(RuntimeError.new("unknown type in REFORMAT: foo")) do
        Config::ARRAY_TYPE.call("1,2", {value_type: :foo})
      end
    end

    test 'array w/ strict option' do
      assert_raise(ArgumentError.new("invalid value for Integer(): \"hoge\"")) do
        Config::ARRAY_TYPE.call("1,hoge", {value_type: :integer, strict: true})
      end
    end
  end
end
