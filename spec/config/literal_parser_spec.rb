require "config/helper"
require "fluent/config/error"
require "fluent/config/literal_parser"
require "fluent/config/v1_parser"

describe Fluent::Config::LiteralParser do
  include_context 'config_helper'

  TestLiteralParserContext = Struct.new(:v1, :v2, :v3)

  let(:v1) { :test }
  let(:v2) { true }
  let(:v3) { nil }

  let(:eval_context) { TestLiteralParserContext.new(v1, v2, v3) }

  def parse_text(text)
    basepath = File.expand_path(File.dirname(__FILE__)+'/../../')
    ss = StringScanner.new(text)
    parser = Fluent::Config::V1Parser.new(ss, basepath, "(test)", eval_context)
    parser.parse_literal
  end

  describe 'boolean parsing' do
    it { expect('true').to be_parsed_as("true") }
    it { expect('false').to be_parsed_as("false") }
    it { expect('trueX').to be_parsed_as("trueX") }
    it { expect('falseX').to be_parsed_as("falseX") }
  end

  describe 'integer parsing' do
    it { expect('0').to be_parsed_as("0") }
    it { expect('1').to be_parsed_as("1") }
    it { expect('10').to be_parsed_as("10") }
    it { expect('-1').to be_parsed_as("-1") }
    it { expect('-10').to be_parsed_as("-10") }
    it { expect('0 ').to be_parsed_as("0") }
    it { expect(' -1 ').to be_parsed_as("-1") }
    # string
    it { expect('01').to be_parsed_as("01") }
    it { expect('00').to be_parsed_as("00") }
    it { expect('-01').to be_parsed_as("-01") }
    it { expect('-00').to be_parsed_as("-00") }
    it { expect('0x61').to be_parsed_as("0x61") }
    it { expect('0s').to be_parsed_as("0s") }
  end

  describe 'float parsing' do
    it { expect('1.1').to be_parsed_as("1.1") }
    it { expect('0.1').to be_parsed_as("0.1") }
    it { expect('0.0').to be_parsed_as("0.0") }
    it { expect('-1.1').to be_parsed_as("-1.1") }
    it { expect('-0.1').to be_parsed_as("-0.1") }
    it { expect('1.10').to be_parsed_as("1.10") }
    # string
    it { expect('12e8').to be_parsed_as("12e8") }
    it { expect('12.1e7').to be_parsed_as("12.1e7") }
    it { expect('-12e8').to be_parsed_as("-12e8") }
    it { expect('-12.1e7').to be_parsed_as("-12.1e7") }
    it { expect('.0').to be_parsed_as(".0") }
    it { expect('.1').to be_parsed_as(".1") }
    it { expect('0.').to be_parsed_as("0.") }
    it { expect('1.').to be_parsed_as("1.") }
    it { expect('.0a').to be_parsed_as(".0a") }
    it { expect('1.a').to be_parsed_as("1.a") }
    it { expect('0@').to be_parsed_as("0@") }
  end

  describe 'float keywords parsing' do
    it { expect('NaN').to be_parsed_as("NaN") }
    it { expect('Infinity').to be_parsed_as("Infinity") }
    it { expect('-Infinity').to be_parsed_as("-Infinity") }
    it { expect('NaNX').to be_parsed_as("NaNX") }
    it { expect('InfinityX').to be_parsed_as("InfinityX") }
    it { expect('-InfinityX').to be_parsed_as("-InfinityX") }
  end

  describe 'quoted string' do
    it { expect('""').to be_parsed_as("") }
    it { expect('"text"').to be_parsed_as("text") }
    it { expect('"\\""').to be_parsed_as("\"") }
    it { expect('"\\t"').to be_parsed_as("\t") }
    it { expect('"\\n"').to be_parsed_as("\n") }
    it { expect('"\\r\\n"').to be_parsed_as("\r\n") }
    it { expect('"\\f\\b"').to be_parsed_as("\f\b") }
    it { expect('"\\.t"').to be_parsed_as(".t") }
    it { expect('"\\$t"').to be_parsed_as("$t") }
    it { expect('"\\#t"').to be_parsed_as("#t") }
    it { expect('"\\z"').to be_parse_error }  # unknown escaped character
    it { expect('"\\0"').to be_parse_error }  # unknown escaped character
    it { expect('"\\1"').to be_parse_error }  # unknown escaped character
    it { expect('"t').to be_parse_error }  # non-terminated quoted character
    it { expect('t"').to be_parsed_as('t"') }
    it { expect('"."').to be_parsed_as('.') }
    it { expect('"*"').to be_parsed_as('*') }
    it { expect('"@"').to be_parsed_as('@') }
    it { expect('"\\#{test}"').to be_parsed_as("\#{test}") }
    it { expect('"$"').to be_parsed_as('$') }
    it { expect('"$t"').to be_parsed_as('$t') }
    it { expect('"$}"').to be_parsed_as('$}') }
  end

  describe 'nonquoted string parsing' do
    # empty
    it { expect('').to be_parsed_as(nil) }

    it { expect('t').to be_parsed_as('t') }
    it { expect('T').to be_parsed_as('T') }
    it { expect('_').to be_parsed_as('_') }
    it { expect('T1').to be_parsed_as('T1') }
    it { expect('_2').to be_parsed_as('_2') }
    it { expect('t0').to be_parsed_as('t0') }
    it { expect('t@').to be_parsed_as('t@') }
    it { expect('t-').to be_parsed_as('t-') }
    it { expect('t.').to be_parsed_as('t.') }
    it { expect('t+').to be_parsed_as('t+') }
    it { expect('t/').to be_parsed_as('t/') }
    it { expect('t=').to be_parsed_as('t=') }
    it { expect('t,').to be_parsed_as('t,') }
    it { expect('0t').to be_parsed_as("0t") }
    it { expect('@1t').to be_parsed_as('@1t') }
    it { expect('-1t').to be_parsed_as('-1t') }
    it { expect('.1t').to be_parsed_as('.1t') }
    it { expect(',1t').to be_parsed_as(',1t') }
    it { expect('.t').to be_parsed_as('.t') }
    it { expect('*t').to be_parsed_as('*t') }
    it { expect('@t').to be_parsed_as('@t') }
    it { expect('$t').to be_parsed_as('$t') }
    it { expect('{t').to be_parse_error }  # '{' begins map
    it { expect('t{').to be_parsed_as('t{') }
    it { expect('}t').to be_parsed_as('}t') }
    it { expect('[t').to be_parse_error }  # '[' begins array
    it { expect('t[').to be_parsed_as('t[') }
    it { expect(']t').to be_parsed_as(']t') }
    it { expect('$t').to be_parsed_as('$t') }
    it { expect('t:').to be_parsed_as('t:') }
    it { expect('t;').to be_parsed_as('t;') }
    it { expect('t?').to be_parsed_as('t?') }
    it { expect('t^').to be_parsed_as('t^') }
    it { expect('t`').to be_parsed_as('t`') }
    it { expect('t~').to be_parsed_as('t~') }
    it { expect('t|').to be_parsed_as('t|') }
    it { expect('t>').to be_parsed_as('t>') }
    it { expect('t<').to be_parsed_as('t<') }
    it { expect('t(').to be_parsed_as('t(') }
    it { expect('t[').to be_parsed_as('t[') }
  end

  describe 'embedded ruby code parsing' do
    it { expect('"#{v1}"').to be_parsed_as("#{v1}") }
    it { expect('"#{v2}"').to be_parsed_as("#{v2}") }
    it { expect('"#{v3}"').to be_parsed_as("#{v3}") }
    it { expect('"#{1+1}"').to be_parsed_as("2") }
    it { expect('"#{}"').to be_parsed_as("") }
    it { expect('"t#{v1}"').to be_parsed_as("t#{v1}") }
    it { expect('"t#{v1}t"').to be_parsed_as("t#{v1}t") }
    it { expect('"#{"}"}"').to be_parsed_as("}") }
    it { expect('"#{#}"').to be_parse_error }  # error in embedded ruby code
    it { expect("\"\#{\n=begin\n}\"").to be_parse_error }  # error in embedded ruby code
  end

  describe 'array parsing' do
    it { expect('[]').to be_parsed_as_json([]) }
    it { expect('[1]').to be_parsed_as_json([1]) }
    it { expect('[1,2]').to be_parsed_as_json([1,2]) }
    it { expect('[1, 2]').to be_parsed_as_json([1,2]) }
    it { expect('[ 1 , 2 ]').to be_parsed_as_json([1,2]) }
    it { expect('[1,2,]').to be_parse_error } # TODO: Need trailing commas support?
    it { expect("[\n1\n,\n2\n]").to be_parsed_as_json([1,2]) }
    it { expect('["a"]').to be_parsed_as_json(["a"]) }
    it { expect('["a","b"]').to be_parsed_as_json(["a","b"]) }
    it { expect('[ "a" , "b" ]').to be_parsed_as_json(["a","b"]) }
    it { expect("[\n\"a\"\n,\n\"b\"\n]").to be_parsed_as_json(["a","b"]) }
    it { expect('["ab","cd"]').to be_parsed_as_json(["ab","cd"]) }
  end

  describe 'map parsing' do
    it { expect('{}').to be_parsed_as_json({}) }
    it { expect('{"a":1}').to be_parsed_as_json({"a"=>1}) }
    it { expect('{"a":1,"b":2}').to be_parsed_as_json({"a"=>1,"b"=>2}) }
    it { expect('{ "a" : 1 , "b" : 2 }').to be_parsed_as_json({"a"=>1,"b"=>2}) }
    it { expect('{"a":1,"b":2,}').to be_parse_error } # TODO: Need trailing commas support?
    it { expect("{\n\"a\"\n:\n1\n,\n\"b\"\n:\n2\n}").to be_parsed_as_json({"a"=>1,"b"=>2}) }
    it { expect('{"a":"b"}').to be_parsed_as_json({"a"=>"b"}) }
    it { expect('{"a":"b","c":"d"}').to be_parsed_as_json({"a"=>"b","c"=>"d"}) }
    it { expect('{ "a" : "b" , "c" : "d" }').to be_parsed_as_json({"a"=>"b","c"=>"d"}) }
    it { expect("{\n\"a\"\n:\n\"b\"\n,\n\"c\"\n:\n\"d\"\n}").to be_parsed_as_json({"a"=>"b","c"=>"d"}) }
  end
end

