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
    it { 'true'.should be_parsed_as("true") }
    it { 'false'.should be_parsed_as("false") }
    it { 'trueX'.should be_parsed_as("trueX") }
    it { 'falseX'.should be_parsed_as("falseX") }
  end

  describe 'integer parsing' do
    it { '0'.should be_parsed_as("0") }
    it { '1'.should be_parsed_as("1") }
    it { '10'.should be_parsed_as("10") }
    it { '-1'.should be_parsed_as("-1") }
    it { '-10'.should be_parsed_as("-10") }
    it { '0 '.should be_parsed_as("0") }
    it { ' -1 '.should be_parsed_as("-1") }
    # string
    it { '01'.should be_parsed_as("01") }
    it { '00'.should be_parsed_as("00") }
    it { '-01'.should be_parsed_as("-01") }
    it { '-00'.should be_parsed_as("-00") }
    it { '0x61'.should be_parsed_as("0x61") }
    it { '0s'.should be_parsed_as("0s") }
  end

  describe 'float parsing' do
    it { '1.1'.should be_parsed_as("1.1") }
    it { '0.1'.should be_parsed_as("0.1") }
    it { '0.0'.should be_parsed_as("0.0") }
    it { '-1.1'.should be_parsed_as("-1.1") }
    it { '-0.1'.should be_parsed_as("-0.1") }
    it { '1.10'.should be_parsed_as("1.10") }
    # string
    it { '12e8'.should be_parsed_as("12e8") }
    it { '12.1e7'.should be_parsed_as("12.1e7") }
    it { '-12e8'.should be_parsed_as("-12e8") }
    it { '-12.1e7'.should be_parsed_as("-12.1e7") }
    it { '.0'.should be_parsed_as(".0") }
    it { '.1'.should be_parsed_as(".1") }
    it { '0.'.should be_parsed_as("0.") }
    it { '1.'.should be_parsed_as("1.") }
    it { '.0a'.should be_parsed_as(".0a") }
    it { '1.a'.should be_parsed_as("1.a") }
    it { '0@'.should be_parsed_as("0@") }
  end

  describe 'float keywords parsing' do
    it { 'NaN'.should be_parsed_as("NaN") }
    it { 'Infinity'.should be_parsed_as("Infinity") }
    it { '-Infinity'.should be_parsed_as("-Infinity") }
    it { 'NaNX'.should be_parsed_as("NaNX") }
    it { 'InfinityX'.should be_parsed_as("InfinityX") }
    it { '-InfinityX'.should be_parsed_as("-InfinityX") }
  end

  describe 'quoted string' do
    it { '""'.should be_parsed_as("") }
    it { '"text"'.should be_parsed_as("text") }
    it { '"\\""'.should be_parsed_as("\"") }
    it { '"\\t"'.should be_parsed_as("\t") }
    it { '"\\n"'.should be_parsed_as("\n") }
    it { '"\\r\\n"'.should be_parsed_as("\r\n") }
    it { '"\\f\\b"'.should be_parsed_as("\f\b") }
    it { '"\\.t"'.should be_parsed_as(".t") }
    it { '"\\$t"'.should be_parsed_as("$t") }
    it { '"\\#t"'.should be_parsed_as("#t") }
    it { '"\\z"'.should be_parse_error }  # unknown escaped character
    it { '"\\0"'.should be_parse_error }  # unknown escaped character
    it { '"\\1"'.should be_parse_error }  # unknown escaped character
    it { '"t'.should be_parse_error }  # non-terminated quoted character
    it { 't"'.should be_parsed_as('t"') }
    it { '"."'.should be_parsed_as('.') }
    it { '"*"'.should be_parsed_as('*') }
    it { '"@"'.should be_parsed_as('@') }
    it { '"\\#{test}"'.should be_parsed_as("\#{test}") }
    it { '"$"'.should be_parsed_as('$') }
    it { '"$t"'.should be_parsed_as('$t') }
    it { '"$}"'.should be_parsed_as('$}') }
  end

  describe 'nonquoted string parsing' do
    # empty
    it { ''.should be_parsed_as(nil) }

    it { 't'.should be_parsed_as('t') }
    it { 'T'.should be_parsed_as('T') }
    it { '_'.should be_parsed_as('_') }
    it { 'T1'.should be_parsed_as('T1') }
    it { '_2'.should be_parsed_as('_2') }
    it { 't0'.should be_parsed_as('t0') }
    it { 't@'.should be_parsed_as('t@') }
    it { 't-'.should be_parsed_as('t-') }
    it { 't.'.should be_parsed_as('t.') }
    it { 't+'.should be_parsed_as('t+') }
    it { 't/'.should be_parsed_as('t/') }
    it { 't='.should be_parsed_as('t=') }
    it { 't,'.should be_parsed_as('t,') }
    it { '0t'.should be_parsed_as("0t") }
    it { '@1t'.should be_parsed_as('@1t') }
    it { '-1t'.should be_parsed_as('-1t') }
    it { '.1t'.should be_parsed_as('.1t') }
    it { ',1t'.should be_parsed_as(',1t') }
    it { '.t'.should be_parsed_as('.t') }
    it { '*t'.should be_parsed_as('*t') }
    it { '@t'.should be_parsed_as('@t') }
    it { '$t'.should be_parsed_as('$t') }
    it { '{t'.should be_parse_error }  # '{' begins map
    it { 't{'.should be_parsed_as('t{') }
    it { '}t'.should be_parsed_as('}t') }
    it { '[t'.should be_parse_error }  # '[' begins array
    it { 't['.should be_parsed_as('t[') }
    it { ']t'.should be_parsed_as(']t') }
    it { '$t'.should be_parsed_as('$t') }
    it { 't:'.should be_parsed_as('t:') }
    it { 't;'.should be_parsed_as('t;') }
    it { 't?'.should be_parsed_as('t?') }
    it { 't^'.should be_parsed_as('t^') }
    it { 't`'.should be_parsed_as('t`') }
    it { 't~'.should be_parsed_as('t~') }
    it { 't|'.should be_parsed_as('t|') }
    it { 't>'.should be_parsed_as('t>') }
    it { 't<'.should be_parsed_as('t<') }
    it { 't('.should be_parsed_as('t(') }
    it { 't['.should be_parsed_as('t[') }
  end

  describe 'embedded ruby code parsing' do
    it { '"#{v1}"'.should be_parsed_as("#{v1}") }
    it { '"#{v2}"'.should be_parsed_as("#{v2}") }
    it { '"#{v3}"'.should be_parsed_as("#{v3}") }
    it { '"#{1+1}"'.should be_parsed_as("2") }
    it { '"#{}"'.should be_parsed_as("") }
    it { '"t#{v1}"'.should be_parsed_as("t#{v1}") }
    it { '"t#{v1}t"'.should be_parsed_as("t#{v1}t") }
    it { '"#{"}"}"'.should be_parsed_as("}") }
    it { '"#{#}"'.should be_parse_error }  # error in embedded ruby code
    it { "\"\#{\n=begin\n}\"".should be_parse_error }  # error in embedded ruby code
  end

  describe 'array parsing' do
    it { '[]'.should be_parsed_as_json([]) }
    it { '[1]'.should be_parsed_as_json([1]) }
    it { '[1,2]'.should be_parsed_as_json([1,2]) }
    it { '[1, 2]'.should be_parsed_as_json([1,2]) }
    it { '[ 1 , 2 ]'.should be_parsed_as_json([1,2]) }
    it { '[1,2,]'.should be_parse_error } # TODO: Need trailing commas support?
    it { "[\n1\n,\n2\n]".should be_parsed_as_json([1,2]) }
    it { '["a"]'.should be_parsed_as_json(["a"]) }
    it { '["a","b"]'.should be_parsed_as_json(["a","b"]) }
    it { '[ "a" , "b" ]'.should be_parsed_as_json(["a","b"]) }
    it { "[\n\"a\"\n,\n\"b\"\n]".should be_parsed_as_json(["a","b"]) }
    it { '["ab","cd"]'.should be_parsed_as_json(["ab","cd"]) }
  end

  describe 'map parsing' do
    it { '{}'.should be_parsed_as_json({}) }
    it { '{"a":1}'.should be_parsed_as_json({"a"=>1}) }
    it { '{"a":1,"b":2}'.should be_parsed_as_json({"a"=>1,"b"=>2}) }
    it { '{ "a" : 1 , "b" : 2 }'.should be_parsed_as_json({"a"=>1,"b"=>2}) }
    it { '{"a":1,"b":2,}'.should be_parse_error } # TODO: Need trailing commas support?
    it { "{\n\"a\"\n:\n1\n,\n\"b\"\n:\n2\n}".should be_parsed_as_json({"a"=>1,"b"=>2}) }
    it { '{"a":"b"}'.should be_parsed_as_json({"a"=>"b"}) }
    it { '{"a":"b","c":"d"}'.should be_parsed_as_json({"a"=>"b","c"=>"d"}) }
    it { '{ "a" : "b" , "c" : "d" }'.should be_parsed_as_json({"a"=>"b","c"=>"d"}) }
    it { "{\n\"a\"\n:\n\"b\"\n,\n\"c\"\n:\n\"d\"\n}".should be_parsed_as_json({"a"=>"b","c"=>"d"}) }
  end
end

