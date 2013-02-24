require 'spec_helper'
require "spec/config/helper"

describe Fluentd::Config::ValueParser do
  include_context 'config_helper'

  TestContext = Struct.new(:v1, :v2, :v3)

  let(:v1) { 'test' }
  let(:v2) { true }
  let(:v3) { nil }

  let(:ruby_context) { TestContext.new(v1, v2, v3) }

  class UnexpectedString < Fluentd::ConfigParseError
  end

  def parse_text(text)
    ss = StringScanner.new(text)
    vp = Fluentd::Config::ValueParser.new(ss, ruby_context)
    v = vp.parse_value
    if ss.eos?
      v
    else
      raise UnexpectedString, ss.rest
    end
  end

  context 'boolean' do
    it { 'true'.should be_parsed_as(true) }
    it { 'false'.should be_parsed_as(false) }
  end

  context 'null' do
    it { 'null'.should be_parsed_as(nil) }
  end

  context 'integer' do
    it { '0'.should be_parsed_as(0) }
    it { '1'.should be_parsed_as(1) }
    it { '10'.should be_parsed_as(10) }
    it { '-1'.should be_parsed_as(-1) }
    it { '-10'.should be_parsed_as(-10) }
    it { '01'.should be_parse_error }
    it { '00'.should be_parse_error }
    it { '-01'.should be_parse_error }
    it { '-00'.should be_parse_error }
  end

  context 'float' do
    it { '1.1'.should be_parsed_as(1.1) }
    it { '0.1'.should be_parsed_as(0.1) }
    it { '12e8'.should be_parsed_as(12e8) }
    it { '12.1e7'.should be_parsed_as(12.1e7) }
    it { '-1.1'.should be_parsed_as(-1.1) }
    it { '-12e8'.should be_parsed_as(-12e8) }
    it { '-12.1e7'.should be_parsed_as(-12.1e7) }
    it { '.0'.should be_parse_error }
    it { '.1'.should be_parse_error }
    it { '0.'.should be_parse_error }
    it { '1.'.should be_parse_error }
  end

  context 'float keywords' do
    it { 'NaN'.should be_parsed_as(Float::NAN) }
    it { 'Infinity'.should be_parsed_as(Float::INFINITY) }
    it { '-Infinity'.should be_parsed_as(-Float::INFINITY) }
  end

  context 'quoted string' do
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
    it { '"\\z"'.should be_parse_error }
    it { '"\\0"'.should be_parse_error }
    it { '"\\1"'.should be_parse_error }
    it { '"t'.should be_parse_error }
    it { 't"'.should be_parse_error }
    it { '"."'.should be_parsed_as('.') }
    it { '"*"'.should be_parsed_as('*') }
    it { '"@"'.should be_parsed_as('@') }
    it { '"\\${test}"'.should be_parsed_as("${test}") }
    it { '"$"'.should be_parsed_as('$') }
    it { '"$t"'.should be_parsed_as('$t') }
    it { '"$}"'.should be_parsed_as('$}') }
  end

  context 'nonquoted string' do
    it { ''.should be_parse_error }
    it { 't'.should be_parsed_as('t') }
    it { 'T'.should be_parsed_as('T') }
    it { '_'.should be_parsed_as('_') }
    it { 'T1'.should be_parsed_as('T1') }
    it { '_2'.should be_parsed_as('_2') }
    it { 't0'.should be_parsed_as('t0') }
    #it { 't@'.should be_parsed_as('t@') }
    #it { 't-'.should be_parsed_as('t-') }
    #it { 't.'.should be_parsed_as('t.') }
    #it { 't,'.should be_parsed_as('t,') }
    it { '0t'.should be_parse_error }
    it { '@1t'.should be_parse_error }
    it { '-1t'.should be_parse_error }
    it { '.1t'.should be_parse_error }
    it { ',1t'.should be_parse_error }
    it { '0 '.should be_parse_error }
    it { '-1 '.should be_parse_error }
    it { '.t'.should be_parse_error }
    it { '*t'.should be_parse_error }
    it { '@t'.should be_parse_error }
    it { '$t'.should be_parse_error }
    it { '{t'.should be_parse_error }
    it { '}t'.should be_parse_error }
    it { '$t'.should be_parse_error }
    it { 't$'.should be_parse_error }
    it { 't:'.should be_parse_error }
    it { 't;'.should be_parse_error }
    it { 't?'.should be_parse_error }
    it { 't+'.should be_parse_error }
    it { 't='.should be_parse_error }
    it { 't^'.should be_parse_error }
    it { 't`'.should be_parse_error }
    it { 't/'.should be_parse_error }
    it { 't~'.should be_parse_error }
    it { 't|'.should be_parse_error }
    it { 't>'.should be_parse_error }
    it { 't<'.should be_parse_error }
    it { 't('.should be_parse_error }
    it { 't['.should be_parse_error }
    it { 't{'.should be_parse_error }
  end

  context 'embedded ruby code' do
    it { '"${v1}"'.should be_parsed_as(v1.to_s) }
    it { '"${v2}"'.should be_parsed_as(v2.to_s) }
    it { '"${v3}"'.should be_parsed_as(v3.to_s) }
    it { '"${1+1}"'.should be_parsed_as("2") }
    it { '"${}"'.should be_parsed_as("") }
    it { '"t${v1}"'.should be_parsed_as("t"+v1.to_s) }
    it { '"${v1}t"'.should be_parsed_as(v1.to_s+"t") }
    it { '"t${v1}t"'.should be_parsed_as("t"+v1.to_s+"t") }
    it { '"${"}"}"'.should be_parsed_as("}") }
    it { "\"${#}\"".should be_parse_error }
    it { "\"${\n=begin\n}\"".should be_parse_error }
  end

  context 'array' do
    it { '[]'.should be_parsed_as([]) }
    it { '[1]'.should be_parsed_as([1]) }
    it { '[1,2]'.should be_parsed_as([1,2]) }
    it { '[1, 2]'.should be_parsed_as([1,2]) }
    it { '[ 1 , 2 ]'.should be_parsed_as([1,2]) }
    it { '[1,2,]'.should be_parsed_as([1,2]) }
    it { '[ 1 , 2 , ]'.should be_parsed_as([1,2]) }
    it { "[\n1\n,\n2\n]".should be_parsed_as([1,2]) }
    it { "[\n1\n,\n2\n,\n]".should be_parsed_as([1,2]) }
    it { '[a]'.should be_parsed_as(["a"]) }
    it { '[a,b]'.should be_parsed_as(["a","b"]) }
    it { '[a,b,]'.should be_parsed_as(["a","b"]) }
    it { '[ a , b ]'.should be_parsed_as(["a","b"]) }
    it { '[ a , b , ]'.should be_parsed_as(["a","b"]) }
    it { "[\na\n,\nb\n]".should be_parsed_as(["a","b"]) }
    it { "[\na\n,\nb\n,\n]".should be_parsed_as(["a","b"]) }
    it { '[ab,cd]'.should be_parsed_as(["ab","cd"]) }
  end

  context 'map' do
    it { '{}'.should be_parsed_as({}) }
    it { '{a:1}'.should be_parsed_as({"a"=>1}) }
    it { '{a:1,b:2}'.should be_parsed_as({"a"=>1,"b"=>2}) }
    it { '{ a : 1 , b : 2 }'.should be_parsed_as({"a"=>1,"b"=>2}) }
    it { '{a:1,b:2,}'.should be_parsed_as({"a"=>1,"b"=>2}) }
    it { '{ a : 1 , b : 2 , }'.should be_parsed_as({"a"=>1,"b"=>2}) }
    it { "{\na\n:\n1\n,\nb\n:\n2\n}".should be_parsed_as({"a"=>1,"b"=>2}) }
    it { "{\na\n:\n1\n,\nb\n:\n2\n,\n}".should be_parsed_as({"a"=>1,"b"=>2}) }
    it { '{a:b}'.should be_parsed_as({"a"=>"b"}) }
    it { '{a:b,c:d}'.should be_parsed_as({"a"=>"b","c"=>"d"}) }
    it { '{ a : b , c : d }'.should be_parsed_as({"a"=>"b","c"=>"d"}) }
    it { '{ a : b , c : d , }'.should be_parsed_as({"a"=>"b","c"=>"d"}) }
    it { "{\na\n:\nb\n,\nc\n:\nd\n}".should be_parsed_as({"a"=>"b","c"=>"d"}) }
    it { "{\na\n:\nb\n,\nc\n:\nd\n,\n}".should be_parsed_as({"a"=>"b","c"=>"d"}) }
  end
end

