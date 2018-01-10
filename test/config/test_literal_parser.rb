require_relative "../helper"
require_relative 'assertions'
require "fluent/config/error"
require "fluent/config/literal_parser"
require "fluent/config/v1_parser"
require 'json'

module Fluent::Config
  class TestLiteralParser < ::Test::Unit::TestCase
    def parse_text(text)
      basepath = File.expand_path(File.dirname(__FILE__)+'/../../')
      ss = StringScanner.new(text)
      parser = Fluent::Config::V1Parser.new(ss, basepath, "(test)", eval_context)
      parser.parse_literal
    end

    TestLiteralParserContext = Struct.new(:v1, :v2, :v3)

    def v1
      :test
    end

    def v2
      true
    end

    def v3
      nil
    end

    def eval_context
      @eval_context ||= TestLiteralParserContext.new(v1, v2, v3)
    end

    sub_test_case 'boolean parsing' do
      def test_true
        assert_text_parsed_as('true', "true")
      end
      def test_false
        assert_text_parsed_as('false', "false")
      end
      def test_trueX
        assert_text_parsed_as('trueX', "trueX")
      end
      def test_falseX
        assert_text_parsed_as('falseX', "falseX")
      end
    end

    sub_test_case 'integer parsing' do
      test('0') { assert_text_parsed_as('0', "0") }
      test('1') { assert_text_parsed_as('1', "1") }
      test('10') { assert_text_parsed_as('10', "10") }
      test('-1') { assert_text_parsed_as('-1', "-1") }
      test('-10') { assert_text_parsed_as('-10', "-10") }
      test('0 ') { assert_text_parsed_as('0', "0 ") }
      test(' -1 ') { assert_text_parsed_as("-1", ' -1 ') }
      # string
      test('01') { assert_text_parsed_as('01', "01") }
      test('00') { assert_text_parsed_as('00', "00") }
      test('-01') { assert_text_parsed_as('-01', "-01") }
      test('-00') { assert_text_parsed_as('-00', "-00") }
      test('0x61') { assert_text_parsed_as('0x61', "0x61") }
      test('0s') { assert_text_parsed_as('0s', "0s") }
    end

    sub_test_case 'float parsing' do
      test('1.1') { assert_text_parsed_as('1.1', "1.1") }
      test('0.1') { assert_text_parsed_as('0.1', "0.1") }
      test('0.0') { assert_text_parsed_as('0.0', "0.0") }
      test('-1.1') { assert_text_parsed_as('-1.1', "-1.1") }
      test('-0.1') { assert_text_parsed_as('-0.1', "-0.1") }
      test('1.10') { assert_text_parsed_as('1.10', "1.10") }
      # string
      test('12e8') { assert_text_parsed_as('12e8', "12e8") }
      test('12.1e7') { assert_text_parsed_as('12.1e7', "12.1e7") }
      test('-12e8') { assert_text_parsed_as('-12e8', "-12e8") }
      test('-12.1e7') { assert_text_parsed_as('-12.1e7', "-12.1e7") }
      test('.0') { assert_text_parsed_as('.0', ".0") }
      test('.1') { assert_text_parsed_as('.1', ".1") }
      test('0.') { assert_text_parsed_as('0.', "0.") }
      test('1.') { assert_text_parsed_as('1.', "1.") }
      test('.0a') { assert_text_parsed_as('.0a', ".0a") }
      test('1.a') { assert_text_parsed_as('1.a', "1.a") }
      test('0@') { assert_text_parsed_as('0@', "0@") }
    end

    sub_test_case 'float keywords parsing' do
      test('NaN') { assert_text_parsed_as('NaN', "NaN") }
      test('Infinity') { assert_text_parsed_as('Infinity', "Infinity") }
      test('-Infinity') { assert_text_parsed_as('-Infinity', "-Infinity") }
      test('NaNX') { assert_text_parsed_as('NaNX', "NaNX") }
      test('InfinityX') { assert_text_parsed_as('InfinityX', "InfinityX") }
      test('-InfinityX') { assert_text_parsed_as('-InfinityX', "-InfinityX") }
    end

    sub_test_case 'double quoted string' do
      test('""') { assert_text_parsed_as("", '""') }
      test('"text"') { assert_text_parsed_as("text", '"text"') }
      test('"\\""') { assert_text_parsed_as("\"", '"\\""') }
      test('"\\t"') { assert_text_parsed_as("\t", '"\\t"') }
      test('"\\n"') { assert_text_parsed_as("\n", '"\\n"') }
      test('"\\r\\n"') { assert_text_parsed_as("\r\n", '"\\r\\n"') }
      test('"\\f\\b"') { assert_text_parsed_as("\f\b", '"\\f\\b"') }
      test('"\\.t"') { assert_text_parsed_as(".t", '"\\.t"') }
      test('"\\$t"') { assert_text_parsed_as("$t", '"\\$t"') }
      test('"\\"') { assert_text_parsed_as("#t", '"\\#t"') }
      test('"\\0"') { assert_text_parsed_as("\0", '"\\0"') }
      test('"\\z"') { assert_parse_error('"\\z"') }  # unknown escaped character
      test('"\\1"') { assert_parse_error('"\\1"') }  # unknown escaped character
      test('"t') { assert_parse_error('"t') }  # non-terminated quoted character
      test("\"t\nt\"") { assert_text_parsed_as("t\nt", "\"t\nt\"" ) } # multiline string
      test("\"t\\\nt\"") { assert_text_parsed_as("tt", "\"t\\\nt\"" ) } # multiline string
      test('t"') { assert_text_parsed_as('t"', 't"') }
      test('"."') { assert_text_parsed_as('.', '"."') }
      test('"*"') { assert_text_parsed_as('*', '"*"') }
      test('"@"') { assert_text_parsed_as('@', '"@"') }
      test('"\\#{test}"') { assert_text_parsed_as("\#{test}", '"\\#{test}"') }
      test('"$"') { assert_text_parsed_as('$', '"$"') }
      test('"$t"') { assert_text_parsed_as('$t', '"$t"') }
      test('"$}"') { assert_text_parsed_as('$}', '"$}"') }
      test('"\\\\"') { assert_text_parsed_as("\\", '"\\\\"') }
      test('"\\["') { assert_text_parsed_as("[", '"\\["') }
    end

    sub_test_case 'single quoted string' do
      test("''") { assert_text_parsed_as("", "''") }
      test("'text'") { assert_text_parsed_as("text", "'text'") }
      test("'\\''") { assert_text_parsed_as('\'', "'\\''") }
      test("'\\t'") { assert_text_parsed_as('\t', "'\\t'") }
      test("'\\n'") { assert_text_parsed_as('\n', "'\\n'") }
      test("'\\r\\n'") { assert_text_parsed_as('\r\n', "'\\r\\n'") }
      test("'\\f\\b'") { assert_text_parsed_as('\f\b', "'\\f\\b'") }
      test("'\\.t'") { assert_text_parsed_as('\.t', "'\\.t'") }
      test("'\\$t'") { assert_text_parsed_as('\$t', "'\\$t'") }
      test("'\\#t'") { assert_text_parsed_as('\#t', "'\\#t'") }
      test("'\\z'") { assert_text_parsed_as('\z', "'\\z'") }
      test("'\\0'") { assert_text_parsed_as('\0', "'\\0'") }
      test("'\\1'") { assert_text_parsed_as('\1', "'\\1'") }
      test("'t") { assert_parse_error("'t") }  # non-terminated quoted character
      test("t'") { assert_text_parsed_as("t'", "t'") }
      test("'.'") { assert_text_parsed_as('.', "'.'") }
      test("'*'") { assert_text_parsed_as('*', "'*'") }
      test("'@'") { assert_text_parsed_as('@', "'@'") }
      test(%q['#{test}']) { assert_text_parsed_as('#{test}', %q['#{test}']) }
      test("'$'") { assert_text_parsed_as('$', "'$'") }
      test("'$t'") { assert_text_parsed_as('$t', "'$t'") }
      test("'$}'") { assert_text_parsed_as('$}', "'$}'") }
      test("'\\\\'") { assert_text_parsed_as('\\', "'\\\\'") }
      test("'\\['") { assert_text_parsed_as('\[', "'\\['") }
    end

    sub_test_case 'nonquoted string parsing' do
      test("''") { assert_text_parsed_as(nil, '') }
      test('text') { assert_text_parsed_as('text', 'text') }
      test('\"') { assert_text_parsed_as('\"', '\"') }
      test('\t') { assert_text_parsed_as('\t', '\t') }
      test('\n') { assert_text_parsed_as('\n', '\n') }
      test('\r\n') { assert_text_parsed_as('\r\n', '\r\n') }
      test('\f\b') { assert_text_parsed_as('\f\b', '\f\b') }
      test('\.t') { assert_text_parsed_as('\.t', '\.t') }
      test('\$t') { assert_text_parsed_as('\$t', '\$t') }
      test('\#t') { assert_text_parsed_as('\#t', '\#t') }
      test('\z') { assert_text_parsed_as('\z', '\z') }
      test('\0') { assert_text_parsed_as('\0', '\0') }
      test('\1') { assert_text_parsed_as('\1', '\1') }
      test('.') { assert_text_parsed_as('.', '.') }
      test('*') { assert_text_parsed_as('*', '*') }
      test('@') { assert_text_parsed_as('@', '@') }
      test('#{test}') { assert_text_parsed_as('#{test}', '#{test}') }
      test('$') { assert_text_parsed_as('$', '$') }
      test('$t') { assert_text_parsed_as('$t', '$t') }
      test('$}') { assert_text_parsed_as('$}', '$}') }
      test('\\\\') { assert_text_parsed_as('\\\\', '\\\\') }
      test('\[') { assert_text_parsed_as('\[', '\[') }
      test('#foo') { assert_text_parsed_as('#foo', '#foo') } # not comment out
      test('foo#bar') { assert_text_parsed_as('foo#bar', 'foo#bar') } # not comment out
      test(' text') { assert_text_parsed_as('text', ' text') } # remove starting spaces
      test(' #foo') { assert_text_parsed_as('#foo', ' #foo') } # remove starting spaces
      test('foo #bar') { assert_text_parsed_as('foo', 'foo #bar') } # comment out
      test('foo\t#bar') { assert_text_parsed_as('foo', "foo\t#bar") } # comment out

      test('t') { assert_text_parsed_as('t', 't') }
      test('T') { assert_text_parsed_as('T', 'T') }
      test('_') { assert_text_parsed_as('_', '_') }
      test('T1') { assert_text_parsed_as('T1', 'T1') }
      test('_2') { assert_text_parsed_as('_2', '_2') }
      test('t0') { assert_text_parsed_as('t0', 't0') }
      test('t@') { assert_text_parsed_as('t@', 't@') }
      test('t-') { assert_text_parsed_as('t-', 't-') }
      test('t.') { assert_text_parsed_as('t.', 't.') }
      test('t+') { assert_text_parsed_as('t+', 't+') }
      test('t/') { assert_text_parsed_as('t/', 't/') }
      test('t=') { assert_text_parsed_as('t=', 't=') }
      test('t,') { assert_text_parsed_as('t,', 't,') }
      test('0t') { assert_text_parsed_as('0t', "0t") }
      test('@1t') { assert_text_parsed_as('@1t', '@1t') }
      test('-1t') { assert_text_parsed_as('-1t', '-1t') }
      test('.1t') { assert_text_parsed_as('.1t', '.1t') }
      test(',1t') { assert_text_parsed_as(',1t', ',1t') }
      test('.t') { assert_text_parsed_as('.t', '.t') }
      test('*t') { assert_text_parsed_as('*t', '*t') }
      test('@t') { assert_text_parsed_as('@t', '@t') }
      test('{t') { assert_parse_error('{t') }  # '{' begins map
      test('t{') { assert_text_parsed_as('t{', 't{') }
      test('}t') { assert_text_parsed_as('}t', '}t') }
      test('[t') { assert_parse_error('[t') }  # '[' begins array
      test('t[') { assert_text_parsed_as('t[', 't[') }
      test(']t') { assert_text_parsed_as(']t', ']t') }
      test('t:') { assert_text_parsed_as('t:', 't:') }
      test('t;') { assert_text_parsed_as('t;', 't;') }
      test('t?') { assert_text_parsed_as('t?', 't?') }
      test('t^') { assert_text_parsed_as('t^', 't^') }
      test('t`') { assert_text_parsed_as('t`', 't`') }
      test('t~') { assert_text_parsed_as('t~', 't~') }
      test('t|') { assert_text_parsed_as('t|', 't|') }
      test('t>') { assert_text_parsed_as('t>', 't>') }
      test('t<') { assert_text_parsed_as('t<', 't<') }
      test('t(') { assert_text_parsed_as('t(', 't(') }
    end

    sub_test_case 'embedded ruby code parsing' do
      test('"#{v1}"') { assert_text_parsed_as("#{v1}", '"#{v1}"') }
      test('"#{v2}"') { assert_text_parsed_as("#{v2}", '"#{v2}"') }
      test('"#{v3}"') { assert_text_parsed_as("#{v3}", '"#{v3}"') }
      test('"#{1+1}"') { assert_text_parsed_as("2", '"#{1+1}"') }
      test('"#{}"') { assert_text_parsed_as("", '"#{}"') }
      test('"t#{v1}"') { assert_text_parsed_as("t#{v1}", '"t#{v1}"') }
      test('"t#{v1}t"') { assert_text_parsed_as("t#{v1}t", '"t#{v1}t"') }
      test('"#{"}"}"') { assert_text_parsed_as("}", '"#{"}"}"') }
      test('"#{#}"') { assert_parse_error('"#{#}"') }  # error in embedded ruby code
      test("\"\#{\n=begin\n}\"") { assert_parse_error("\"\#{\n=begin\n}\"") }  # error in embedded ruby code
      test('"#{v1}foo#{v2}"') { assert_text_parsed_as("#{v1}foo#{v2}", '"#{v1}foo#{v2}"') }
      test('"#{1+1}foo#{2+2}bar"') { assert_text_parsed_as("#{1+1}foo#{2+2}bar", '"#{1+1}foo#{2+2}bar"') }
      test('"foo#{hostname}"') { assert_text_parsed_as("foo#{Socket.gethostname}", '"foo#{hostname}"') }
      test('"foo#{worker_id}"') {
        ENV.delete('SERVERENGINE_WORKER_ID')
        assert_text_parsed_as("foo", '"foo#{worker_id}"')
        ENV['SERVERENGINE_WORKER_ID'] = '1'
        assert_text_parsed_as("foo1", '"foo#{worker_id}"')
        ENV.delete('SERVERENGINE_WORKER_ID')
      }
    end

    sub_test_case 'array parsing' do
      test('[]') { assert_text_parsed_as_json([], '[]') }
      test('[1]') { assert_text_parsed_as_json([1], '[1]') }
      test('[1,2]') { assert_text_parsed_as_json([1,2], '[1,2]') }
      test('[1, 2]') { assert_text_parsed_as_json([1,2], '[1, 2]') }
      test('[ 1 , 2 ]') { assert_text_parsed_as_json([1,2], '[ 1 , 2 ]') }
      test('[1,2,]') { assert_parse_error('[1,2,]') } # TODO: Need trailing commas support?
      test("[\n1\n,\n2\n]") { assert_text_parsed_as_json([1,2], "[\n1\n,\n2\n]") }
      test('["a"]') { assert_text_parsed_as_json(["a"], '["a"]') }
      test('["a","b"]') { assert_text_parsed_as_json(["a","b"], '["a","b"]') }
      test('[ "a" , "b" ]') { assert_text_parsed_as_json(["a","b"], '[ "a" , "b" ]') }
      test("[\n\"a\"\n,\n\"b\"\n]") { assert_text_parsed_as_json(["a","b"], "[\n\"a\"\n,\n\"b\"\n]") }
      test('["ab","cd"]') { assert_text_parsed_as_json(["ab","cd"], '["ab","cd"]') }
      json_array_with_js_comment = <<EOA
[
 "a", // this is a
 "b", // this is b
 "c"  // this is c
]
EOA
      test(json_array_with_js_comment) { assert_text_parsed_as_json(["a","b","c"], json_array_with_js_comment) }
      json_array_with_comment = <<EOA
[
 "a", # this is a
 "b", # this is b
 "c"  # this is c
]
EOA
      test(json_array_with_comment) { assert_text_parsed_as_json(["a","b","c"], json_array_with_comment) }
      json_array_with_tailing_comma = <<EOA
[
 "a", # this is a
 "b", # this is b
 "c", # this is c
]
EOA
      test(json_array_with_tailing_comma) { assert_parse_error(json_array_with_tailing_comma) }
    end

    sub_test_case 'map parsing' do
      test('{}') { assert_text_parsed_as_json({}, '{}') }
      test('{"a":1}') { assert_text_parsed_as_json({"a"=>1}, '{"a":1}') }
      test('{"a":1,"b":2}') { assert_text_parsed_as_json({"a"=>1,"b"=>2}, '{"a":1,"b":2}') }
      test('{ "a" : 1 , "b" : 2 }') { assert_text_parsed_as_json({"a"=>1,"b"=>2}, '{ "a" : 1 , "b" : 2 }') }
      test('{"a":1,"b":2,}') { assert_parse_error('{"a":1,"b":2,}') } # TODO: Need trailing commas support?
      test('{\n\"a\"\n:\n1\n,\n\"b\"\n:\n2\n}') { assert_text_parsed_as_json({"a"=>1,"b"=>2}, "{\n\"a\"\n:\n1\n,\n\"b\"\n:\n2\n}") }
      test('{"a":"b"}') { assert_text_parsed_as_json({"a"=>"b"}, '{"a":"b"}') }
      test('{"a":"b","c":"d"}') { assert_text_parsed_as_json({"a"=>"b","c"=>"d"}, '{"a":"b","c":"d"}') }
      test('{ "a" : "b" , "c" : "d" }') { assert_text_parsed_as_json({"a"=>"b","c"=>"d"}, '{ "a" : "b" , "c" : "d" }') }
      test('{\n\"a\"\n:\n\"b\"\n,\n\"c\"\n:\n\"d\"\n}') { assert_text_parsed_as_json({"a"=>"b","c"=>"d"}, "{\n\"a\"\n:\n\"b\"\n,\n\"c\"\n:\n\"d\"\n}") }
      json_hash_with_comment = <<EOH
{
 "a": 1, # this is a
 "b": 2, # this is b
 "c": 3  # this is c
}
EOH
      test(json_hash_with_comment) { assert_text_parsed_as_json({"a"=>1,"b"=>2,"c"=>3}, json_hash_with_comment) }
    end
  end
end
