include Fluentd

describe Fluentd::Config::ValueParser do
  TestContext = Struct.new(:v1, :v2, :v3)

  let(:v1) { 'test' }
  let(:v2) { true }
  let(:v3) { nil }

  let(:context) { TestContext.new(v1, v2, v3) }

  class UnexpectedString < ConfigParseError
  end

  def parse_value(text, context)
    ss = StringScanner.new(text)
    vp = Fluentd::Config::ValueParser.new(ss, context)
    v = vp.parse_value
    if ss.eos?
      v
    else
      raise UnexpectedString, ss.rest
    end
  end

  RSpec::Matchers.define :be_parsed_as do |obj|
    match do |text|
      v = parse_value(text, context)
      if obj.is_a?(Float)
        v.is_a?(Float) && (v == obj || (v.nan? && obj.nan?) || (v - obj).abs < 0.000001)
      else
        v == obj
      end
    end

    failure_message_for_should do |text|
      msg = parse_value(text, context).inspect rescue 'failed'
      "expected that #{text.inspect} would be a parsed as #{obj.inspect} but got #{msg}"
    end
  end

  RSpec::Matchers.define :be_parse_error do |obj|
    match do |text,context|
      begin
        parse_value(text, context)
        false
      rescue ConfigParseError
        true
      end
    end

    failure_message_for_should do |text|
      begin
        msg = parse_value(text, context).inspect
      rescue
        msg = $!.inspect
      end
      "expected that #{text.inspect} would cause a parse error but got #{msg}"
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

  context 'string' do
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
    it { '"\\${test}"'.should be_parsed_as("${test}") }
  end

  context 'embedded ruby code' do
    it { '"${v1}"'.should be_parsed_as(v1.to_s) }
    it { '"${v2}"'.should be_parsed_as(v2.to_s) }
    it { '"${v3}"'.should be_parsed_as(v3.to_s) }
    it { '"${1+1}"'.should be_parsed_as("2") }
    it { '"${}"'.should be_parsed_as("") }
    it { '"${"}"}"'.should be_parsed_as("}") }
    it { "\"${#}\"".should be_parse_error }
    it { "\"${\n=begin\n}\"".should be_parse_error }
  end

  # TODO array
  # TODO map
end

