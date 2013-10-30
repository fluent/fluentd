require "json"
require "config/helper"
require "fluentd/config_error"
require "fluentd/config/basic_parser"
require "fluentd/config/literal_parser"
require "fluentd/config/parser"

describe Fluentd::Config::Parser do
  include_context 'config_helper'

  def parse_text(text)
    basepath = File.expand_path(File.dirname(__FILE__)+'/../../')
    Fluentd::Config::Parser.parse(text, '(test)', basepath, nil)
  end

  def root(*elements)
    if elements.first.is_a?(Fluentd::Config::Element)
      attrs = {}
    else
      attrs = elements.shift || {}
    end
    Fluentd::Config::Element.new('ROOT', '', attrs, elements)
  end

  def e(name, arg='', attrs={}, elements=[])
    Fluentd::Config::Element.new(name, arg, attrs, elements)
  end

  describe 'attribute parsing' do
    it "parses attributes" do
      %[
        k1 v1
        k2 v2
      ].should be_parsed_as("k1"=>"v1", "k2"=>"v2")
    end

    it "parses attribute key always string" do
      "1 1".should be_parsed_as("1" => 1)
    end

    [
      "_.%$!,",
      "/=~-~@`:?",
      "()*{}.[]",
    ].each do |k|
      it "parses a value with symbol #{k.inspect} to #{k.inspect}" do
        "k1 1".should be_parsed_as("k1" => 1)
      end
    end

    it "ignores spacing around value" do
      "  k1     a    ".should be_parsed_as("k1" => "a")
    end

    it "allows spaces in value" do
      "k1 a  b  c".should be_parsed_as("k1" => "a  b  c")
    end

    it "ignores comments after value" do
      "  k1 a#comment".should be_parsed_as("k1" => "a")
    end

    it "allows # in value if quoted" do
      '  k1 "a#comment"'.should be_parsed_as("k1" => "a#comment")
    end

    it "rejects characters after quoted string" do
      '  k1 "a" 1'.should be_parse_error
    end
  end

  describe 'element parsing' do
    it do
      "".should be_parsed_as(root)
    end

    it "accepts empty element" do
      %[
        <test>
        </test>
      ].should be_parsed_as(
        root(
          e("test")
        )
      )
    end

    it "accepts argument and attributes" do
      %[
        <test var>
          key val
        </test>
      ].should be_parsed_as(root(
          e("test", 'var', {'key'=>"val"})
        ))
    end

    it "accepts nested elements" do
      %[
        <test var>
          key 1
          <nested1>
          </nested1>
          <nested2>
          </nested2>
        </test>
      ].should be_parsed_as(root(
          e("test", 'var', {'key'=>'val'}, [
            e('nested1'),
            e('nested2')
          ])
        ))
    end

    [
      "**",
      "*.*",
      "1",
      "_.%$!",
      "/",
      "()*{}.[]",
    ].each do |arg|
      it "parses element argument #{arg.inspect}" do
        %[
          <test #{arg}>
          </test>
        ].should be_parsed_as(root(
            e("test", arg)
          ))
      end
    end

    it "parses empty element argument to nil" do
      %[
        <test >
        </test>
      ].should be_parsed_as(root(
          e("test", nil)
        ))
    end

    it "ignores spacing around element argument" do
      %[
        <test    a    >
        </test>
      ].should be_parsed_as(root(
          e("test", "a")
        ))
    end

    it "considers comments in element argument" do
      %[
        <test #a>
        </test>
      ].should be_parse_error
    end

    it "requires line_end after begin tag" do
      %[
        <test></test>
      ].should be_parse_error
    end

    it "requires line_end after end tag" do
      %[
        <test>
        </test><test>
        </test>
      ].should be_parse_error
    end
  end

  describe '@include parsing' do
    # TODO
  end
end

