require "config/helper"
require "fluentd/config.rb"
require "fluentd/config/basic_parser"
require "fluentd/config/literal_parser"
require "fluentd/config/parser"

describe Fluentd::Config::Parser do
  include_context 'config_helper'

  TestConfigParserContext = Struct.new(:v1, :v2, :v3)

  let(:v1) { 'test' }
  let(:v2) { true }
  let(:v3) { nil }

  let(:eval_context) { TestConfigParserContext.new(v1, v2, v3) }

  def parse_text(text)
    basepath = File.expand_path(File.dirname(__FILE__)+'/../../')
    Fluentd::Config::Parser.parse(text, '(test)', basepath, eval_context)
  end

  def root(attrs={}, elements=[])
    Fluentd::Config::Element.new('ROOT', '', attrs, elements)
  end

  def e(name, arg='', attrs={}, elements=[])
    Fluentd::Config::Element.new(name, arg, attrs, elements)
  end

  context 'key-value' do
    it { "k1 1".should be_parsed_as("k1"=>1) }
    it { "k1 v1".should be_parsed_as("k1"=>"v1") }
    it { "k1 v1".should be_parsed_as("k1"=>"v1") }
    it { "k1 v1\nk2 v2".should be_parsed_as("k1"=>"v1", "k2"=>"v2") }
    it { "k1 v1;k2 v2".should be_parsed_as("k1"=>"v1", "k2"=>"v2") }
    it { "k1 v1 ; k2 v2".should be_parsed_as("k1"=>"v1", "k2"=>"v2") }
    it { "1 1".should be_parsed_as("1"=>1) }  # map key is allowed to be digits
  end

  #context 'compatible key-value' do
  #  it { "k1 v1".should be_parsed_as(root({'k1'=>'v1'})) }
  #  it { "k1 v1 #".should be_parsed_as(root({'k1'=>'v1'})) }
  #  it { "k1 v1#".should be_parsed_as(root({'k1'=>'v1'})) }
  #  it { "k1 v1#x".should be_parsed_as(root({'k1'=>'v1'})) }
  #  it { "k1".should be_parsed_as(root({'k1'=>''})) }
  #  it { "k1 #".should be_parsed_as(root({'k1'=>''})) }
  #  it { "k1#".should be_parsed_as(root({'k1'=>''})) }
  #  it { "k1#x".should be_parsed_as(root({'k1'=>''})) }
  #end

  context 'simple' do
    it { "".should be_parsed_as(root) }

    it { "<test>\n</test>".should be_parsed_as(
          root(
            {},
            [e("test")]
          )) }

    it { "<test></test>".should be_parsed_as(
          root(
            {},
            [e("test")]
          )) }

    it { "<test var>\n</test>".should be_parsed_as(
          root(
            {},
            [e("test", 'var')]
          )) }

    it { "<test var>\nkey 1\n</test>".should be_parsed_as(
          root(
            {},
            [e("test", 'var', {'key'=>'val'})]
          )) }

    it { "<test var>\nkey 1\n<nested>\n</nested>\n</test>".should be_parsed_as(
          root(
            {},
            [e("test", 'var', {'key'=>'val'}, e('nested'))]
          )) }
  end

  context 'element arg' do
    it { "<test **></test>".should be_parsed_as(
          root(
            {},
            [e("test", '**')]
          )) }

    it { "<test *.*></test>".should be_parsed_as(
          root(
            {},
            [e("test", '*.*')]
          )) }
  end

  # TODO more
end

