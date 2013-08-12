require_relative "./helper"

require 'fluentd/config/element'
require "fluentd/config/dsl"

DSL_CONFIG_EXAMPLE = %q[
worker {
  hostname = "myhostname"

  (0..9).each { |i|
    source {
      type :tail
      path "/var/log/httpd/access.part#{i}.log"

      filter ('bar.**') {
        type :hoge
        val1 "moge"
        val2 ["foo", "bar", "baz"]
        val3 10
        id :hoge

        subsection {
          foo "bar"
        }
        subsection {
          foo "baz"
        }
      }

      filter ('foo.**') {
        type "pass"
      }

      match ('{foo,bar}.**') {
        type "file"
        path "/var/log/httpd/access.#{hostname}.#{i}.log"
      }
    }
  }
}
]

describe Fluentd::Config::DSL::DSLParser do
  include_context 'config_helper'

  def parse_dsl(text)
    Fluentd::Config::DSL::DSLParser.parse(text)
  end

  def e(name, arg='', attrs={}, elements=[])
    Fluentd::Config::Element.new(name, arg, attrs, elements)
  end

  root = nil

  describe '.parse' do

    it 'makes root element' do
      root = parse_dsl(DSL_CONFIG_EXAMPLE)

      expect(root.name).to eql('ROOT')
      expect(root.arg).to be_nil
      expect(root.keys.size).to eql(0)
      expect(root.elements.size).to eql(1)
    end

    it 'makes worker element for worker tag' do
      worker = root.elements.first

      expect(worker.name).to eql('worker')
      expect(worker.arg).to be_nil
      expect(worker.keys.size).to eql(0)
      expect(worker.elements.size).to eql(10)
    end

    it 'makes subsections for blocks, with variable substitution' do
      ele4 = root.elements.first.elements[4]

      expect(ele4.name).to eql('source')
      expect(ele4.arg).to be_nil
      expect(ele4.keys.size).to eql(2)
      expect(ele4['type']).to eql('tail')
      expect(ele4['path']).to eql("/var/log/httpd/access.part4.log")
    end

    it 'makes user-defined sections with blocks' do
      filter0 = root.elements.first.elements[4].elements.first

      expect(filter0.name).to eql('filter')
      expect(filter0.arg).to eql('bar.**')
      expect(filter0['type']).to eql('hoge')
      expect(filter0['val1']).to eql('moge')
      expect(filter0['val2']).to eql(['foo', 'bar', 'baz'])
      expect(filter0['val3']).to eql(10)
      expect(filter0['id']).to eql('hoge')

      expect(filter0.elements.size).to eql(2)
      expect(filter0.elements[0].name).to eql('subsection')
      expect(filter0.elements[0]['foo']).to eql('bar')
      expect(filter0.elements[1].name).to eql('subsection')
      expect(filter0.elements[1]['foo']).to eql('baz')
    end

    it 'makes values with user-assigned variable substitutions' do
      match0 = root.elements.first.elements[4].elements.last

      expect(match0.name).to eql('match')
      expect(match0.arg).to eql('{foo,bar}.**')
      expect(match0['type']).to eql('file')
      expect(match0['path']).to eql('/var/log/httpd/access.myhostname.4.log')
    end
  end

end
