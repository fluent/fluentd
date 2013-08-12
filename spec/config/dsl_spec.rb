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

  describe '.parse' do
    it 'can parse simple dsl style configuration' do
      root = parse_dsl(DSL_CONFIG_EXAMPLE)
      expect(root.name).to eql('ROOT')
      expect(root.arg).to be_nil
      expect(root.keys.size).to eql(0)
      expect(root.elements.size).to eql(1)

      worker = root.elements.first
      expect(worker.name).to eql('worker')
      expect(worker.arg).to be_nil
      expect(worker.keys.size).to eql(0)
      expect(worker.elements.size).to eql(10)

      ele4 = worker.elements[4]
      expect(ele4.name).to eql('source')
      expect(ele4.arg).to be_nil
      expect(ele4.keys.size).to eql(2)
      expect(ele4['type']).to eql('tail')
      expect(ele4['path']).to eql("/var/log/httpd/access.part4.log")
    end
  end

end
