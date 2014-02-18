require 'fluent/plugin_spec_helper'
require_relative 'util/out_retag'

include Fluent::PluginSpecHelper

describe Fluent::RetagOutput do
  config = %[
    type retag
    tag  yyy
  ]
  driver = generate_driver(Fluent::RetagOutput, config)

  it 'copy events' do
    driver.run do |d|
      t = Time.now.to_i
      d.with('xxx', t) do |d|
        d.pitch({"f1"=>"data1", "f2"=>"data2"})
        d.pitch({"f1"=>"data1", "f2"=>"data2"})
        d.pitch({"f1"=>"data1", "f2"=>"data2"})
        d.pitch({"f1"=>"data1", "f2"=>"data2"})
      end
    end

    expect(driver.events.keys.size).to eql(1)
    expect(driver.events.keys.first).to eql('yyy')
    expect(driver.events['yyy'].size).to eql(4)
    driver.events['yyy'].each do |e|
      expect(e.r).to eql({"f1"=>"data1", "f2"=>"data2"})
    end

    # p driver.instance.log.logs
    expect(driver.instance.log.logs.size).to eql(4)
  end
end
