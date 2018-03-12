require_relative '../helper'
require 'fluent/plugin/input'
require 'fluent/test/driver/input'

module ConfigurationForPlugins
  class AllBooleanParams < Fluent::Plugin::Input
    config_param :flag1, :bool, default: true
    config_param :flag2, :bool, default: true
    config_param :flag3, :bool, default: false
    config_param :flag4, :bool, default: false

    config_section :child, param_name: :children, multi: true, required: true do
      config_param :flag1, :bool, default: true
      config_param :flag2, :bool, default: true
      config_param :flag3, :bool, default: false
      config_param :flag4, :bool, default: false
    end
  end

  class BooleanParamsWithoutValue < ::Test::Unit::TestCase
    CONFIG = <<CONFIG
    flag1 
    flag2 # yaaaaaaaaaay
    flag3 
    flag4 # yaaaaaaaaaay
    <child>
      flag1
      flag2 # yaaaaaaaaaay
      flag3
      flag4 # yaaaaaaaaaay
    </child>
    <child>
      flag1 # yaaaaaaaaaay
      flag2
      flag3 # yaaaaaaaaaay
      flag4
    </child>
    # with following whitespace
    <child>
      flag1 
      flag2 
      flag3 
      flag4 
    </child>
CONFIG

    test 'create plugin via driver' do
      d = Fluent::Test::Driver::Input.new(AllBooleanParams)
      d.configure(CONFIG)
      assert_equal([true] * 4, [d.instance.flag1, d.instance.flag2, d.instance.flag3, d.instance.flag4])
      num_of_sections = 3
      assert_equal num_of_sections, d.instance.children.size
      assert_equal([true] * (num_of_sections * 4), d.instance.children.map{|c| [c.flag1, c.flag2, c.flag3, c.flag4]}.flatten)
    end
  end
end
