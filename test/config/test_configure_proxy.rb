require_relative '../helper'
require 'fluent/config/configure_proxy'

module Fluent::Config
  class TestConfigureProxy < ::Test::Unit::TestCase
    sub_test_case 'to generate a instance' do
      sub_test_case '#initialize' do
        test 'has default values' do
          proxy = Fluent::Config::ConfigureProxy.new('section')
          assert_equal(:section, proxy.name)

          proxy = Fluent::Config::ConfigureProxy.new(:section)
          assert_equal(:section, proxy.name)
          assert_equal(:section, proxy.param_name)
          assert_nil(proxy.required)
          assert_false(proxy.required?)
          assert_nil(proxy.multi)
          assert_true(proxy.multi?)
        end

        test 'can specify param_name/required/multi with optional arguments' do
          proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: 'sections', required: false, multi: true)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_false(proxy.required)
          assert_false(proxy.required?)
          assert_true(proxy.multi)
          assert_true(proxy.multi?)

          proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, required: true, multi: false)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
        end
      end

      sub_test_case '#merge' do
        test 'generate a new instance which values are overwritten by the argument object' do
          proxy = p1 = Fluent::Config::ConfigureProxy.new(:section)
          assert_equal(:section, proxy.name)
          assert_equal(:section, proxy.param_name)
          assert_nil(proxy.required)
          assert_false(proxy.required?)
          assert_nil(proxy.multi)
          assert_true(proxy.multi?)

          p2 = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, required: true, multi: false)
          proxy = p1.merge(p2)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
        end

        test 'does not overwrite with argument object without any specifications of required/multi' do
          p1 = Fluent::Config::ConfigureProxy.new(:section1)
          p2 = Fluent::Config::ConfigureProxy.new(:section2, param_name: :sections, required: true, multi: false)
          p3 = Fluent::Config::ConfigureProxy.new(:section3)
          proxy = p1.merge(p2).merge(p3)
          assert_equal(:section3, proxy.name)
          assert_equal(:section3, proxy.param_name)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
        end
      end

      sub_test_case '#config_param / #config_set_default / #config_argument' do
        test 'does not permit config_set_default for param w/ :default option' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          proxy.config_param(:name, :string, default: "name1")
          assert_raise(ArgumentError) { proxy.config_set_default(:name, "name2") }
        end

        test 'does not permit default value specification twice' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          proxy.config_param(:name, :string)
          proxy.config_set_default(:name, "name1")
          assert_raise(ArgumentError) { proxy.config_set_default(:name, "name2") }
        end

        test 'does not permit default value specification twice, even on config_argument' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          proxy.config_param(:name, :string)
          proxy.config_set_default(:name, "name1")

          proxy.config_argument(:name)
          assert_raise(ArgumentError) { proxy.config_argument(:name, default: "name2") }
        end
      end
    end
  end
end
