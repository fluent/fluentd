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
          assert_nil(proxy.init)
          assert_nil(proxy.required)
          assert_false(proxy.required?)
          assert_nil(proxy.multi)
          assert_true(proxy.multi?)
        end

        test 'can specify param_name/required/multi with optional arguments' do
          proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: 'sections', init: true, required: false, multi: true)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_false(proxy.required)
          assert_false(proxy.required?)
          assert_true(proxy.multi)
          assert_true(proxy.multi?)

          proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, init: false, required: true, multi: false)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
        end
        test 'raise error if both of init and required are true' do
          assert_raise "init and required are exclusive" do
            Fluent::Config::ConfigureProxy.new(:section, init: true, required: true)
          end
        end
      end

      sub_test_case '#merge' do
        test 'generate a new instance which values are overwritten by the argument object' do
          proxy = p1 = Fluent::Config::ConfigureProxy.new(:section)
          assert_equal(:section, proxy.name)
          assert_equal(:section, proxy.param_name)
          assert_nil(proxy.init)
          assert_nil(proxy.required)
          assert_false(proxy.required?)
          assert_nil(proxy.multi)
          assert_true(proxy.multi?)

          p2 = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, init: false, required: true, multi: false)
          proxy = p1.merge(p2)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_false(proxy.init)
          assert_false(proxy.init?)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
        end

        test 'does not overwrite with argument object without any specifications of required/multi' do
          p1 = Fluent::Config::ConfigureProxy.new(:section1)
          p2 = Fluent::Config::ConfigureProxy.new(:section2, param_name: :sections, init: false, required: true, multi: false)
          p3 = Fluent::Config::ConfigureProxy.new(:section3)
          proxy = p1.merge(p2).merge(p3)
          assert_equal(:section3, proxy.name)
          assert_equal(:section3, proxy.param_name)
          assert_false(proxy.init)
          assert_false(proxy.init?)
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

      sub_test_case '#config_param without default values cause error if section is configured as init:true' do
        test 'with simple config_param with default value' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          assert_nothing_raised do
            proxy.config_section :subsection, init: true do
              config_param :param1, :integer, default: 1
            end
          end
        end
        test 'with simple config_param without default value' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          assert_raise ArgumentError do
            proxy.config_section :subsection, init: true do
              config_param :param1, :integer
            end
          end
        end
        test 'with config_param with config_set_default' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          assert_nothing_raised do
            proxy.config_section :subsection, init: true do
              config_param :param1, :integer
              config_set_default :param1, 1
            end
          end
        end
        test 'with config_argument' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          assert_raise ArgumentError do
            proxy.config_section :subsection, init: true do
              config_argument :param0, :string
            end
          end
        end
        test 'with config_argument with default value' do
          proxy = Fluent::Config::ConfigureProxy.new(:section)
          assert_nothing_raised do
            proxy.config_section :subsection, init: true do
              config_argument :param0, :string, default: ''
            end
          end
        end
      end

      sub_test_case '#config_set_desc' do
        setup do
          @proxy = Fluent::Config::ConfigureProxy.new(:section)
        end

        test 'does not permit description specification twice w/ :desc option' do
          @proxy.config_param(:name, :string, desc: "description")
          assert_raise(ArgumentError) { @proxy.config_set_desc(:name, "description2") }
        end

        test 'does not permit description specification twice' do
          @proxy.config_param(:name, :string)
          @proxy.config_set_desc(:name, "description")
          assert_raise(ArgumentError) { @proxy.config_set_desc(:name, "description2") }
        end
      end

      sub_test_case '#desc' do
        setup do
          @proxy = Fluent::Config::ConfigureProxy.new(:section)
        end

        test 'permit to specify description twice' do
          @proxy.desc("description1")
          @proxy.desc("description2")
          @proxy.config_param(:name, :string)
          assert_equal("description2", @proxy.descriptions[:name])
        end

        test 'does not permit description specification twice' do
          @proxy.desc("description1")
          assert_raise(ArgumentError) do
            @proxy.config_param(:name, :string, desc: "description2")
          end
        end
      end

      sub_test_case '#dump' do
        setup do
          @proxy = Fluent::Config::ConfigureProxy.new(:section)
        end

        test 'empty proxy' do
          assert_equal("", @proxy.dump)
        end

        test 'plain proxy w/o default value' do
          @proxy.config_param(:name, :string)
          assert_equal(<<CONFIG, @proxy.dump)
name: string: <nil>
CONFIG
        end

        test 'plain proxy w/ default value' do
          @proxy.config_param(:name, :string, default: "name1")
          assert_equal(<<CONFIG, @proxy.dump)
name: string: <"name1">
CONFIG
        end

        test 'plain proxy w/ default value using config_set_default' do
          @proxy.config_param(:name, :string)
          @proxy.config_set_default(:name, "name1")
          assert_equal(<<CONFIG, @proxy.dump)
name: string: <"name1">
CONFIG
        end

        test 'single sub proxy' do
          @proxy.config_section(:sub) do
            config_param(:name, :string, default: "name1")
          end
          assert_equal(<<CONFIG, @proxy.dump)
sub
 name: string: <"name1">
CONFIG
        end

        test 'nested sub proxy' do
          @proxy.config_section(:sub) do
            config_param(:name1, :string, default: "name1")
            config_param(:name2, :string, default: "name2")
            config_section(:sub2) do
              config_param(:name3, :string, default: "name3")
              config_param(:name4, :string, default: "name4")
            end
          end
          assert_equal(<<CONFIG, @proxy.dump)
sub
 name1: string: <"name1">
 name2: string: <"name2">
 sub2
  name3: string: <"name3">
  name4: string: <"name4">
CONFIG
        end

        sub_test_case 'w/ description' do
          test 'single proxy' do
            @proxy.config_param(:name, :string, desc: "description for name")
          assert_equal(<<CONFIG, @proxy.dump)
name: string: <nil> # description for name
CONFIG
          end

          test 'single proxy using config_set_desc' do
            @proxy.config_param(:name, :string)
            @proxy.config_set_desc(:name, "description for name")
          assert_equal(<<CONFIG, @proxy.dump)
name: string: <nil> # description for name
CONFIG
          end

          test 'sub proxy' do
            @proxy.config_section(:sub) do
              config_param(:name1, :string, default: "name1", desc: "desc1")
              config_param(:name2, :string, default: "name2", desc: "desc2")
              config_section(:sub2) do
                config_param(:name3, :string, default: "name3")
                config_param(:name4, :string, default: "name4", desc: "desc4")
              end
            end
            assert_equal(<<CONFIG, @proxy.dump)
sub
 name1: string: <"name1"> # desc1
 name2: string: <"name2"> # desc2
 sub2
  name3: string: <"name3">
  name4: string: <"name4"> # desc4
CONFIG
          end

          test 'sub proxy w/ desc method' do
            @proxy.config_section(:sub) do
              desc("desc1")
              config_param(:name1, :string, default: "name1")
              config_param(:name2, :string, default: "name2", desc: "desc2")
              config_section(:sub2) do
                config_param(:name3, :string, default: "name3")
                desc("desc4")
                config_param(:name4, :string, default: "name4")
              end
            end
            assert_equal(<<CONFIG, @proxy.dump)
sub
 name1: string: <"name1"> # desc1
 name2: string: <"name2"> # desc2
 sub2
  name3: string: <"name3">
  name4: string: <"name4"> # desc4
CONFIG
          end
        end
      end
    end
  end
end
