require_relative '../helper'
require 'fluent/config/configure_proxy'

module Fluent::Config
  class TestConfigureProxy < ::Test::Unit::TestCase
    setup do
      @type_lookup = ->(type) { Fluent::Configurable.lookup_type(type) }
    end

    sub_test_case 'to generate a instance' do
      sub_test_case '#initialize' do
        test 'has default values' do
          proxy = Fluent::Config::ConfigureProxy.new('section', type_lookup: @type_lookup)
          assert_equal(:section, proxy.name)

          proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
          assert_equal(:section, proxy.name)
          assert_nil(proxy.param_name)
          assert_equal(:section, proxy.variable_name)
          assert_false(proxy.root?)
          assert_nil(proxy.init)
          assert_nil(proxy.required)
          assert_false(proxy.required?)
          assert_nil(proxy.multi)
          assert_true(proxy.multi?)
        end

        test 'can specify param_name/required/multi with optional arguments' do
          proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: 'sections', init: true, required: false, multi: true, type_lookup: @type_lookup)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_equal(:sections, proxy.variable_name)
          assert_false(proxy.required)
          assert_false(proxy.required?)
          assert_true(proxy.multi)
          assert_true(proxy.multi?)

          proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, init: false, required: true, multi: false, type_lookup: @type_lookup)
          assert_equal(:section, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_equal(:sections, proxy.variable_name)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
        end
        test 'raise error if both of init and required are true' do
          assert_raise RuntimeError.new("init and required are exclusive") do
            Fluent::Config::ConfigureProxy.new(:section, init: true, required: true, type_lookup: @type_lookup)
          end
        end
      end

      sub_test_case '#merge' do
        test 'generate a new instance which values are overwritten by the argument object' do
          proxy = p1 = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
          assert_equal(:section, proxy.name)
          assert_nil(proxy.param_name)
          assert_equal(:section, proxy.variable_name)
          assert_nil(proxy.init)
          assert_nil(proxy.required)
          assert_false(proxy.required?)
          assert_nil(proxy.multi)
          assert_true(proxy.multi?)
          assert_nil(proxy.configured_in_section)

          p2 = Fluent::Config::ConfigureProxy.new(:section, init: false, required: true, multi: false, type_lookup: @type_lookup)
          proxy = p1.merge(p2)
          assert_equal(:section, proxy.name)
          assert_nil(proxy.param_name)
          assert_equal(:section, proxy.variable_name)
          assert_false(proxy.init)
          assert_false(proxy.init?)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
          assert_nil(proxy.configured_in_section)
        end

        test 'does not overwrite with argument object without any specifications of required/multi' do
          p1 = Fluent::Config::ConfigureProxy.new(:section1, param_name: :sections, type_lookup: @type_lookup)
          p1.configured_in_section = :subsection
          p2 = Fluent::Config::ConfigureProxy.new(:section2, init: false, required: true, multi: false, type_lookup: @type_lookup)
          p3 = Fluent::Config::ConfigureProxy.new(:section3, type_lookup: @type_lookup)
          proxy = p1.merge(p2).merge(p3)
          assert_equal(:section1, proxy.name)
          assert_equal(:sections, proxy.param_name)
          assert_equal(:sections, proxy.variable_name)
          assert_false(proxy.init)
          assert_false(proxy.init?)
          assert_true(proxy.required)
          assert_true(proxy.required?)
          assert_false(proxy.multi)
          assert_false(proxy.multi?)
          assert_equal :subsection, proxy.configured_in_section
        end

        test "does overwrite name of proxy for root sections which are used for plugins" do
          # latest plugin class shows actual plugin implementation
          p1 = Fluent::Config::ConfigureProxy.new('Fluent::Plugin::MyP1'.to_sym, root: true, required: true, multi: false, type_lookup: @type_lookup)
          p1.config_param :key1, :integer

          p2 = Fluent::Config::ConfigureProxy.new('Fluent::Plugin::MyP2'.to_sym, root: true, required: true, multi: false, type_lookup: @type_lookup)
          p2.config_param :key2, :string, default: "value2"

          merged = p1.merge(p2)

          assert_equal 'Fluent::Plugin::MyP2'.to_sym, merged.name
          assert_true merged.root?
        end
      end

      sub_test_case '#overwrite_defaults' do
        test 'overwrites only defaults with others defaults' do
          type_lookup = ->(type) { Fluent::Configurable.lookup_type(type) }
          p1 = Fluent::Config::ConfigureProxy.new(:mychild, type_lookup: type_lookup)
          p1.configured_in_section = :child
          p1.config_param(:k1a, :string)
          p1.config_param(:k1b, :string)
          p1.config_param(:k2a, :integer, default: 0)
          p1.config_param(:k2b, :integer, default: 0)
          p1.config_section(:sub1) do
            config_param :k3, :time, default: 30
          end

          p0 = Fluent::Config::ConfigureProxy.new(:myparent, type_lookup: type_lookup)
          p0.config_section(:child) do
            config_set_default :k1a, "v1a"
            config_param :k1b, :string, default: "v1b"
            config_set_default :k2a, 21
            config_param :k2b, :integer, default: 22
            config_section :sub1 do
              config_set_default :k3, 60
            end
          end

          p1.overwrite_defaults(p0.sections[:child])

          assert_equal "v1a", p1.defaults[:k1a]
          assert_equal "v1b", p1.defaults[:k1b]
          assert_equal 21, p1.defaults[:k2a]
          assert_equal 22, p1.defaults[:k2b]
          assert_equal 60, p1.sections[:sub1].defaults[:k3]
        end
      end

      sub_test_case '#configured_in' do
        test 'sets a section name which have configuration parameters of target plugin in owners configuration' do
          proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
          proxy.configured_in(:mysection)
          assert_equal :mysection, proxy.configured_in_section
        end

        test 'do not permit to be called twice' do
          proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
          proxy.configured_in(:mysection)
          assert_raise(ArgumentError) { proxy.configured_in(:myothersection) }
        end
      end

      sub_test_case '#config_param / #config_set_default / #config_argument' do
        setup do
          @proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
        end

        test 'handles configuration parameters without type as string' do
          @proxy.config_argument(:label)
          @proxy.config_param(:name)
          assert_equal :label, @proxy.argument[0]
          assert_equal :string, @proxy.argument[2][:type]
          assert_equal :string, @proxy.params[:name][1][:type]
        end

        data(
          default: [:default, nil],
          alias: [:alias, :alias_name_in_config],
          secret: [:secret, true],
          skip_accessor: [:skip_accessor, true],
          deprecated: [:deprecated, 'it is deprecated'],
          obsoleted: [:obsoleted, 'it is obsoleted'],
          desc: [:desc, "description"],
        )
        test 'always allow options for all types' do |(option, value)|
          opt = {option => value}
          assert_nothing_raised{ @proxy.config_argument(:param0, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p1, :string, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p2, :enum, list: [:a, :b, :c], **opt) }
          assert_nothing_raised{ @proxy.config_param(:p3, :integer, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p4, :float, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p5, :size, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p6, :bool, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p7, :time, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p8, :hash, **opt) }
          assert_nothing_raised{ @proxy.config_param(:p9, :array, **opt) }
          assert_nothing_raised{ @proxy.config_param(:pa, :regexp, **opt) }
        end

        data(string: :string, integer: :integer, float: :float, size: :size, bool: :bool, time: :time, hash: :hash, array: :array, regexp: :regexp)
        test 'deny list for non-enum types' do |type|
          assert_raise ArgumentError.new(":list is valid only for :enum type, but #{type}: arg") do
            @proxy.config_argument(:arg, type, list: [:a, :b])
          end
          assert_raise ArgumentError.new(":list is valid only for :enum type, but #{type}: p1") do
            @proxy.config_param(:p1, type, list: [:a, :b])
          end
        end

        data(string: :string, integer: :integer, float: :float, size: :size, bool: :bool, time: :time, regexp: :regexp)
        test 'deny value_type for non-hash/array types' do |type|
          assert_raise ArgumentError.new(":value_type is valid only for :hash and :array, but #{type}: arg") do
            @proxy.config_argument(:arg, type, value_type: :string)
          end
          assert_raise ArgumentError.new(":value_type is valid only for :hash and :array, but #{type}: p1") do
            @proxy.config_param(:p1, type, value_type: :integer)
          end
        end

        data(string: :string, integer: :integer, float: :float, size: :size, bool: :bool, time: :time, array: :array, regexp: :regexp)
        test 'deny symbolize_keys for non-hash types' do |type|
          assert_raise ArgumentError.new(":symbolize_keys is valid only for :hash, but #{type}: arg") do
            @proxy.config_argument(:arg, type, symbolize_keys: true)
          end
          assert_raise ArgumentError.new(":symbolize_keys is valid only for :hash, but #{type}: p1") do
            @proxy.config_param(:p1, type, symbolize_keys: true)
          end
        end

        data(string: :string, integer: :integer, float: :float, size: :size, bool: :bool, time: :time, hash: :hash, array: :array)
        test 'deny unknown options' do |type|
          assert_raise ArgumentError.new("unknown option 'required' for configuration parameter: arg") do
            @proxy.config_argument(:arg, type, required: true)
          end
          assert_raise ArgumentError.new("unknown option 'param_name' for configuration parameter: p1") do
            @proxy.config_argument(:p1, type, param_name: :yay)
          end
        end

        test 'desc gets string' do
          assert_nothing_raised do
            @proxy.config_param(:name, :string, desc: "it is description")
          end
          assert_raise ArgumentError.new("name1: desc must be a String, but Symbol") do
            @proxy.config_param(:name1, :string, desc: :yaaaaaaaay)
          end
        end

        test 'alias gets symbol' do
          assert_nothing_raised do
            @proxy.config_param(:name, :string, alias: :label)
          end
          assert_raise ArgumentError.new("name1: alias must be a Symbol, but String") do
            @proxy.config_param(:name1, :string, alias: 'label1')
          end
        end

        test 'secret gets true/false' do
          assert_nothing_raised do
            @proxy.config_param(:name1, :string, secret: false)
          end
          assert_nothing_raised do
            @proxy.config_param(:name2, :string, secret: true)
          end
          assert_raise ArgumentError.new("name3: secret must be true or false, but String") do
            @proxy.config_param(:name3, :string, secret: 'yes')
          end
          assert_raise ArgumentError.new("name4: secret must be true or false, but NilClass") do
            @proxy.config_param(:name4, :string, secret: nil)
          end
        end

        test 'symbolize_keys gets true/false' do
          assert_nothing_raised do
            @proxy.config_param(:data1, :hash, symbolize_keys: false)
          end
          assert_nothing_raised do
            @proxy.config_param(:data2, :hash, symbolize_keys: true)
          end
          assert_raise ArgumentError.new("data3: symbolize_keys must be true or false, but NilClass") do
            @proxy.config_param(:data3, :hash, symbolize_keys: nil)
          end
        end

        test 'value_type gets symbol' do
          assert_nothing_raised do
            @proxy.config_param(:data1, :array, value_type: :integer)
          end
          assert_raise ArgumentError.new("data2: value_type must be a Symbol, but Class") do
            @proxy.config_param(:data2, :array, value_type: Integer)
          end
        end

        test 'list gets an array of symbols' do
          assert_nothing_raised do
            @proxy.config_param(:proto1, :enum, list: [:a, :b])
          end
          assert_raise ArgumentError.new("proto2: enum parameter requires :list of Symbols") do
            @proxy.config_param(:proto2, :enum, list: nil)
          end
          assert_raise ArgumentError.new("proto3: enum parameter requires :list of Symbols") do
            @proxy.config_param(:proto3, :enum, list: ['a', 'b'])
          end
          assert_raise ArgumentError.new("proto4: enum parameter requires :list of Symbols") do
            @proxy.config_param(:proto4, :enum, list: [])
          end
        end

        test 'deprecated gets string' do
          assert_nothing_raised do
            @proxy.config_param(:name1, :string, deprecated: "use name2 instead")
          end
          assert_raise ArgumentError.new("name2: deprecated must be a String, but TrueClass") do
            @proxy.config_param(:name2, :string, deprecated: true)
          end
        end

        test 'obsoleted gets string' do
          assert_nothing_raised do
            @proxy.config_param(:name1, :string, obsoleted: "use name2 instead")
          end
          assert_raise ArgumentError.new("name2: obsoleted must be a String, but TrueClass") do
            @proxy.config_param(:name2, :string, obsoleted: true)
          end
        end

        test 'skip_accessor gets true/false' do
          assert_nothing_raised do
            @proxy.config_param(:format1, :string, skip_accessor: false)
          end
          assert_nothing_raised do
            @proxy.config_param(:format2, :string, skip_accessor: true)
          end
          assert_raise ArgumentError.new("format2: skip_accessor must be true or false, but String") do
            @proxy.config_param(:format2, :string, skip_accessor: 'yes')
          end
        end

        test 'list is required for :enum' do
          assert_nothing_raised do
            @proxy.config_param(:proto1, :enum, list: [:a, :b])
          end
          assert_raise ArgumentError.new("proto1: enum parameter requires :list of Symbols") do
            @proxy.config_param(:proto1, :enum, default: :a)
          end
        end

        test 'does not permit config_set_default for param w/ :default option' do
          @proxy.config_param(:name, :string, default: "name1")
          assert_raise(ArgumentError) { @proxy.config_set_default(:name, "name2") }
        end

        test 'does not permit default value specification twice' do
          @proxy.config_param(:name, :string)
          @proxy.config_set_default(:name, "name1")
          assert_raise(ArgumentError) { @proxy.config_set_default(:name, "name2") }
        end

        test 'does not permit default value specification twice, even on config_argument' do
          @proxy.config_param(:name, :string)
          @proxy.config_set_default(:name, "name1")

          @proxy.config_argument(:name)
          assert_raise(ArgumentError) { @proxy.config_argument(:name, default: "name2") }
        end
      end

      sub_test_case '#config_set_desc' do
        setup do
          @proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
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
          @proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
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

      sub_test_case '#dump_config_definition' do
        setup do
          @proxy = Fluent::Config::ConfigureProxy.new(:section, type_lookup: @type_lookup)
        end

        test 'empty proxy' do
          assert_equal({}, @proxy.dump_config_definition)
        end

        test 'plain proxy w/o default value' do
          @proxy.config_param(:name, :string)
          expected = {
            name: { type: :string, required: true }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        test 'plain proxy w/ default value' do
          @proxy.config_param(:name, :string, default: "name1")
          expected = {
            name: { type: :string, default: "name1", required: false }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        test 'plain proxy w/ default value using config_set_default' do
          @proxy.config_param(:name, :string)
          @proxy.config_set_default(:name, "name1")
          expected = {
            name: { type: :string, default: "name1", required: false }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        test 'plain proxy w/ argument' do
          @proxy.instance_eval do
            config_argument(:argname, :string)
            config_param(:name, :string, default: "name1")
          end
          expected = {
            argname: { type: :string, required: true, argument: true },
            name: { type: :string, default: "name1", required: false }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        test 'plain proxy w/ argument default value' do
          @proxy.instance_eval do
            config_argument(:argname, :string, default: "value")
            config_param(:name, :string, default: "name1")
          end
          expected = {
            argname: { type: :string, default: "value", required: false, argument: true },
            name: { type: :string, default: "name1", required: false }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        test 'plain proxy w/ argument overwriting default value' do
          @proxy.instance_eval do
            config_argument(:argname, :string)
            config_param(:name, :string, default: "name1")
            config_set_default(:argname, "value1")
          end
          expected = {
            argname: { type: :string, default: "value1", required: false, argument: true },
            name: { type: :string, default: "name1", required: false }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        test 'single sub proxy' do
          @proxy.config_section(:sub) do
            config_param(:name, :string, default: "name1")
          end
          expected = {
            sub: {
              alias: nil,
              multi: true,
              required: false,
              section: true,
              name: { type: :string, default: "name1", required: false }
            }
          }
          assert_equal(expected, @proxy.dump_config_definition)
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
          expected = {
            sub: {
              alias: nil,
              multi: true,
              required: false,
              section: true,
              name1: { type: :string, default: "name1", required: false },
              name2: { type: :string, default: "name2", required: false },
              sub2: {
                alias: nil,
                multi: true,
                required: false,
                section: true,
                name3: { type: :string, default: "name3", required: false },
                name4: { type: :string, default: "name4", required: false },
              }
            }
          }
          assert_equal(expected, @proxy.dump_config_definition)
        end

        sub_test_case 'w/ description' do
          test 'single proxy' do
            @proxy.config_param(:name, :string, desc: "description for name")
            expected = {
              name: { type: :string, desc: "description for name", required: true }
            }
            assert_equal(expected, @proxy.dump_config_definition)
          end

          test 'single proxy using config_set_desc' do
            @proxy.config_param(:name, :string)
            @proxy.config_set_desc(:name, "description for name")
            expected = {
              name: { type: :string, desc: "description for name", required: true }
            }
            assert_equal(expected, @proxy.dump_config_definition)
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
            expected = {
              sub: {
                alias: nil,
                multi: true,
                required: false,
                section: true,
                name1: { type: :string, default: "name1", desc: "desc1", required: false },
                name2: { type: :string, default: "name2", desc: "desc2", required: false },
                sub2: {
                  alias: nil,
                  multi: true,
                  required: false,
                  section: true,
                  name3: { type: :string, default: "name3", required: false },
                  name4: { type: :string, default: "name4", desc: "desc4", required: false },
                }
              }
            }
            assert_equal(expected, @proxy.dump_config_definition)
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
            expected = {
              sub: {
                alias: nil,
                multi: true,
                required: false,
                section: true,
                name1: { type: :string, default: "name1", desc: "desc1", required: false },
                name2: { type: :string, default: "name2", desc: "desc2", required: false },
                sub2: {
                  alias: nil,
                  multi: true,
                  required: false,
                  section: true,
                  name3: { type: :string, default: "name3", required: false },
                  name4: { type: :string, default: "name4", desc: "desc4", required: false },
                }
              }
            }
            assert_equal(expected, @proxy.dump_config_definition)
          end
        end
      end
    end
  end
end
