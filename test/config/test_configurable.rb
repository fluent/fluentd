require_relative '../helper'
require 'fluent/configurable'
require 'fluent/config/element'
require 'fluent/config/section'

module ConfigurableSpec
  class Base1
    include Fluent::Configurable

    config_param :node, :string, :default => "node"
    config_param :flag1, :bool, :default => false
    config_param :flag2, :bool, :default => true

    config_param :name1, :string
    config_param :name2, :string
    config_param :name3, :string, :default => "base1"
    config_param :name4, :string, :default => "base1"

    def get_all
      [@node, @flag1, @flag2, @name1, @name2, @name3, @name4]
    end
  end

  class Base2 < Base1
    config_set_default :name2, "base2"
    config_set_default :name4, "base2"
    config_param :name5, :string
    config_param :name6, :string, :default => "base2"

    def get_all
      ary = super
      ary + [@name5, @name6]
    end
  end

  class Base3 < Base2
    config_section :node do
      config_param :name, :string, :default => "node"
      config_param :type, :string
    end
    config_section :branch, required: true, multi: true do
      config_argument :name, :string
      config_param :size, :integer, default: 10
      config_section :leaf, required: false, multi: true do
        config_param :weight, :integer
        config_section :worm, param_name: 'worms', multi: true do
          config_param :type, :string, default: 'ladybird'
        end
      end
    end

    def get_all
      ary = super
      ary + [@branch]
    end
  end

  class Base4 < Base2
    config_section :node, param_name: :nodes do
      config_argument :num, :integer
      config_param :name, :string, :default => "node"
      config_param :type, :string, :default => "b4"
    end
    config_section :description1, required: false, multi: false do
      config_argument :note, :string, default: "desc1"
      config_param :text, :string
    end
    config_section :description2, required: true, multi: false do
      config_argument :note, :string, default: "desc2"
      config_param :text, :string
    end
    config_section :description3, required: true, multi: true do
      config_argument :note, default: "desc3" do |val|
        "desc3: #{val}"
      end
      config_param :text, :string
    end

    def get_all
      ary = super
      ary + [@nodes, @description1, @description2, @description3]
    end
  end

  class Example0
    include Fluent::Configurable

    config_param :stringvalue, :string
    config_param :boolvalue, :bool
    config_param :integervalue, :integer
    config_param :sizevalue, :size
    config_param :timevalue, :time
    config_param :floatvalue, :float
    config_param :hashvalue, :hash
    config_param :arrayvalue, :array
  end

  class Example1
    include Fluent::Configurable

    config_param :name, :string, alias: :fullname
    config_param :bool, :bool, alias: :flag
    config_section :detail, required: true, multi: false, alias: "information" do
      config_param :address, :string
    end

    def get_all
      [@name, @detail]
    end
  end
end

module Fluent::Config
  class TestConfigurable < ::Test::Unit::TestCase
    sub_test_case 'class defined without config_section' do
      sub_test_case '#initialize' do
        test 'create instance methods and default values by config_param and config_set_default' do
          obj1 = ConfigurableSpec::Base1.new
          assert_equal("node", obj1.node)
          assert_false(obj1.flag1)
          assert_true(obj1.flag2)
          assert_nil(obj1.name1)
          assert_nil(obj1.name2)
          assert_equal("base1", obj1.name3)
          assert_equal("base1", obj1.name4)
        end

        test 'create instance methods and default values overwritten by sub class definition' do
          obj2 = ConfigurableSpec::Base2.new
          assert_equal("node", obj2.node)
          assert_false(obj2.flag1)
          assert_true(obj2.flag2)
          assert_nil(obj2.name1)
          assert_equal("base2", obj2.name2)
          assert_equal("base1", obj2.name3)
          assert_equal("base2", obj2.name4)
          assert_nil(obj2.name5)
          assert_equal("base2", obj2.name6)
        end
      end

      sub_test_case '#configure' do
        test 'returns configurable object itself' do
          b2 = ConfigurableSpec::Base2.new
          assert_instance_of(ConfigurableSpec::Base2, b2.configure({"name1" => "t1", "name5" => "t5"}))
        end

        test 'raise errors without any specifications for param without defaults' do
          b2 = ConfigurableSpec::Base2.new
          assert_raise(Fluent::ConfigError) { b2.configure({}) }
          assert_raise(Fluent::ConfigError) { b2.configure({"name1" => "t1"}) }
          assert_raise(Fluent::ConfigError) { b2.configure({"name5" => "t5"}) }
          assert_nothing_raised { b2.configure({"name1" => "t1", "name5" => "t5"}) }

          assert_equal(["node", false, true, "t1", "base2", "base1", "base2", "t5", "base2"], b2.get_all)
        end

        test 'can configure bool values' do
          b2a = ConfigurableSpec::Base2.new
          assert_nothing_raised { b2a.configure({"flag1" => "true", "flag2" => "yes", "name1" => "t1", "name5" => "t5"}) }
          assert_true(b2a.flag1)
          assert_true(b2a.flag2)

          b2b = ConfigurableSpec::Base2.new
          assert_nothing_raised { b2b.configure({"flag1" => false, "flag2" => "no", "name1" => "t1", "name5" => "t5"}) }
          assert_false(b2b.flag1)
          assert_false(b2b.flag2)
        end

        test 'overwrites values of defaults' do
          b2 = ConfigurableSpec::Base2.new
          b2.configure({"name1" => "t1", "name2" => "t2", "name3" => "t3", "name4" => "t4", "name5" => "t5"})
          assert_equal("t1", b2.name1)
          assert_equal("t2", b2.name2)
          assert_equal("t3", b2.name3)
          assert_equal("t4", b2.name4)
          assert_equal("t5", b2.name5)
          assert_equal("base2", b2.name6)

          assert_equal(["node", false, true, "t1", "t2", "t3", "t4", "t5", "base2"], b2.get_all)
        end
      end
    end

    sub_test_case 'class defined with config_section' do
      sub_test_case '#initialize' do
        test 'create instance methods and default values as nil for params from config_section specified as non-multi' do
          b4 = ConfigurableSpec::Base4.new
          assert_nil(b4.description1)
          assert_nil(b4.description2)
        end

        test 'create instance methods and default values as [] for params from config_section specified as multi' do
          b4 = ConfigurableSpec::Base4.new
          assert_equal([], b4.description3)
        end

        test 'overwrite base class definition by config_section of sub class definition' do
          b3 = ConfigurableSpec::Base3.new
          assert_equal([], b3.node)
        end

        test 'create instance methods and default values by param_name' do
          b4 = ConfigurableSpec::Base4.new
          assert_equal([], b4.nodes)
          assert_equal("node", b4.node)
        end

        test 'create non-required and multi without any specifications' do
          b3 = ConfigurableSpec::Base3.new
          assert_false(b3.class.merged_configure_proxy.sections[:node].required?)
          assert_true(b3.class.merged_configure_proxy.sections[:node].multi?)
        end
      end

      sub_test_case '#configure' do
        def e(name, arg = '', attrs = {}, elements = [])
          attrs_str_keys = {}
          attrs.each{|key, value| attrs_str_keys[key.to_s] = value }
          Fluent::Config::Element.new(name, arg, attrs_str_keys, elements)
        end

        BASE_ATTRS = {
          "name1" => "1", "name2" => "2", "name3" => "3",
          "name4" => "4", "name5" => "5", "name6" => "6",
        }
        test 'checks required subsections' do
          b3 = ConfigurableSpec::Base3.new
          # branch sections required
          assert_raise(Fluent::ConfigError) { b3.configure(e('ROOT', '', BASE_ATTRS, [])) }

          # branch argument required
          msg = "'<branch ARG>' section requires argument, in section branch"
          #expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [e('branch', '')])) }.to raise_error(Fluent::ConfigError, msg)
          assert_raise(Fluent::ConfigError.new(msg)) { b3.configure(e('ROOT', '', BASE_ATTRS, [e('branch', '')])) }

          # leaf is not required
          assert_nothing_raised { b3.configure(e('ROOT', '', BASE_ATTRS, [e('branch', 'branch_name')])) }

          # leaf weight required
          msg = "'weight' parameter is required, in section branch > leaf"
          branch1 = e('branch', 'branch_name', {size: 1}, [e('leaf', '10', {weight: 1})])
          assert_nothing_raised { b3.configure(e('ROOT', '', BASE_ATTRS, [branch1])) }
          branch2 = e('branch', 'branch_name', {size: 1}, [e('leaf', '20')])
          assert_raise(Fluent::ConfigError.new(msg)) { b3.configure(e('ROOT', '', BASE_ATTRS, [branch1, branch2])) }
          branch3 = e('branch', 'branch_name', {size: 1}, [e('leaf', '10', {weight: 3}), e('leaf', '20')])
          assert_raise(Fluent::ConfigError.new(msg)) { b3.configure(e('ROOT', '', BASE_ATTRS, [branch3])) }

          ### worm not required

          b4 = ConfigurableSpec::Base4.new

          d1 = e('description1', '', {text:"d1"})
          d2 = e('description2', '', {text:"d2"})
          d3 = e('description3', '', {text:"d3"})
          assert_nothing_raised { b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d3.dup])) }

          # description1 cannot be specified 2 or more
          msg = "'<description1>' section cannot be written twice or more"
          assert_raise(Fluent::ConfigError.new(msg)) { b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d1.dup, d3.dup])) }

          # description2 cannot be specified 2 or more
          msg = "'<description2>' section cannot be written twice or more"
          assert_raise(Fluent::ConfigError.new(msg)) { b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d3.dup, d2.dup])) }

          # description3 can be specified 2 or more
          assert_nothing_raised { b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d3.dup, d3.dup])) }
        end

        test 'constructs confuguration object tree for Base3' do
          conf = e(
            'ROOT',
            '',
            BASE_ATTRS,
            [
              e('node', '', {type:"1"}), e('node', '', {name:"node2",type:"2"}),
              e('branch', 'b1.*', {}, []),
              e('branch',
                'b2.*',
                {size: 5},
                [
                  e('leaf', 'THIS IS IGNORED', {weight: 55}, []),
                  e('leaf', 'THIS IS IGNORED', {weight: 50}, [ e('worm', '', {}) ]),
                  e('leaf', 'THIS IS IGNORED', {weight: 50}, [ e('worm', '', {type:"w1"}), e('worm', '', {type:"w2"}) ]),
                ]
                ),
              e('branch',
                'b3.*',
                {size: "503"},
                [
                  e('leaf', 'THIS IS IGNORED', {weight: 55}, []),
                ]
                )
            ],
            )
          b3 = ConfigurableSpec::Base3.new.configure(conf)

          assert_not_equal("node", b3.node) # overwritten

          assert_equal("1", b3.name1)
          assert_equal("2", b3.name2)
          assert_equal("3", b3.name3)
          assert_equal("4", b3.name4)
          assert_equal("5", b3.name5)
          assert_equal("6", b3.name6)

          assert_instance_of(Array, b3.node)
          assert_equal(2, b3.node.size)

          assert_equal("node", b3.node[0].name)
          assert_equal("1", b3.node[0].type)
          assert_equal(b3.node[0].type, b3.node[0][:type])
          assert_equal("node2", b3.node[1].name)
          assert_equal("2", b3.node[1].type)
          assert_equal(b3.node[1].type, b3.node[1][:type])

          assert_instance_of(Array, b3.branch)
          assert_equal(3, b3.branch.size)

          assert_equal('b1.*', b3.branch[0].name)
          assert_equal(10, b3.branch[0].size)
          assert_equal([], b3.branch[0].leaf)

          assert_equal('b2.*', b3.branch[1].name)
          assert_equal(5, b3.branch[1].size)
          assert_equal(3, b3.branch[1].leaf.size)
          assert_equal(b3.branch[1].leaf, b3.branch[1][:leaf])

          assert_equal(55, b3.branch[1].leaf[0].weight)
          assert_equal(0, b3.branch[1].leaf[0].worms.size)

          assert_equal(50, b3.branch[1].leaf[1].weight)
          assert_equal(1, b3.branch[1].leaf[1].worms.size)
          assert_equal("ladybird", b3.branch[1].leaf[1].worms[0].type)

          assert_equal(50, b3.branch[1].leaf[2].weight)
          assert_equal(2, b3.branch[1].leaf[2].worms.size)
          assert_equal("w1", b3.branch[1].leaf[2].worms[0].type)
          assert_equal("w2", b3.branch[1].leaf[2].worms[1].type)

          assert_equal('b3.*', b3.branch[2].name)
          assert_equal(503, b3.branch[2].size)
          assert_equal(1, b3.branch[2].leaf.size)
          assert_equal(55, b3.branch[2].leaf[0].weight)
        end

        test 'constructs confuguration object tree for Base4' do
          conf = e(
            'ROOT',
            '',
            BASE_ATTRS,
            [
              e('node', '1', {type:"1"}), e('node', '2', {name:"node2"}),
              e('description3', '', {text:"dddd3-1"}),
              e('description2', 'd-2', {text:"dddd2"}),
              e('description1', '', {text:"dddd1"}),
              e('description3', 'd-3', {text:"dddd3-2"}),
              e('description3', 'd-3a', {text:"dddd3-3"}),
              e('node', '4', {type:"four"}),
            ],
            )
          b4 = ConfigurableSpec::Base4.new.configure(conf)

          assert_equal("node", b4.node)

          assert_equal("1", b4.name1)
          assert_equal("2", b4.name2)
          assert_equal("3", b4.name3)
          assert_equal("4", b4.name4)
          assert_equal("5", b4.name5)
          assert_equal("6", b4.name6)

          assert_instance_of(Array, b4.nodes)
          assert_equal(3, b4.nodes.size)
          assert_equal(1, b4.nodes[0].num)
          assert_equal("node", b4.nodes[0].name)
          assert_equal("1", b4.nodes[0].type)
          assert_equal(2, b4.nodes[1].num)
          assert_equal("node2", b4.nodes[1].name)
          assert_equal("b4", b4.nodes[1].type)
          assert_equal(4, b4.nodes[2].num)
          assert_equal("node", b4.nodes[2].name)
          assert_equal("four", b4.nodes[2].type)

          # e('description3', '', {text:"dddd3-1"}),
          # e('description3', 'd-3', {text:"dddd3-2"}),
          # e('description3', 'd-3a', {text:"dddd3-3"}),

          # NoMethodError: undefined method `class' for <Fluent::Config::Section {...}>:Fluent::Config::Section occurred. Should we add class method to Section?
          #assert_equal('Fluent::Config::Section', b4.description1.class.name)
          assert_equal("desc1", b4.description1.note)
          assert_equal("dddd1", b4.description1.text)

          # same with assert_equal('Fluent::Config::Section', b4.description1)
          #assert_equal('Fluent::Config::Section', b4.description2)
          assert_equal("d-2", b4.description2.note)
          assert_equal("dddd2", b4.description2.text)

          assert_instance_of(Array, b4.description3)
          assert_equal(3, b4.description3.size)
          assert_equal("desc3", b4.description3[0].note)
          assert_equal("dddd3-1", b4.description3[0].text)
          assert_equal('desc3: d-3', b4.description3[1].note)
          assert_equal('dddd3-2', b4.description3[1].text)
          assert_equal('desc3: d-3a', b4.description3[2].note)
          assert_equal('dddd3-3', b4.description3[2].text)
        end

        test 'checks missing of specifications' do
          conf0 = e('ROOT', '', {}, [])
          ex01 = ConfigurableSpec::Example0.new
          assert_raise(Fluent::ConfigError) { ex01.configure(conf0) }

          complete = {
            "stringvalue" => "s1", "boolvalue" => "yes", "integervalue" => "10",
            "sizevalue" => "10m", "timevalue" => "100s", "floatvalue" => "1.001",
            "hashvalue" => '{"foo":1, "bar":2}',
            "arrayvalue" => '[1,"ichi"]',
          }

          checker = lambda { |conf| ConfigurableSpec::Example0.new.configure(conf) }

          assert_nothing_raised { checker.call(complete) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "stringvalue" }) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "boolvalue"   }) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "integervalue"}) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "sizevalue"   }) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "timevalue"   }) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "floatvalue"  }) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "hashvalue"   }) }
          assert_raise(Fluent::ConfigError) { checker.call(complete.reject{|k,v| k == "arrayvalue"  }) }
        end

        test 'accepts configuration values as string representation' do
          conf = {
            "stringvalue" => "s1", "boolvalue" => "yes", "integervalue" => "10",
            "sizevalue" => "10m", "timevalue" => "10m", "floatvalue" => "1.001",
            "hashvalue" => '{"foo":1, "bar":2}',
            "arrayvalue" => '[1,"ichi"]',
          }
          ex = ConfigurableSpec::Example0.new.configure(conf)
          assert_equal("s1", ex.stringvalue)
          assert_true(ex.boolvalue)
          assert_equal(10, ex.integervalue)
          assert_equal(10 * 1024 * 1024, ex.sizevalue)
          assert_equal(10 * 60, ex.timevalue)
          assert_equal(1.001, ex.floatvalue)
          assert_equal({"foo" => 1, "bar" => 2}, ex.hashvalue)
          assert_equal([1, "ichi"], ex.arrayvalue)
        end

        test 'accepts configuration values as ruby value representation (especially for DSL)' do
          conf = {
            "stringvalue" => "s1", "boolvalue" => true, "integervalue" => 10,
            "sizevalue" => 10 * 1024 * 1024, "timevalue" => 10 * 60, "floatvalue" => 1.001,
            "hashvalue" => {"foo" => 1, "bar" => 2},
            "arrayvalue" => [1,"ichi"],
          }
          ex = ConfigurableSpec::Example0.new.configure(conf)
          assert_equal("s1", ex.stringvalue)
          assert_true(ex.boolvalue)
          assert_equal(10, ex.integervalue)
          assert_equal(10 * 1024 * 1024, ex.sizevalue)
          assert_equal(10 * 60, ex.timevalue)
          assert_equal(1.001, ex.floatvalue)
          assert_equal({"foo" => 1, "bar" => 2}, ex.hashvalue)
          assert_equal([1, "ichi"], ex.arrayvalue)
        end

        test 'gets both of true(yes) and false(no) for bool value parameter' do
          conf = {
            "stringvalue" => "s1", "integervalue" => 10,
            "sizevalue" => 10 * 1024 * 1024, "timevalue" => 10 * 60, "floatvalue" => 1.001,
            "hashvalue" => {"foo" => 1, "bar" => 2},
            "arrayvalue" => [1,"ichi"],
          }
          ex0 = ConfigurableSpec::Example0.new.configure(conf.merge({"boolvalue" => "true"}))
          assert_true(ex0.boolvalue)

          ex1 = ConfigurableSpec::Example0.new.configure(conf.merge({"boolvalue" => "yes"}))
          assert_true(ex1.boolvalue)

          ex2 = ConfigurableSpec::Example0.new.configure(conf.merge({"boolvalue" => true}))
          assert_true(ex2.boolvalue)

          ex3 = ConfigurableSpec::Example0.new.configure(conf.merge({"boolvalue" => "false"}))
          assert_false(ex3.boolvalue)

          ex4 = ConfigurableSpec::Example0.new.configure(conf.merge({"boolvalue" => "no"}))
          assert_false(ex4.boolvalue)

          ex5 = ConfigurableSpec::Example0.new.configure(conf.merge({"boolvalue" => false}))
          assert_false(ex5.boolvalue)
        end
      end
    end

    sub_test_case 'class defined with config_param/config_section having :alias' do
      sub_test_case '#initialize' do
        test 'does not create methods for alias' do
          ex1 = ConfigurableSpec::Example1.new
          assert_nothing_raised { ex1.name }
          assert_raise(NoMethodError) { ex1.fullname }
          assert_nothing_raised { ex1.bool }
          assert_raise(NoMethodError) { ex1.flag }
          assert_nothing_raised { ex1.detail }
          assert_raise(NoMethodError) { ex1.information}
        end
      end

      sub_test_case '#configure' do
        def e(name, arg = '', attrs = {}, elements = [])
          attrs_str_keys = {}
          attrs.each{|key, value| attrs_str_keys[key.to_s] = value }
          Fluent::Config::Element.new(name, arg, attrs_str_keys, elements)
        end

        test 'provides accessible data for alias attribute keys' do
          ex1 = ConfigurableSpec::Example1.new
          ex1.configure(e('ROOT', '', {"fullname" => "foo bar", "bool" => false}, [e('information', '', {"address" => "Mountain View 0"})]))
          assert_equal("foo bar", ex1.name)
          assert_not_nil(ex1.bool)
          assert_false(ex1.bool)
          assert_not_nil(ex1.detail)
          assert_equal("Mountain View 0", ex1.detail.address)
        end
      end
    end
  end
end
