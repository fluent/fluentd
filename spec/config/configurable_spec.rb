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
end

describe Fluent::Configurable do
  context 'class defined without config_section' do
    describe '#initialize' do
      it 'create instance methods and default values by config_param and config_set_default' do
        obj1 = ConfigurableSpec::Base1.new
        expect(obj1.node).to eql("node")
        expect(obj1.flag1).to eq(false)
        expect(obj1.flag2).to eq(true)
        expect(obj1.name1).to be_nil
        expect(obj1.name2).to be_nil
        expect(obj1.name3).to eql("base1")
        expect(obj1.name4).to eql("base1")
      end

      it 'create instance methods and default values overwritten by sub class definition' do
        obj2 = ConfigurableSpec::Base2.new
        expect(obj2.node).to eql("node")
        expect(obj2.flag1).to eq(false)
        expect(obj2.flag2).to eq(true)
        expect(obj2.name1).to be_nil
        expect(obj2.name2).to eql("base2")
        expect(obj2.name3).to eql("base1")
        expect(obj2.name4).to eql("base2")
        expect(obj2.name5).to be_nil
        expect(obj2.name6).to eql("base2")
      end
    end

    describe '#configure' do
      it 'raise errors without any specifications for param without defaults' do
        b2 = ConfigurableSpec::Base2.new
        expect{ b2.configure({}) }.to raise_error(Fluent::ConfigError)
        expect{ b2.configure({"name1" => "t1"}) }.to raise_error(Fluent::ConfigError)
        expect{ b2.configure({"name5" => "t5"}) }.to raise_error(Fluent::ConfigError)
        expect{ b2.configure({"name1" => "t1", "name5" => "t5"}) }.not_to raise_error()

        expect(b2.get_all).to eql(["node", false, true, "t1", "base2", "base1", "base2", "t5", "base2"])
      end

      it 'can configure bool values' do
        b2a = ConfigurableSpec::Base2.new
        expect{ b2a.configure({"flag1" => "true", "flag2" => "yes", "name1" => "t1", "name5" => "t5"}) }.not_to raise_error()
        expect(b2a.flag1).to eq(true)
        expect(b2a.flag2).to eq(true)

        b2b = ConfigurableSpec::Base2.new
        expect{ b2b.configure({"flag1" => "false", "flag2" => "no", "name1" => "t1", "name5" => "t5"}) }.not_to raise_error()
        expect(b2b.flag1).to eq(false)
        expect(b2b.flag2).to eq(false)
      end

      it 'overwrites values of defaults' do
        b2 = ConfigurableSpec::Base2.new
        b2.configure({"name1" => "t1", "name2" => "t2", "name3" => "t3", "name4" => "t4", "name5" => "t5"})
        expect(b2.name1).to eql("t1")
        expect(b2.name2).to eql("t2")
        expect(b2.name3).to eql("t3")
        expect(b2.name4).to eql("t4")
        expect(b2.name5).to eql("t5")
        expect(b2.name6).to eql("base2")

        expect(b2.get_all).to eql(["node", false, true, "t1", "t2", "t3", "t4", "t5", "base2"])
      end
    end
  end

  context 'class defined with config_section' do
    describe '#initialize' do
      it 'create instance methods and default values as nil for params from config_section specified as non-multi' do
        b4 = ConfigurableSpec::Base4.new
        expect(b4.description1).to be_nil
        expect(b4.description2).to be_nil
      end

      it 'create instance methods and default values as [] for params from config_section specified as multi' do
        b4 = ConfigurableSpec::Base4.new
        expect(b4.description3).to eql([])
      end

      it 'overwrite base class definition by config_section of sub class definition' do
        b3 = ConfigurableSpec::Base3.new
        expect(b3.node).to eql([])
      end

      it 'create instance methods and default values by param_name' do
        b4 = ConfigurableSpec::Base4.new
        expect(b4.nodes).to eql([])
        expect(b4.node).to eql("node")
      end

      it 'create non-required and multi without any specifications' do
        b3 = ConfigurableSpec::Base3.new
        expect(b3.class.merged_configure_proxy.sections[:node].required?).to be_false
        expect(b3.class.merged_configure_proxy.sections[:node].multi?).to be_true
      end
    end

    describe '#configure' do
      def e(name, arg = '', attrs = {}, elements = [])
        attrs_str_keys = {}
        attrs.each{|key, value| attrs_str_keys[key.to_s] = value }
        Fluent::Config::Element.new(name, arg, attrs_str_keys, elements)
      end

      BASE_ATTRS = {
        "name1" => "1", "name2" => "2", "name3" => "3",
        "name4" => "4", "name5" => "5", "name6" => "6",
      }
      it 'checks required subsections' do
        b3 = ConfigurableSpec::Base3.new
        # branch sections required
        expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [])) }.to raise_error(Fluent::ConfigError)

        # branch argument required
        msg = "'<branch ARG>' section requires argument, in section branch"
        expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [e('branch', '')])) }.to raise_error(Fluent::ConfigError, msg)

        # leaf is not required
        expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [e('branch', 'branch_name')])) }.not_to raise_error()

        # leaf weight required
        msg = "'weight' parameter is required, in section branch > leaf"
        branch1 = e('branch', 'branch_name', {size: 1}, [e('leaf', '10', {weight: 1})])
        expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [branch1])) }.not_to raise_error()
        branch2 = e('branch', 'branch_name', {size: 1}, [e('leaf', '20')])
        expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [branch1, branch2])) }.to raise_error(Fluent::ConfigError, msg)
        branch3 = e('branch', 'branch_name', {size: 1}, [e('leaf', '10', {weight: 3}), e('leaf', '20')])
        expect{ b3.configure(e('ROOT', '', BASE_ATTRS, [branch3])) }.to raise_error(Fluent::ConfigError, msg)

        ### worm not required

        b4 = ConfigurableSpec::Base4.new

        d1 = e('description1', '', {text:"d1"})
        d2 = e('description2', '', {text:"d2"})
        d3 = e('description3', '', {text:"d3"})
        expect{ b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d3.dup])) }.not_to raise_error()

        # description1 cannot be specified 2 or more
        msg = "'<description1>' section cannot be written twice or more"
        expect{ b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d1.dup, d3.dup])) }.to raise_error(Fluent::ConfigError, msg)

        # description2 cannot be specified 2 or more
        msg = "'<description2>' section cannot be written twice or more"
        expect{ b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d3.dup, d2.dup])) }.to raise_error(Fluent::ConfigError, msg)

        # description3 can be specified 2 or more
        expect{ b4.configure(e('ROOT', '', BASE_ATTRS, [d1.dup, d2.dup, d3.dup, d3.dup])) }.not_to raise_error()
      end

      it 'constructs confuguration object tree for Base3' do
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

        expect(b3.node).not_to eql("node") # overwritten

        expect(b3.name1).to eql("1")
        expect(b3.name2).to eql("2")
        expect(b3.name3).to eql("3")
        expect(b3.name4).to eql("4")
        expect(b3.name5).to eql("5")
        expect(b3.name6).to eql("6")

        expect(b3.node).to be_a(Array)
        expect(b3.node.size).to eql(2)

        expect(b3.node[0].name).to eql("node")
        expect(b3.node[0].type).to eql("1")
        expect(b3.node[0][:type]).to eq(b3.node[0].type)
        expect(b3.node[1].name).to eql("node2")
        expect(b3.node[1].type).to eql("2")
        expect(b3.node[1][:type]).to eq(b3.node[1].type)

        expect(b3.branch).to be_a(Array)
        expect(b3.branch.size).to eql(3)

        expect(b3.branch[0].name).to eql('b1.*')
        expect(b3.branch[0].size).to eql(10)
        expect(b3.branch[0].leaf).to eql([])

        expect(b3.branch[1].name).to eql('b2.*')
        expect(b3.branch[1].size).to eql(5)
        expect(b3.branch[1].leaf.size).to eql(3)
        expect(b3.branch[1][:leaf]).to eq(b3.branch[1].leaf)

        expect(b3.branch[1].leaf[0].weight).to eql(55)
        expect(b3.branch[1].leaf[0].worms.size).to eql(0)

        expect(b3.branch[1].leaf[1].weight).to eql(50)
        expect(b3.branch[1].leaf[1].worms.size).to eql(1)
        expect(b3.branch[1].leaf[1].worms[0].type).to eql("ladybird")

        expect(b3.branch[1].leaf[2].weight).to eql(50)
        expect(b3.branch[1].leaf[2].worms.size).to eql(2)
        expect(b3.branch[1].leaf[2].worms[0].type).to eql("w1")
        expect(b3.branch[1].leaf[2].worms[1].type).to eql("w2")

        expect(b3.branch[2].name).to eql('b3.*')
        expect(b3.branch[2].size).to eql(503)
        expect(b3.branch[2].leaf.size).to eql(1)
        expect(b3.branch[2].leaf[0].weight).to eql(55)
      end

      it 'constructs confuguration object tree for Base4' do
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

        expect(b4.node).to eql("node")

        expect(b4.name1).to eql("1")
        expect(b4.name2).to eql("2")
        expect(b4.name3).to eql("3")
        expect(b4.name4).to eql("4")
        expect(b4.name5).to eql("5")
        expect(b4.name6).to eql("6")

        expect(b4.nodes).to be_a(Array)
        expect(b4.nodes.size).to eql(3)
        expect(b4.nodes[0].num).to eql(1)
        expect(b4.nodes[0].name).to eql("node")
        expect(b4.nodes[0].type).to eql("1")
        expect(b4.nodes[1].num).to eql(2)
        expect(b4.nodes[1].name).to eql("node2")
        expect(b4.nodes[1].type).to eql("b4")
        expect(b4.nodes[2].num).to eql(4)
        expect(b4.nodes[2].name).to eql("node")
        expect(b4.nodes[2].type).to eql("four")

            # e('description3', '', {text:"dddd3-1"}),
            # e('description3', 'd-3', {text:"dddd3-2"}),
            # e('description3', 'd-3a', {text:"dddd3-3"}),

        expect(b4.description1).to be_a(Fluent::Config::Section)
        expect(b4.description1.note).to eql("desc1")
        expect(b4.description1.text).to eql("dddd1")

        expect(b4.description2).to be_a(Fluent::Config::Section)
        expect(b4.description2.note).to eql("d-2")
        expect(b4.description2.text).to eql("dddd2")

        expect(b4.description3).to be_a(Array)
        expect(b4.description3.size).to eql(3)
        expect(b4.description3[0].note).to eql('desc3')
        expect(b4.description3[0].text).to eql('dddd3-1')
        expect(b4.description3[1].note).to eql('desc3: d-3')
        expect(b4.description3[1].text).to eql('dddd3-2')
        expect(b4.description3[2].note).to eql('desc3: d-3a')
        expect(b4.description3[2].text).to eql('dddd3-3')
      end
    end
  end
end
