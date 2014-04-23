require 'fluent/config/configure_proxy'

describe Fluent::Config::ConfigureProxy do
  context 'to generate a instance' do
    describe '#initialize' do
      it 'has default values' do
        proxy = Fluent::Config::ConfigureProxy.new('section')
        expect(proxy.name).to eql(:section)

        proxy = Fluent::Config::ConfigureProxy.new(:section)
        expect(proxy.name).to eql(:section)
        expect(proxy.param_name).to eql(:section)
        expect(proxy.required).to be_nil
        expect(proxy.required?).to be_false
        expect(proxy.multi).to be_nil
        expect(proxy.multi?).to be_true
      end

      it 'can specify param_name/required/multi with optional arguments' do
        proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: 'sections', required: false, multi: true)
        expect(proxy.name).to eql(:section)
        expect(proxy.param_name).to eql(:sections)
        expect(proxy.required).to be_false
        expect(proxy.required?).to be_false
        expect(proxy.multi).to be_true
        expect(proxy.multi?).to be_true

        proxy = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, required: true, multi: false)
        expect(proxy.name).to eql(:section)
        expect(proxy.param_name).to eql(:sections)
        expect(proxy.required).to be_true
        expect(proxy.required?).to be_true
        expect(proxy.multi).to be_false
        expect(proxy.multi?).to be_false
      end
    end

    describe '#merge' do
      it 'generate a new instance which values are overwritten by the argument object' do
        proxy = p1 = Fluent::Config::ConfigureProxy.new(:section)
        expect(proxy.name).to eql(:section)
        expect(proxy.param_name).to eql(:section)
        expect(proxy.required).to be_nil
        expect(proxy.required?).to be_false
        expect(proxy.multi).to be_nil
        expect(proxy.multi?).to be_true

        p2 = Fluent::Config::ConfigureProxy.new(:section, param_name: :sections, required: true, multi: false)
        proxy = p1.merge(p2)
        expect(proxy.name).to eql(:section)
        expect(proxy.param_name).to eql(:sections)
        expect(proxy.required).to be_true
        expect(proxy.required?).to be_true
        expect(proxy.multi).to be_false
        expect(proxy.multi?).to be_false
      end

      it 'does not overwrite with argument object without any specifications of required/multi' do
        p1 = Fluent::Config::ConfigureProxy.new(:section1)
        p2 = Fluent::Config::ConfigureProxy.new(:section2, param_name: :sections, required: true, multi: false)
        p3 = Fluent::Config::ConfigureProxy.new(:section3)
        proxy = p1.merge(p2).merge(p3)
        expect(proxy.name).to eql(:section3)
        expect(proxy.param_name).to eql(:section3)
        expect(proxy.required).to be_true
        expect(proxy.required?).to be_true
        expect(proxy.multi).to be_false
        expect(proxy.multi?).to be_false
      end
    end

    describe '#config_param / #config_set_default / #config_argument' do
      it 'does not permit config_set_default for param w/ :default option' do
        proxy = Fluent::Config::ConfigureProxy.new(:section)
        proxy.config_param(:name, :string, default: "name1")
        expect{ proxy.config_set_default(:name, "name2") }.to raise_error(ArgumentError)
      end

      it 'does not permit default value specification twice' do
        proxy = Fluent::Config::ConfigureProxy.new(:section)
        proxy.config_param(:name, :string)
        proxy.config_set_default(:name, "name1")
        expect{ proxy.config_set_default(:name, "name2") }.to raise_error(ArgumentError)
      end

      it 'does not permit default value specification twice, even on config_argument' do
        proxy = Fluent::Config::ConfigureProxy.new(:section)
        proxy.config_param(:name, :string)
        proxy.config_set_default(:name, "name1")

        proxy.config_argument(:name)
        expect{ proxy.config_argument(:name, default: "name2") }.to raise_error(ArgumentError)
      end
    end
  end
end
