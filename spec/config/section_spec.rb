require 'fluent/config/section'

describe Fluent::Config::Section do
  context 'class' do
    describe '.name' do
      it 'returns its full module name as String' do
        expect(Fluent::Config::Section.name).to eql('Fluent::Config::Section')
      end
    end
  end

  context 'instance object' do
    describe '#initialize' do
      it 'creates blank object without argument' do
        s = Fluent::Config::Section.new
        expect(s.instance_eval{ @params }).to eql({})
      end

      it 'creates object which contains specified hash object itself' do
        hash = {
          name: 'tagomoris',
          age: 34,
          send: 'email',
          class: 'normal',
          keys: 5,
        }
        s1 = Fluent::Config::Section.new(hash)
        expect(s1.instance_eval{ @params }).to eq(hash)
        expect(s1[:name]).to eql("tagomoris")
        expect(s1[:age]).to eql(34)
        expect(s1[:send]).to eql("email")
        expect(s1[:class]).to eql("normal")
        expect(s1[:keys]).to eql(5)

        expect(s1.name).to eql("tagomoris")
        expect(s1.age).to eql(34)
        expect(s1.send).to eql("email")
        expect(s1.class).to eql("normal")
        expect(s1.keys).to eql(5)

        expect{ s1.dup }.to raise_error(NoMethodError)
      end
    end

    describe '#to_h' do
      it 'returns internal hash itself' do
        hash = {
          name: 'tagomoris',
          age: 34,
          send: 'email',
          class: 'normal',
          keys: 5,
        }
        s = Fluent::Config::Section.new(hash)
        expect(s.to_h).to eq(hash)
        expect(s.to_h.class).to eq(Hash)
      end
    end

    describe '#instance_of?' do
      it 'can judge whether it is a Section object or not' do
        s = Fluent::Config::Section.new
        expect(s.instance_of?(Fluent::Config::Section)).to be true
        expect(s.instance_of?(BasicObject)).to be false
      end
    end

    describe '#is_a?' do
      it 'can judge whether it belongs to or not' do
        s = Fluent::Config::Section.new
        expect(s.is_a?(Fluent::Config::Section)).to be true
        expect(s.kind_of?(Fluent::Config::Section)).to be true
        expect(s.is_a?(BasicObject)).to be true
      end
    end

    describe '#+' do
      it 'can merge 2 sections: argument side is primary, internal hash is newly created' do
        h1 = {name: "s1", num: 10, class: "A"}
        s1 = Fluent::Config::Section.new(h1)

        h2 = {name: "s2", class: "A", num2: "5", num3: "8"}
        s2 = Fluent::Config::Section.new(h2)
        s = s1 + s2

        expect(s.to_h.object_id).not_to eq(h1.object_id)
        expect(s.to_h.object_id).not_to eq(h2.object_id)

        expect(s.name).to eql("s2")
        expect(s.num).to eql(10)
        expect(s.class).to eql("A")
        expect(s.num2).to eql("5")
        expect(s.num3).to eql("8")
      end
    end
  end
end
