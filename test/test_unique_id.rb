require_relative 'helper'
require 'fluent/plugin/base'
require 'fluent/unique_id'

module UniqueIdTestEnv
  class Dummy < Fluent::Plugin::Base
    include Fluent::UniqueId::Mixin
  end
end

class UniqueIdTest < Test::Unit::TestCase
  sub_test_case 'module used directly' do
    test '.generate generates 128bit length unique id (16bytes)' do
      assert_equal 16, Fluent::UniqueId.generate.bytesize
      ary = []
      100_000.times do
        ary << Fluent::UniqueId.generate
      end
      assert_equal 100_000, ary.uniq.size
    end

    test '.hex dumps 16bytes id into 32 chars' do
      assert_equal 32, Fluent::UniqueId.hex(Fluent::UniqueId.generate).size
      assert(Fluent::UniqueId.hex(Fluent::UniqueId.generate) =~ /^[0-9a-z]{32}$/)
    end
  end

  sub_test_case 'mixin' do
    setup do
      @i = UniqueIdTestEnv::Dummy.new
    end

    test '#generate_unique_id generates 128bit length id (16bytes)' do
      assert_equal 16, @i.generate_unique_id.bytesize
      ary = []
      100_000.times do
        ary << @i.generate_unique_id
      end
      assert_equal 100_000, ary.uniq.size
    end

    test '#dump_unique_id_hex dumps 16bytes id into 32 chars' do
      assert_equal 32, @i.dump_unique_id_hex(@i.generate_unique_id).size
      assert(@i.dump_unique_id_hex(@i.generate_unique_id) =~ /^[0-9a-z]{32}$/)
    end
  end
end
