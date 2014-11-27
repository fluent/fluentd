require_relative '../helper'
require 'fluent/config/section'

module Fluent::Config
  class TestSection < ::Test::Unit::TestCase
    sub_test_case Fluent::Config::Section do
      sub_test_case 'class' do
        sub_test_case '.name' do
          test 'returns its full module name as String' do
            assert_equal('Fluent::Config::Section', Fluent::Config::Section.name)
          end
        end
      end

      sub_test_case 'instance object' do
        sub_test_case '#initialize' do
          test 'creates blank object without argument' do
            s = Fluent::Config::Section.new
            assert_equal({}, s.instance_eval{ @params })
          end

          test 'creates object which contains specified hash object itself' do
            hash = {
              name: 'tagomoris',
              age: 34,
              send: 'email',
              class: 'normal',
              keys: 5,
            }
            s1 = Fluent::Config::Section.new(hash)
            assert_equal(hash, s1.instance_eval { @params })
            assert_equal("tagomoris", s1[:name])
            assert_equal(34, s1[:age])
            assert_equal("email", s1[:send])
            assert_equal("normal", s1[:class])
            assert_equal(5, s1[:keys])

            assert_equal("tagomoris", s1.name)
            assert_equal(34, s1.age)
            assert_equal("email", s1.send)
            assert_equal("normal", s1.class)
            assert_equal(5, s1.keys)

            assert_raise(NoMethodError) { s1.dup }
          end
        end

        sub_test_case '#object_id' do
          test 'returns its object id' do
            s1 = Fluent::Config::Section.new({})
            assert s1.object_id
            s2 = Fluent::Config::Section.new({})
            assert s2.object_id
            assert_not_equal s1.object_id, s2.object_id
          end
        end

        sub_test_case '#to_h' do
          test 'returns internal hash itself' do
            hash = {
              name: 'tagomoris',
              age: 34,
              send: 'email',
              class: 'normal',
              keys: 5,
            }
            s = Fluent::Config::Section.new(hash)
            assert_equal(hash, s.to_h)
            assert_instance_of(Hash, s.to_h)
          end
        end

        sub_test_case '#instance_of?' do
          test 'can judge whether it is a Section object or not' do
            s = Fluent::Config::Section.new
            assert_true(s.instance_of?(Fluent::Config::Section))
            assert_false(s.instance_of?(BasicObject))
          end
        end

        sub_test_case '#is_a?' do
          test 'can judge whether it belongs to or not' do
            s = Fluent::Config::Section.new
            assert_true(s.is_a?(Fluent::Config::Section))
            assert_true(s.kind_of?(Fluent::Config::Section))
            assert_true(s.is_a?(BasicObject))
          end
        end

        sub_test_case '#+' do
          test 'can merge 2 sections: argument side is primary, internal hash is newly created' do
            h1 = {name: "s1", num: 10, class: "A"}
            s1 = Fluent::Config::Section.new(h1)

            h2 = {name: "s2", class: "A", num2: "5", num3: "8"}
            s2 = Fluent::Config::Section.new(h2)
            s = s1 + s2

            assert_not_equal(h1.object_id, s.to_h.object_id)
            assert_not_equal(h2.object_id, s.to_h.object_id)

            assert_equal("s2", s.name)
            assert_equal(10, s.num)
            assert_equal("A", s.class)
            assert_equal("5", s.num2)
            assert_equal("8", s.num3)
          end
        end
      end
    end
  end
end
