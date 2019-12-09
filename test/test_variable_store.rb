require_relative 'helper'
require 'fluent/variable_store'

class VariableStoreTest < Test::Unit::TestCase
  def setup
  end

  def teardown
    Fluent::VariableStore.try_to_reset do
      # nothing
    end
  end

  sub_test_case '#fetch_or_build' do
    test 'fetch same object when the same key is passed' do
      c1 = Fluent::VariableStore.fetch_or_build(:test)
      c2 = Fluent::VariableStore.fetch_or_build(:test)

      assert_equal c1, c2
      assert_equal c1.object_id, c2.object_id

      c3 = Fluent::VariableStore.fetch_or_build(:test2)
      assert_not_equal c1.object_id, c3.object_id
    end

    test 'can be passed a default value' do
      c1 = Fluent::VariableStore.fetch_or_build(:test, default_value: Set.new)
      c2 = Fluent::VariableStore.fetch_or_build(:test)

      assert_kind_of Set, c1
      assert_equal c1, c2
      assert_equal c1.object_id, c2.object_id
    end
  end

  sub_test_case '#try_to_reset' do
    test 'reset all values' do
      c1 = Fluent::VariableStore.fetch_or_build(:test)
      c1[:k1] = 1
      assert_equal 1, c1[:k1]

      Fluent::VariableStore.try_to_reset do
        # nothing
      end

      c1 = Fluent::VariableStore.fetch_or_build(:test)
      assert_nil c1[:k1]
    end

    test 'rollback resetting if error raised' do
      c1 = Fluent::VariableStore.fetch_or_build(:test)
      c1[:k1] = 1
      assert_equal 1, c1[:k1]

      assert_raise(RuntimeError.new('pass')) do
        Fluent::VariableStore.try_to_reset do
          raise 'pass'
        end
      end

      c1 = Fluent::VariableStore.fetch_or_build(:test)
      assert_equal 1, c1[:k1]
    end
  end
end
