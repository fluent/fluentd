require_relative '../helper'
require 'fluent/plugin/buffer'

class BufferMetadataTest < Test::Unit::TestCase

  def meta(timekey=nil, tag=nil, variables=nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end

  setup do
    Fluent::Test.setup
  end

  sub_test_case 'about metadata' do
    test 'comparison of variables should be stable' do
      m = meta(nil, nil, nil)
      # different sets of keys
      assert_equal(-1, m.cmp_variables({}, {a: 1}))
      assert_equal(1, m.cmp_variables({a: 1}, {}))
      assert_equal(1, m.cmp_variables({c: 1}, {a: 1}))
      assert_equal(-1, m.cmp_variables({a: 1}, {a: 1, b: 2}))
      assert_equal(1, m.cmp_variables({a: 1, c: 1}, {a: 1, b: 2}))
      assert_equal(1, m.cmp_variables({a: 1, b: 0, c: 1}, {a: 1, b: 2}))
      # same set of keys
      assert_equal(-1, m.cmp_variables({a: 1}, {a: 2}))
      assert_equal(-1, m.cmp_variables({a: 1, b: 0}, {a: 1, b: 1}))
      assert_equal(-1, m.cmp_variables({a: 1, b: 1, c: 100}, {a: 1, b: 1, c: 200}))
      assert_equal(-1, m.cmp_variables({b: 1, c: 100, a: 1}, {a: 1, b: 1, c: 200})) # comparison sorts keys
      assert_equal(-1, m.cmp_variables({a: nil}, {a: 1}))
      assert_equal(-1, m.cmp_variables({a: 1, b: nil}, {a: 1, b: 1}))
    end

    test 'comparison of metadata should be stable' do
      n = Time.now.to_i

      assert_equal(0, meta(nil, nil, nil) <=> meta(nil, nil, nil))
      assert_equal(0, meta(n, nil, nil) <=> meta(n, nil, nil))
      assert_equal(0, meta(nil, "t1", nil) <=> meta(nil, "t1", nil))
      assert_equal(0, meta(nil, nil, {}) <=> meta(nil, nil, {}))
      assert_equal(0, meta(nil, nil, {a: "1"}) <=> meta(nil, nil, {a: "1"}))
      assert_equal(0, meta(n, nil, {}) <=> meta(n, nil, {}))
      assert_equal(0, meta(n, "t1", {}) <=> meta(n, "t1", {}))
      assert_equal(0, meta(n, "t1", {a: "x", b: 10}) <=> meta(n, "t1", {a: "x", b: 10}))

      # timekey is 1st comparison key
      assert_equal(-1, meta(n - 300, nil, nil) <=> meta(n - 270, nil, nil))
      assert_equal(1, meta(n + 1, "a", nil) <=> meta(n - 1, "b", nil))
      assert_equal(-1, meta(n - 1, nil, {a: 100}) <=> meta(n + 1, nil, {}))

      # tag is 2nd
      assert_equal(-1, meta(nil, "a", {}) <=> meta(nil, "b", {}))
      assert_equal(-1, meta(n, "a", {}) <=> meta(n, "b", {}))
      assert_equal(1, meta(nil, "x", {a: 1}) <=> meta(nil, "t", {}))
      assert_equal(1, meta(n, "x", {a: 1}) <=> meta(n, "t", {}))
      assert_equal(1, meta(nil, "x", {a: 1}) <=> meta(nil, "t", {a: 1}))
      assert_equal(1, meta(n, "x", {a: 1}) <=> meta(n, "t", {a: 2}))
      assert_equal(1, meta(n, "x", {a: 1}) <=> meta(n, "t", {a: 10, b: 1}))

      # variables is the last
      assert_equal(-1, meta(nil, nil, {}) <=> meta(nil, nil, {a: 1}))
      assert_equal(-1, meta(n, "t", {}) <=> meta(n, "t", {a: 1}))
      assert_equal(1, meta(n, "t", {a: 1}) <=> meta(n, "t", {}))
      assert_equal(-1, meta(n, "t", {a: 1}) <=> meta(n, "t", {a: 2}))
      assert_equal(-1, meta(n, "t", {a: 1}) <=> meta(n, "t", {a: 1, b: 1}))
      assert_equal(1, meta(nil, nil, {b: 1}) <=> meta(nil, nil, {a: 1}))
      assert_equal(1, meta(n, "t", {b: 1}) <=> meta(n, "t", {a: 1}))
    end

    test 'metadata can be sorted' do
      n = Time.now.to_i
      m0 = meta(nil, nil, nil)
      m1 = meta(n - 1, nil, nil)
      m2 = meta(n - 1, "a", nil)
      m3 = meta(n - 1, "a", {a: 1})
      m4 = meta(n - 1, "a", {a: 100})
      m5 = meta(n - 1, "a", {a: 100, b: 1})
      m6 = meta(n - 1, "aa", nil)
      m7 = meta(n - 1, "aa", {a: 1})
      m8 = meta(n - 1, "b", nil)
      m9 = meta(n, nil, nil)
      m10 = meta(n + 1, nil, {a: 1})
      expected = [m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10].freeze
      ary = expected.dup
      100.times do
        assert_equal expected, ary.shuffle.sort
      end
    end
  end
end
