require 'test/unit'

class FuzzyIncludeAssertion
  include Test::Unit::Assertions

  def self.assert(expected, actual, message = nil)
    new(expected, actual, message).assert
  end

  def initialize(expected, actual, message)
    @expected = expected
    @actual = actual
    @message = message
  end

  def assert
    if collection?
      assert_same_collection
    else
      assert_same_value
    end
  end

  private

  def assert_same_value
    m = "expected(#{@expected}) !== actual(#{@actual.inspect})"
    if @message
      m = "#{@message}: #{m}"
    end
    assert_true(@expected === @actual, m)
  end

  def assert_same_class
    if @expected.class != @actual.class
     if (@expected.class.ancestors | @actual.class.ancestors).empty?
       assert_equal(@expected.class, @actual.class, @message)
     end
    end
  end

  def assert_same_collection
    assert_same_class
    assert_same_values
  end

  def assert_same_values
    if @expected.is_a?(Array)
      @expected.each_with_index do |val, i|
        self.class.assert(val, @actual[i], @message)
      end
    else
      @expected.each do |key, val|
        self.class.assert(val, @actual[key], "#{key}: ")
      end
    end
  end

  def collection?
    @actual.is_a?(Array) || @actual.is_a?(Hash)
  end
end

class FuzzyAssertion < FuzzyIncludeAssertion
  private

  def assert_same_collection
    super
    assert_same_keys
  end

  def assert_same_keys
    if @expected.is_a?(Array)
      assert_equal(@expected.size, @actual.size, "expected.size(#{@expected}) != actual.size(#{@expected})")
    else
      assert_equal(@expected.keys.sort, @actual.keys.sort)
    end
  end
end

module FuzzyAssert
  def assert_fuzzy_include(left, right, message = nil)
    FuzzyIncludeAssertion.new(left, right, message).assert
  end

  def assert_fuzzy_equal(left, right, message = nil)
    FuzzyAssertion.new(left, right, message).assert
  end
end
