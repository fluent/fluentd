require_relative 'helper'
require 'fluent/process'

class ProcessCompatibilityTest < ::Test::Unit::TestCase
  test 'DetachProcessMixin is defined' do
    assert defined?(::Fluent::DetachProcessMixin)
    assert_equal ::Fluent::DetachProcessMixin, ::Fluent::Compat::DetachProcessMixin
  end

  test 'DetachMultiProcessMixin is defined' do
    assert defined?(::Fluent::DetachMultiProcessMixin)
    assert_equal ::Fluent::DetachMultiProcessMixin, ::Fluent::Compat::DetachMultiProcessMixin
  end
end
