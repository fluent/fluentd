require_relative 'helper'
require 'fluent/process'

class ProcessCompatibilityTest < ::Test::Unit::TestCase
  test 'DetachMultiProcessMixin is defined' do
    assert defined?(::Fluent::DetachMultiProcessMixin)
    assert_equal ::Fluent::DetachMultiProcessMixin, ::Fluent::Compat::DetachMultiProcessMixin
  end
end
