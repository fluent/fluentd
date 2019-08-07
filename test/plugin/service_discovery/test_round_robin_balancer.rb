require_relative '../../helper'
require 'fluent/plugin/service_discovery/round_robin_balancer'

class TestRoundRobinBalancer < ::Test::Unit::TestCase
  test 'select_service' do
    rrb = Fluent::Plugin::ServiceDiscovery::RoundRobinBalancer.new
    rrb.rebalance([1, 2, 3])

    rrb.select_service { |n| assert_equal 1, n }
    rrb.select_service { |n| assert_equal 2, n }
    rrb.select_service { |n| assert_equal 3, n }
    rrb.select_service { |n| assert_equal 1, n }
    rrb.select_service { |n| assert_equal 2, n }
    rrb.select_service { |n| assert_equal 3, n }
    rrb.rebalance([1, 2, 3, 4])
    rrb.select_service { |n| assert_equal 1, n }
    rrb.select_service { |n| assert_equal 2, n }
    rrb.select_service { |n| assert_equal 3, n }
    rrb.select_service { |n| assert_equal 4, n }
  end
end
