require_relative '../../helper'
require 'flexmock/test_unit'

require 'fluent/plugin/out_forward/load_balancer'

class LoadBalancerTest < Test::Unit::TestCase
  sub_test_case 'select_healthy_node' do
    test 'select healty node' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      n1 = flexmock('node', :'standby?' => false, :'available?' => false, weight: 1)
      n2 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)

      lb.rebuild_weight_array([n1, n2])
      lb.select_healthy_node do |node|
        assert_equal(node, n2)
      end

      lb.select_healthy_node do |node|
        assert_equal(node, n2)
      end
    end

    test 'call like round robin' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      n1 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)
      n2 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)

      lb.rebuild_weight_array([n1, n2])

      lb.select_healthy_node do |node|
        # to handle random choice
        if node == n1
          lb.select_healthy_node do |node|
            assert_equal(node, n2)
          end

          lb.select_healthy_node do |node|
            assert_equal(node, n1)
          end
        else
          lb.select_healthy_node do |node|
            assert_equal(node, n1)
          end

          lb.select_healthy_node do |node|
            assert_equal(node, n2)
          end
        end
      end
    end

    test 'raise an error if all node are unavialble' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      lb.rebuild_weight_array([flexmock('node', :'standby?' => false, :'available?' => false, weight: 1)])
      assert_raise(Fluent::Plugin::ForwardOutput::NoNodesAvailable) do
        lb.select_healthy_node
      end
    end
  end
end
