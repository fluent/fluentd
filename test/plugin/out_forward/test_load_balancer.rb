require_relative '../../helper'
require 'flexmock/test_unit'

require 'fluent/plugin/out_forward/load_balancer'

class LoadBalancerTest < Test::Unit::TestCase
  sub_test_case 'select_healthy_node' do
    test 'select healthy node' do
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

    test 'call like round robin without weight=0 node' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      n1 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)
      n2 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)
      n3 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 0)

      lb.rebuild_weight_array([n1, n2, n3])

      lb.select_healthy_node do |node|
        # to handle random choice
        if node == n1
          lb.select_healthy_node do |node|
            assert_equal(node, n2)
          end

          lb.select_healthy_node do |node|
            assert_equal(node, n1)
          end

          lb.select_healthy_node do |node|
            assert_equal(node, n2)
          end
        else
          lb.select_healthy_node do |node|
            assert_equal(node, n1)
          end

          lb.select_healthy_node do |node|
            assert_equal(node, n2)
          end

          lb.select_healthy_node do |node|
            assert_equal(node, n1)
          end
        end
      end
    end

    test 'raise an error if all node are unavailable' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      lb.rebuild_weight_array([flexmock('node', :'standby?' => false, :'available?' => false, weight: 1)])
      assert_raise(Fluent::Plugin::ForwardOutput::NoNodesAvailable) do
        lb.select_healthy_node
      end
    end

    test 'it regards weight=0 node as unavailable' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      lb.rebuild_weight_array([flexmock('node', :'standby?' => false, :'available?' => true, weight: 0)])
      assert_raise(Fluent::Plugin::ForwardOutput::NoNodesAvailable) do
        lb.select_healthy_node
      end
    end
  end

  sub_test_case 'availability_changed?' do
    test 'compares the nodes with their availability at the last rebuild' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      available = true
      n1 = flexmock('node', :'standby?' => false, weight: 1)
      n1.should_receive(:available?).and_return { available }

      lb.rebuild_weight_array([n1])
      assert_false lb.availability_changed?([n1])

      available = false
      assert_true lb.availability_changed?([n1])

      lb.rebuild_weight_array([n1])
      assert_false lb.availability_changed?([n1])
    end

    test 'regards a node added after the last rebuild as a change' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      n1 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)
      n2 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)

      lb.rebuild_weight_array([n1])

      assert_true lb.availability_changed?([n1, n2])
    end
  end

  sub_test_case 'rebuild_weight_array generation' do
    test 'an older rebuild does not replace a newer weight array' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      n1 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1, host: 'n1', port: 1)
      n2 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1, host: 'n2', port: 2)
      n3 = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1, host: 'n3', port: 3)

      lb.rebuild_weight_array([n1, n2, n3], 1)
      # A later rebuild publishes only n3
      lb.rebuild_weight_array([n3], 2)
      # The older rebuild resumes and must not bring n1/n2 back
      lb.rebuild_weight_array([n1, n2, n3], 1)

      selected = 6.times.map { lb.select_healthy_node { |n| n }[1] }.uniq
      assert_equal [n3], selected
    end
  end

end
