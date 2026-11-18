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

  sub_test_case 'nodes_provider' do
    test 'reads the node list from the provider instead of the given list' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      old_node = flexmock('old', :'standby?' => false, :'available?' => true, weight: 1)
      new_node = flexmock('new', :'standby?' => false, :'available?' => true, weight: 1)
      lb.nodes_provider = -> { [new_node] }

      lb.rebuild_weight_array([old_node])

      3.times { assert_equal new_node, lb.select_healthy_node { |n| n }.last }
    end

    test 'reads the node list while holding the rebuild lock' do
      lb = Fluent::Plugin::ForwardOutput::LoadBalancer.new($log)
      node = flexmock('node', :'standby?' => false, :'available?' => true, weight: 1)
      reading = Queue.new
      resume = Queue.new
      calls = 0
      lb.nodes_provider = -> {
        calls += 1
        if calls == 1
          reading << true
          resume.pop
        end
        [node]
      }

      first = Thread.new { lb.rebuild_weight_array }
      reading.pop
      second = Thread.new { lb.rebuild_weight_array }
      Timeout.timeout(10) { sleep 0.01 until second.status == 'sleep' || second.status == false }
      # the second rebuild waits for the first one before reading the list
      assert_equal 1, calls
      resume << true
      [first, second].each { |t| t.join(10) }
      assert_equal 2, calls
    end
  end
end
