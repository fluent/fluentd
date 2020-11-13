require_relative 'helper'
require 'fluent/test'
require 'fluent/capability'

class FluentCapabilityTest < ::Test::Unit::TestCase
  setup do
    @capability = Fluent::Capability.new(:current_process)
    omit "Fluent::Capability class is not usable on this environment" unless @capability.usable?
  end

  sub_test_case "check capability" do
    test "effective" do
      @capability.clear(:both)
      assert_true @capability.update(:add, :effective, :dac_read_search)
      assert_equal CapNG::Result::PARTIAL, @capability.have_capabilities?(:caps)
      assert_nothing_raised do
        @capability.apply(:caps)
      end
      assert_equal CapNG::Result::NONE, @capability.have_capabilities?(:bounds)
      assert_true @capability.have_capability?(:effective, :dac_read_search)
      assert_false @capability.have_capability?(:inheritable, :dac_read_search)
      assert_false @capability.have_capability?(:permitted, :dac_read_search)
    end

    test "inheritable" do
      @capability.clear(:both)
      capabilities = [:chown, :dac_override]
      assert_equal [true, true], @capability.update(:add, :inheritable, capabilities)
      assert_equal CapNG::Result::NONE, @capability.have_capabilities?(:caps)
      assert_nothing_raised do
        @capability.apply(:caps)
      end
      assert_equal CapNG::Result::NONE, @capability.have_capabilities?(:bounds)
      capabilities.each do |capability|
        assert_false @capability.have_capability?(:effective, capability)
        assert_true @capability.have_capability?(:inheritable, capability)
        assert_false @capability.have_capability?(:permitted, capability)
      end
    end

    test "permitted" do
      @capability.clear(:both)
      capabilities = [:fowner, :fsetid, :kill]
      assert_equal [true, true, true], @capability.update(:add, :permitted, capabilities)
      assert_equal CapNG::Result::NONE, @capability.have_capabilities?(:caps)
      assert_nothing_raised do
        @capability.apply(:caps)
      end
      assert_equal CapNG::Result::NONE, @capability.have_capabilities?(:bounds)
      capabilities.each do |capability|
        assert_false @capability.have_capability?(:effective, capability)
        assert_false @capability.have_capability?(:inheritable, capability)
        assert_true @capability.have_capability?(:permitted, capability)
      end
    end

    test "effective/inheritable/permitted" do
      @capability.clear(:both)
      capabilities = [:setpcap, :net_admin, :net_raw, :sys_boot, :sys_time]
      update_type = CapNG::Type::EFFECTIVE | CapNG::Type::INHERITABLE | CapNG::Type::PERMITTED
      assert_equal [true, true, true, true, true], @capability.update(:add, update_type, capabilities)
      assert_equal CapNG::Result::PARTIAL, @capability.have_capabilities?(:caps)
      assert_nothing_raised do
        @capability.apply(:caps)
      end
      assert_equal CapNG::Result::NONE, @capability.have_capabilities?(:bounds)
      capabilities.each do |capability|
        assert_true @capability.have_capability?(:effective, capability)
        assert_true @capability.have_capability?(:inheritable, capability)
        assert_true @capability.have_capability?(:permitted, capability)
      end
    end
  end
end
