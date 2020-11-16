require_relative '../helper'

require 'tempfile'
require 'fluent/command/cap_ctl'

class TestFluentCapCtl < Test::Unit::TestCase
  setup do
    omit "This environment does not handle Linux capability" unless defined?(CapNG)
  end

  sub_test_case "success" do
    test "clear capability" do
      logs = capture_stdout do
        Fluent::CapCtl.new(["--clear"]).call
      end
      expression = /\AClear capabilities .*\n/m
      assert_match expression, logs
    end

    test "add capability" do
      logs = capture_stdout do
        Fluent::CapCtl.new(["--add-cap", "dac_override"]).call
      end
      expression = /\AUpdating .* done.\nAdding .*\n/m
      assert_match expression, logs
    end

    test "drop capability" do
      logs = capture_stdout do
        Fluent::CapCtl.new(["--drop-cap", "chown"]).call
      end
      expression = /\AUpdating .* done.\nDropping .*\n/m
      assert_match expression, logs
    end

    test "get capability" do
      logs = capture_stdout do
        Fluent::CapCtl.new(["--get-cap"]).call
      end
      expression = /\ACapabilities in .*,\nEffective:   .*\nInheritable: .*\nPermitted:   .*/m
      assert_match expression, logs
    end
  end

  sub_test_case "success with file" do
    test "clear capability" do
      logs = capture_stdout do
        Tempfile.create("fluent-cap-") do |tempfile|
          Fluent::CapCtl.new(["--clear-cap", "-f", tempfile.path]).call
        end
      end
      expression = /\AClear capabilities .*\n/m
      assert_match expression, logs
    end

    test "add capability" do
      logs = capture_stdout do
        Tempfile.create("fluent-cap-") do |tempfile|
          Fluent::CapCtl.new(["--add-cap", "dac_override", "-f", tempfile.path]).call
        end
      end
      expression = /\AUpdating .* done.\nAdding .*\n/m
      assert_match expression, logs
    end

    test "drop capability" do
      logs = capture_stdout do
        Tempfile.create("fluent-cap-") do |tempfile|
          Fluent::CapCtl.new(["--drop-cap", "chown", "-f", tempfile.path]).call
        end
      end
      expression = /\AUpdating .* done.\nDropping .*\n/m
      assert_match expression, logs
    end

    test "get capability" do
      logs = capture_stdout do
        Tempfile.create("fluent-cap-") do |tempfile|
          Fluent::CapCtl.new(["--get-cap", "-f", tempfile.path]).call
        end
      end
      expression = /\ACapabilities in .*,\nEffective:   .*\nInheritable: .*\nPermitted:   .*/m
      assert_match expression, logs
    end
  end

  sub_test_case "invalid" do
    test "add capability" do
      assert_raise(ArgumentError) do
        Fluent::CapCtl.new(["--add-cap", "nonexitent"]).call
      end
    end

    test "drop capability" do
      assert_raise(ArgumentError) do
        Fluent::CapCtl.new(["--drop-cap", "invalid"]).call
      end
    end
  end
end
