require_relative '../helper'

require 'test-unit'
require 'win32/event' if Fluent.windows?

require 'fluent/command/ctl'

class TestFluentdCtl < ::Test::Unit::TestCase
  def assert_win32_event(event_name, command, pid_or_svcname)
    command, event_suffix = data
    event = Win32::Event.new(event_name)
    ipc = Win32::Ipc.new(event.handle)
    ret = Win32::Ipc::TIMEOUT

    wait_thread = Thread.new do
      ret = ipc.wait(1)
    end
    Fluent::Ctl.new([command, pid_or_svcname]).call
    wait_thread.join
    assert_equal(Win32::Ipc::SIGNALED, ret)
  end

  data("shutdown" => ["shutdown", "TERM", ""],
       "restart" => ["restart", "HUP", "HUP"],
       "flush" => ["flush", "USR1", "USR1"],
       "reload" => ["reload", "USR2", "USR2"])
  def test_commands(data)
    command, signal, event_suffix = data

    if Fluent.windows?
      event_name = "fluentd_54321"
      event_name << "_#{event_suffix}" unless event_suffix.empty?
      assert_win32_event(event_name, command, "54321")
    else
      got_signal = false
      Signal.trap(signal) do
        got_signal = true
      end
      Fluent::Ctl.new([command, "#{$$}"]).call
      assert_true(got_signal)
    end
  end

  data("shutdown" => ["shutdown", ""],
       "restart" => ["restart", "HUP"],
       "flush" => ["flush", "USR1"],
       "reload" => ["reload", "USR2"])
  def test_commands_with_winsvcname(data)
    omit "Only for Windows" unless Fluent.windows?

    command, event_suffix = data
    event_name = "testfluentdwinsvc"
    event_name << "_#{event_suffix}" unless event_suffix.empty?

    assert_win32_event(event_name, command, "testfluentdwinsvc")
  end
end
