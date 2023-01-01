require_relative '../helper'

require 'fluent/log'
require 'fluent/log/console_adapter'

class ConsoleAdapterTest < Test::Unit::TestCase
  def setup
    @timestamp = Time.parse("2023-01-01 15:32:41 +0000")
    @timestamp_str = @timestamp.strftime("%Y-%m-%d %H:%M:%S %z")
    Timecop.freeze(@timestamp)

    @logdev = Fluent::Test::DummyLogDevice.new
    @logger = ServerEngine::DaemonLogger.new(@logdev)
    @fluent_log = Fluent::Log.new(@logger)
    @console_logger = Fluent::Log::ConsoleAdapter.wrap(@fluent_log)
    @console_logger.level = :debug
  end

  def teardown
    Timecop.return
  end

  def test_expected_log_levels
    assert_equal({debug: 0, info: 1, warn: 2, error: 3, fatal: 4},
                 Console::Logger::LEVELS)
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_one_message(level)
    @console_logger.send(level, "message1")
    assert_equal(["#{@timestamp_str} [#{level}]: message1\n"],
                 @logdev.logs)
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_block(level)
    @console_logger.send(level) { "block message" }
    assert_equal(["#{@timestamp_str} [#{level}]: block message\n"],
                 @logdev.logs)
  end
end
