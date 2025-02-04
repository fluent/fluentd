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
  end

  def teardown
    Timecop.return
  end

  def test_expected_log_levels
    assert_equal({debug: 0, info: 1, warn: 2, error: 3, fatal: 4},
                 Console::Logger::LEVELS)
  end

  data(trace: [Fluent::Log::LEVEL_TRACE, :debug],
       debug: [Fluent::Log::LEVEL_DEBUG, :debug],
       info: [Fluent::Log::LEVEL_INFO, :info],
       warn: [Fluent::Log::LEVEL_WARN, :warn],
       error: [Fluent::Log::LEVEL_ERROR, :error],
       fatal: [Fluent::Log::LEVEL_FATAL, :fatal])
  def test_reflect_log_level(data)
    level, expected = data
    @fluent_log.level = level
    console_logger = Fluent::Log::ConsoleAdapter.wrap(@fluent_log)
    assert_equal(Console::Logger::LEVELS[expected],
                 console_logger.level)
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_string_subject(level)
    @console_logger.send(level, "subject")
    assert_equal(["#{@timestamp_str} [#{level}]:   0.0s: subject\n"],
                 @logdev.logs)
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_args(level)
    @console_logger.send(level, "subject", 1, 2, 3)
    assert_equal([
                   "#{@timestamp_str} [#{level}]:   0.0s: subject\n" +
                   "      | 1\n" +
                   "      | 2\n" +
                   "      | 3\n"
                 ],
                 @logdev.logs)
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_options(level)
    @console_logger.send(level, "subject", kwarg1: "opt1", kwarg2: "opt2")
    lines = @logdev.logs[0].split("\n")
    args = JSON.parse(lines[1..].collect { |str| str.sub(/\s+\|/, "") }.join("\n"));
    assert_equal([
                   1,
                   "#{@timestamp_str} [#{level}]:   0.0s: subject",
                   { "kwarg1" => "opt1", "kwarg2" => "opt2" }
                 ],
                 [
                   @logdev.logs.size,
                   lines[0],
                   args
                 ])
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_block(level)
    @console_logger.send(level, "subject") { "block message" }
    assert_equal([
                   "#{@timestamp_str} [#{level}]:   0.0s: subject\n" +
                   "      | block message\n"
                 ],
                 @logdev.logs)
  end

  data(debug: :debug,
       info: :info,
       warn: :warn,
       error: :error,
       fatal: :fatal)
  def test_multiple_entries(level)
    @console_logger.send(level, "subject1")
    @console_logger.send(level, "line2")
    assert_equal([
                   "#{@timestamp_str} [#{level}]:   0.0s: subject1\n",
                   "#{@timestamp_str} [#{level}]:   0.0s: line2\n"
                 ],
                 @logdev.logs)
  end
end
