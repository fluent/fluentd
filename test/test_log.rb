require_relative 'helper'
require 'fluent/engine'
require 'fluent/log'

class LogTest < Test::Unit::TestCase
  def setup
    @log_device = Fluent::Test::DummyLogDevice.new
    @timestamp = Time.parse("2016-04-21 11:58:41 +0900")
    @timestamp_str = @timestamp.strftime("%Y-%m-%d %H:%M:%S %z")
    stub(Time).now { @timestamp }
  end

  sub_test_case "log level" do
    data(
      trace: [Fluent::Log::LEVEL_TRACE, 0],
      debug: [Fluent::Log::LEVEL_DEBUG, 1],
      info: [Fluent::Log::LEVEL_INFO, 2],
      warn: [Fluent::Log::LEVEL_WARN, 3],
      error: [Fluent::Log::LEVEL_ERROR, 4],
      fatal: [Fluent::Log::LEVEL_FATAL, 5],
    )
    def test_output(data)
      log_level, start = data
      log = Fluent::Log.new(@log_device, log_level)
      log.trace "trace log"
      log.debug "debug log"
      log.info "info log"
      log.warn "warn log"
      log.error "error log"
      log.fatal "fatal log"
      expected = [
        "#{@timestamp_str} [trace]: trace log\n",
        "#{@timestamp_str} [debug]: debug log\n",
        "#{@timestamp_str} [info]: info log\n",
        "#{@timestamp_str} [warn]: warn log\n",
        "#{@timestamp_str} [error]: error log\n",
        "#{@timestamp_str} [fatal]: fatal log\n"
      ][start..-1]
      assert_equal(expected, log.out.logs)
    end

    data(
      trace: [Fluent::Log::LEVEL_TRACE, 0],
      debug: [Fluent::Log::LEVEL_DEBUG, 1],
      info: [Fluent::Log::LEVEL_INFO, 2],
      warn: [Fluent::Log::LEVEL_WARN, 3],
      error: [Fluent::Log::LEVEL_ERROR, 4],
      fatal: [Fluent::Log::LEVEL_FATAL, 5],
    )
    def test_output_with_block(data)
      log_level, start = data
      log = Fluent::Log.new(@log_device, log_level)
      log.trace { "trace log" }
      log.debug { "debug log" }
      log.info { "info log" }
      log.warn { "warn log" }
      log.error { "error log" }
      log.fatal { "fatal log" }
      expected = [
        "#{@timestamp_str} [trace]: trace log\n",
        "#{@timestamp_str} [debug]: debug log\n",
        "#{@timestamp_str} [info]: info log\n",
        "#{@timestamp_str} [warn]: warn log\n",
        "#{@timestamp_str} [error]: error log\n",
        "#{@timestamp_str} [fatal]: fatal log\n"
      ][start..-1]
      assert_equal(expected, log.out.logs)
    end

    data(
      trace: [Fluent::Log::LEVEL_TRACE, { trace: true, debug: true, info: true, warn: true, error: true, fatal: true }],
      debug: [Fluent::Log::LEVEL_DEBUG, { trace: false, debug: true, info: true, warn: true, error: true, fatal: true }],
      info: [Fluent::Log::LEVEL_INFO, { trace: false, debug: false, info: true, warn: true, error: true, fatal: true }],
      warn: [Fluent::Log::LEVEL_WARN, { trace: false, debug: false, info: false, warn: true, error: true, fatal: true }],
      error: [Fluent::Log::LEVEL_ERROR, { trace: false, debug: false, info: false, warn: false, error: true, fatal: true }],
      fatal: [Fluent::Log::LEVEL_FATAL, { trace: false, debug: false, info: false, warn: false, error: false, fatal: true }],
    )
    def test_execute_block(data)
      log_level, expected = data
      log = Fluent::Log.new(@log_device, log_level)
      block_called = {
        trace: false,
        debug: false,
        info: false,
        warn: false,
        error: false,
        fatal: false,
      }
      log.trace { block_called[:trace] = true }
      log.debug { block_called[:debug] = true }
      log.info { block_called[:info] = true }
      log.warn { block_called[:warn] = true }
      log.error { block_called[:error] = true }
      log.fatal { block_called[:fatal] = true }
      assert_equal(expected, block_called)
    end

    data(
      trace: [Fluent::Log::LEVEL_TRACE, 0],
      debug: [Fluent::Log::LEVEL_DEBUG, 3],
      info: [Fluent::Log::LEVEL_INFO, 6],
      warn: [Fluent::Log::LEVEL_WARN, 9],
      error: [Fluent::Log::LEVEL_ERROR, 12],
      fatal: [Fluent::Log::LEVEL_FATAL, 15],
    )
    def test_backtrace(data)
      log_level, start = data
      backtrace = ["line 1", "line 2", "line 3"]
      log = Fluent::Log.new(@log_device, log_level)
      log.trace_backtrace(backtrace)
      log.debug_backtrace(backtrace)
      log.info_backtrace(backtrace)
      log.warn_backtrace(backtrace)
      log.error_backtrace(backtrace)
      log.fatal_backtrace(backtrace)
      expected = [
        "  #{@timestamp_str} [trace]: line 1\n",
        "  #{@timestamp_str} [trace]: line 2\n",
        "  #{@timestamp_str} [trace]: line 3\n",
        "  #{@timestamp_str} [debug]: line 1\n",
        "  #{@timestamp_str} [debug]: line 2\n",
        "  #{@timestamp_str} [debug]: line 3\n",
        "  #{@timestamp_str} [info]: line 1\n",
        "  #{@timestamp_str} [info]: line 2\n",
        "  #{@timestamp_str} [info]: line 3\n",
        "  #{@timestamp_str} [warn]: line 1\n",
        "  #{@timestamp_str} [warn]: line 2\n",
        "  #{@timestamp_str} [warn]: line 3\n",
        "  #{@timestamp_str} [error]: line 1\n",
        "  #{@timestamp_str} [error]: line 2\n",
        "  #{@timestamp_str} [error]: line 3\n",
        "  #{@timestamp_str} [fatal]: line 1\n",
        "  #{@timestamp_str} [fatal]: line 2\n",
        "  #{@timestamp_str} [fatal]: line 3\n"
      ][start..-1]
      assert_equal(expected, log.out.logs)
    end
  end

  def test_dup
    log1 = Fluent::Log.new(@log_device, Fluent::Log::LEVEL_TRACE)
    log2 = log1.dup
    log1.level = Fluent::Log::LEVEL_DEBUG
    original_tag = log1.tag
    log1.tag = "changed"
    assert_equal(Fluent::Log::LEVEL_DEBUG, log1.level)
    assert_equal(Fluent::Log::LEVEL_TRACE, log2.level)
    assert_equal("changed", log1.tag)
    assert_equal(original_tag, log2.tag)
  end

  def test_disable_events
    log = Fluent::Log.new(@log_device, Fluent::Log::LEVEL_TRACE)
    engine = log.instance_variable_get("@engine")
    mock(engine).push_log_event(anything, anything, anything).once
    log.trace "trace log"
    log.disable_events(Thread.current)
    log.trace "trace log"
  end
end
