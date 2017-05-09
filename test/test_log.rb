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

  def teardown
    Thread.current[:last_repeated_stacktrace] = nil
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

  sub_test_case "suppress repeated backtrace" do
    def test_same_log_level
      backtrace = ["line 1", "line 2", "line 3"]
      log = Fluent::Log.new(@log_device, Fluent::Log::LEVEL_TRACE, suppress_repeated_stacktrace: true)
      log.trace_backtrace(backtrace)
      log.trace_backtrace(backtrace)
      log.trace_backtrace(backtrace + ["line 4"])
      log.trace_backtrace(backtrace)
      log.trace_backtrace(backtrace)
      expected = [
        "  #{@timestamp_str} [trace]: line 1\n",
        "  #{@timestamp_str} [trace]: line 2\n",
        "  #{@timestamp_str} [trace]: line 3\n",
        "  #{@timestamp_str} [trace]: suppressed same stacktrace\n",
        "  #{@timestamp_str} [trace]: line 1\n",
        "  #{@timestamp_str} [trace]: line 2\n",
        "  #{@timestamp_str} [trace]: line 3\n",
        "  #{@timestamp_str} [trace]: line 4\n",
        "  #{@timestamp_str} [trace]: line 1\n",
        "  #{@timestamp_str} [trace]: line 2\n",
        "  #{@timestamp_str} [trace]: line 3\n",
        "  #{@timestamp_str} [trace]: suppressed same stacktrace\n",
      ]
      assert_equal(expected, log.out.logs)
    end

    def test_different_log_level
      backtrace = ["line 1", "line 2", "line 3"]
      log = Fluent::Log.new(@log_device, Fluent::Log::LEVEL_TRACE, suppress_repeated_stacktrace: true)
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
        "  #{@timestamp_str} [debug]: suppressed same stacktrace\n",
        "  #{@timestamp_str} [info]: suppressed same stacktrace\n",
        "  #{@timestamp_str} [warn]: suppressed same stacktrace\n",
        "  #{@timestamp_str} [error]: suppressed same stacktrace\n",
        "  #{@timestamp_str} [fatal]: suppressed same stacktrace\n",
      ]
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

class PluginLoggerTest < Test::Unit::TestCase
  def setup
    @log_device = Fluent::Test::DummyLogDevice.new
    @timestamp = Time.parse("2016-04-21 11:58:41 +0900")
    @timestamp_str = @timestamp.strftime("%Y-%m-%d %H:%M:%S %z")
    stub(Time).now { @timestamp }
    @logger = Fluent::Log.new(@log_device, Fluent::Log::LEVEL_TRACE)
  end

  def teardown
    Thread.current[:last_repeated_stacktrace] = nil
  end

  def test_initialize
    log = Fluent::PluginLogger.new(@logger)
    logger = log.instance_variable_get("@logger")
    assert_equal(logger, @logger)
  end

  def test_level
    log = Fluent::PluginLogger.new(@logger)
    assert_equal(log.level, @logger.level)
    log.level = "fatal"
    assert_equal(Fluent::Log::LEVEL_FATAL, log.level)
    assert_equal(Fluent::Log::LEVEL_TRACE, @logger.level)
  end

  def test_enable_color
    log = Fluent::PluginLogger.new(@logger)
    log.enable_color(true)
    assert_equal(true, log.enable_color?)
    assert_equal(true, @logger.enable_color?)
    log.enable_color(false)
    assert_equal(false, log.enable_color?)
    assert_equal(false, @logger.enable_color?)
    log.enable_color
    assert_equal(true, log.enable_color?)
    assert_equal(true, @logger.enable_color?)
  end

  sub_test_case "delegators" do
    def setup
      super
      @log = Fluent::PluginLogger.new(@logger)
    end

    def test_enable_debug
      mock(@logger).enable_debug
      @log.enable_debug
    end

    def test_enable_event
      mock(@logger).enable_event
      @log.enable_event
    end

    def test_disable_events
      mock(@logger).disable_events(Thread.current)
      @log.disable_events(Thread.current)
    end

    def test_tag
      assert_equal(@log.tag, @logger.tag)
      @log.tag = "dummy"
      assert_equal(@log.tag, @logger.tag)
    end

    def test_time_format
      assert_equal(@log.time_format, @logger.time_format)
      @log.time_format = "time_format"
      assert_equal(@log.time_format, @logger.time_format)
    end

    def test_event
      mock(@logger).event(Fluent::Log::LEVEL_TRACE, { key: "value" })
      @log.event(Fluent::Log::LEVEL_TRACE, { key: "value" })
    end

    def test_caller_line
      mock(@logger).caller_line(Time.now, 1, Fluent::Log::LEVEL_TRACE)
      @log.caller_line(Time.now, 1, Fluent::Log::LEVEL_TRACE)
    end

    def test_puts
      mock(@logger).puts("log")
      @log.puts("log")
    end

    def test_write
      mock(@logger).write("log")
      @log.write("log")
    end

    def test_write_alias
      assert(@log.respond_to?(:<<))
      mock(@log.out).write("log")
      @log << "log"
    end

    def test_out
      assert_equal(@log.out, @logger.out)
      @log.out = Object.new
      assert_equal(@log.out, @logger.out)
    end
  end
end

class PluginLoggerMixinTest < Test::Unit::TestCase
  class DummyPlugin < Fluent::Input
  end

  def create_driver(conf)
    Fluent::Test::TestDriver.new(DummyPlugin).configure(conf)
  end

  def setup
    Fluent::Test.setup
  end

  def test_default_log
    plugin = DummyPlugin.new
    log = plugin.log
    assert_equal($log, log)
  end

  def test_log_level
    d = create_driver(%[log_level fatal])
    log = d.instance.log
    assert_not_equal($log.level, log.level)
    assert_equal(Fluent::Log::LEVEL_FATAL, log.level)
  end
end
