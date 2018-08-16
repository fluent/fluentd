require_relative 'helper'
require 'fluent/test/driver/input'
require 'fluent/engine'
require 'fluent/log'
require 'timecop'
require 'logger'

class LogTest < Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/log/#{ENV['TEST_ENV_NUMBER']}")

  def setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
    @log_device = Fluent::Test::DummyLogDevice.new
    @timestamp = Time.parse("2016-04-21 11:58:41 +0900")
    @timestamp_str = @timestamp.strftime("%Y-%m-%d %H:%M:%S %z")
    Timecop.freeze(@timestamp)
  end

  def teardown
    @log_device.reset
    Timecop.return
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
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev)
      log = Fluent::Log.new(logger)
      log.level = log_level
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
        trace: [ServerEngine::DaemonLogger::TRACE, 0],
        debug: [ServerEngine::DaemonLogger::DEBUG, 1],
        info: [ServerEngine::DaemonLogger::INFO, 2],
        warn: [ServerEngine::DaemonLogger::WARN, 3],
        error: [ServerEngine::DaemonLogger::ERROR, 4],
        fatal: [ServerEngine::DaemonLogger::FATAL, 5],
    )
    def test_output_with_serverengine_loglevel(data)
      log_level, start = data

      dl_opts = {}
      dl_opts[:log_level] = log_level
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      log = Fluent::Log.new(logger)
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

      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev)
      log = Fluent::Log.new(logger)
      log.level = log_level
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
        trace: [ServerEngine::DaemonLogger::TRACE, 0],
        debug: [ServerEngine::DaemonLogger::DEBUG, 1],
        info: [ServerEngine::DaemonLogger::INFO, 2],
        warn: [ServerEngine::DaemonLogger::WARN, 3],
        error: [ServerEngine::DaemonLogger::ERROR, 4],
        fatal: [ServerEngine::DaemonLogger::FATAL, 5],
    )
    def test_output_with_block_with_serverengine_loglevel(data)
      log_level, start = data

      dl_opts = {}
      dl_opts[:log_level] = log_level
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      log = Fluent::Log.new(logger)
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
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev)
      log = Fluent::Log.new(logger)
      log.level = log_level
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
      trace: [ServerEngine::DaemonLogger::TRACE, { trace: true, debug: true, info: true, warn: true, error: true, fatal: true }],
      debug: [ServerEngine::DaemonLogger::DEBUG, { trace: false, debug: true, info: true, warn: true, error: true, fatal: true }],
      info: [ServerEngine::DaemonLogger::INFO, { trace: false, debug: false, info: true, warn: true, error: true, fatal: true }],
      warn: [ServerEngine::DaemonLogger::WARN, { trace: false, debug: false, info: false, warn: true, error: true, fatal: true }],
      error: [ServerEngine::DaemonLogger::ERROR, { trace: false, debug: false, info: false, warn: false, error: true, fatal: true }],
      fatal: [ServerEngine::DaemonLogger::FATAL, { trace: false, debug: false, info: false, warn: false, error: false, fatal: true }],
    )
    def test_execute_block_with_serverengine_loglevel(data)
      log_level, expected = data
      dl_opts = {}
      dl_opts[:log_level] = log_level
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      log = Fluent::Log.new(logger)
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
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev)
      log = Fluent::Log.new(logger)
      log.level = log_level
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

    data(
      trace: [ServerEngine::DaemonLogger::TRACE, 0],
      debug: [ServerEngine::DaemonLogger::DEBUG, 3],
      info: [ServerEngine::DaemonLogger::INFO, 6],
      warn: [ServerEngine::DaemonLogger::WARN, 9],
      error: [ServerEngine::DaemonLogger::ERROR, 12],
      fatal: [ServerEngine::DaemonLogger::FATAL, 15],
    )
    def test_backtrace_with_serverengine_loglevel(data)
      log_level, start = data
      backtrace = ["line 1", "line 2", "line 3"]
      dl_opts = {}
      dl_opts[:log_level] = log_level
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      log = Fluent::Log.new(logger)
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
      dl_opts = {}
      dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      opts = {}
      opts[:suppress_repeated_stacktrace] = true
      log = Fluent::Log.new(logger, opts)
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
      dl_opts = {}
      dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      opts = {}
      opts[:suppress_repeated_stacktrace] = true
      log = Fluent::Log.new(logger, opts)
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
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
    logdev = @log_device
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    log1 = Fluent::Log.new(logger)
    log2 = log1.dup
    log1.level = Fluent::Log::LEVEL_DEBUG
    assert_equal(Fluent::Log::LEVEL_DEBUG, log1.level)
    assert_equal(Fluent::Log::LEVEL_TRACE, log2.level)
  end

  def test_format_json
    logdev = @log_device
    logger = ServerEngine::DaemonLogger.new(logdev)
    log = Fluent::Log.new(logger)
    log.format = :json
    log.level = Fluent::Log::LEVEL_TRACE
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
    ]
    assert_equal(expected, log.out.logs.map { |l|
                   r = JSON.parse(l)
                   "#{r['time']} [#{r['level']}]: #{r['message']}\n"
                 })
  end

  def test_time_format
    logdev = @log_device
    logger = ServerEngine::DaemonLogger.new(logdev)
    log = Fluent::Log.new(logger)
    log.time_format = "%Y"
    log.level = Fluent::Log::LEVEL_TRACE
    log.trace "trace log"
    log.debug "debug log"
    log.info "info log"
    log.warn "warn log"
    log.error "error log"
    log.fatal "fatal log"
    timestamp_str = @timestamp.strftime("%Y")
    expected = [
      "#{timestamp_str} [trace]: trace log\n",
      "#{timestamp_str} [debug]: debug log\n",
      "#{timestamp_str} [info]: info log\n",
      "#{timestamp_str} [warn]: warn log\n",
      "#{timestamp_str} [error]: error log\n",
      "#{timestamp_str} [fatal]: fatal log\n"
    ]
    assert_equal(expected, log.out.logs)
  end

  def test_disable_events
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
    logdev = @log_device
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    log = Fluent::Log.new(logger)
    log.enable_event(true)
    engine = log.instance_variable_get("@engine")
    mock(engine).push_log_event(anything, anything, anything).once
    log.trace "trace log"
    log.disable_events(Thread.current)
    log.trace "trace log"
  end

  def test_level_reload
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
    logdev = @log_device
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    log = Fluent::Log.new(logger)
    assert_equal(ServerEngine::DaemonLogger::TRACE, logger.level)
    assert_equal(Fluent::Log::LEVEL_TRACE, log.level)
    # change daemon logger side level
    logger.level = ServerEngine::DaemonLogger::DEBUG
    assert_equal(ServerEngine::DaemonLogger::DEBUG, logger.level)
    # check fluentd log side level is also changed
    assert_equal(Fluent::Log::LEVEL_DEBUG, log.level)
  end

  DAY_SEC = 60 * 60 * 24
  data(
    rotate_daily_age: ['daily', 100000, DAY_SEC + 1],
    rotate_weekly_age: ['weekly', 100000, DAY_SEC * 7 + 1],
    rotate_monthly_age: ['monthly', 100000, DAY_SEC * 31 + 1],
    rotate_size: [1, 100, 0, '0'],
  )
  def test_log_with_logdevio(expected)
    with_timezone('utc') do
      @timestamp = Time.parse("2016-04-21 00:00:00 +0000")
      @timestamp_str = @timestamp.strftime("%Y-%m-%d %H:%M:%S %z")
      Timecop.freeze(@timestamp)

      rotate_age, rotate_size, travel_term = expected
      path = "#{TMP_DIR}/log-dev-io-#{rotate_size}-#{rotate_age}"

      logdev = Fluent::LogDeviceIO.new(path, shift_age: rotate_age, shift_size: rotate_size)
      logger = ServerEngine::DaemonLogger.new(logdev)
      log = Fluent::Log.new(logger)

      msg = 'a' * 101
      log.info msg
      assert_match msg, File.read(path)

      Timecop.freeze(@timestamp + travel_term)

      msg2 = 'b' * 101
      log.info msg2
      c = File.read(path)

      assert_match msg2, c
      assert_not_equal msg, c
    end
  end

  def test_log_rotates_specified_size_with_logdevio
    with_timezone('utc') do
      rotate_age = 2
      rotate_size = 100
      path = "#{TMP_DIR}/log-dev-io-#{rotate_size}-#{rotate_age}"
      path0 = path + '.0'
      path1 = path + '.1'

      logdev = Fluent::LogDeviceIO.new(path, shift_age: rotate_age, shift_size: rotate_size)
      logger = ServerEngine::DaemonLogger.new(logdev)
      log = Fluent::Log.new(logger)

      msg = 'a' * 101
      log.info msg
      assert_match msg, File.read(path)
      assert_true File.exist?(path)
      assert_true !File.exist?(path0)
      assert_true !File.exist?(path1)

      # create log.0
      msg2 = 'b' * 101
      log.info msg2
      c = File.read(path)
      c0 = File.read(path0)
      assert_match msg2, c
      assert_match msg, c0
      assert_true File.exist?(path)
      assert_true File.exist?(path0)
      assert_true !File.exist?(path1)

      # rotate
      msg3 = 'c' * 101
      log.info msg3
      c = File.read(path)
      c0 = File.read(path0)
      assert_match msg3, c
      assert_match msg2, c0
      assert_true File.exist?(path)
      assert_true File.exist?(path0)
      assert_true !File.exist?(path1)
    end
  end
end

class PluginLoggerTest < Test::Unit::TestCase
  def setup
    @log_device = Fluent::Test::DummyLogDevice.new
    @timestamp = Time.parse("2016-04-21 11:58:41 +0900")
    @timestamp_str = @timestamp.strftime("%Y-%m-%d %H:%M:%S %z")
    Timecop.freeze(@timestamp)
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
    logdev = @log_device
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    @logger = Fluent::Log.new(logger)
  end

  def teardown
    @log_device.reset
    Timecop.return
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

  def test_log_type_in_default
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_TRACE).once
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_DEBUG).once
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_INFO).once
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_WARN).once
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_ERROR).once
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_FATAL).once

    @logger.trace "trace log 1"
    @logger.debug "debug log 2"
    @logger.info  "info log 3"
    @logger.warn  "warn log 4"
    @logger.error "error log 5"
    @logger.fatal "fatal log 6"
  end

  def test_log_types
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_TRACE).once
    mock(@logger).caller_line(:supervisor, Time.now, 1, Fluent::Log::LEVEL_DEBUG).once
    mock(@logger).caller_line(:worker0, Time.now, 1, Fluent::Log::LEVEL_INFO).once
    mock(@logger).caller_line(:default, Time.now, 1, Fluent::Log::LEVEL_WARN).once
    mock(@logger).caller_line(:supervisor, Time.now, 1, Fluent::Log::LEVEL_ERROR).once
    mock(@logger).caller_line(:worker0, Time.now, 1, Fluent::Log::LEVEL_FATAL).once

    @logger.trace :default, "trace log 1"
    @logger.debug :supervisor, "debug log 2"
    @logger.info  :worker0, "info log 3"
    @logger.warn  :default, "warn log 4"
    @logger.error :supervisor, "error log 5"
    @logger.fatal :worker0, "fatal log 6"
  end

  sub_test_case "supervisor process type" do
    setup do
      dl_opts = {}
      dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      @logger = Fluent::Log.new(logger, process_type: :supervisor)
    end

    test 'default type logs are shown    w/o worker id' do
      @logger.info "yaaay"
      @logger.info :default, "booo"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: booo\n") }
    end

    test 'supervisor type logs are shown w/o worker id' do
      @logger.info :supervisor, "yaaay"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end

    test 'worker0 type logs are not shown' do
      @logger.info :worker0, "yaaay"
      assert{ !@log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end
  end

  sub_test_case "worker0 process type" do
    setup do
      dl_opts = {}
      dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      @logger = Fluent::Log.new(logger, process_type: :worker0, worker_id: 10)
    end

    test 'default type logs are shown w/ worker id' do
      @logger.info "yaaay"
      @logger.info :default, "booo"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: #10 yaaay\n") }
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: #10 booo\n") }
    end

    test 'supervisor type logs are not shown' do
      @logger.info :supervisor, "yaaay"
      assert{ !@log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end

    test 'worker0 type logs are shown w/o worker id' do
      @logger.info :worker0, "yaaay"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end
  end

  sub_test_case "workers process type" do
    setup do
      dl_opts = {}
      dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      @logger = Fluent::Log.new(logger, process_type: :workers, worker_id: 7)
    end

    test 'default type logs are shown w/ worker id' do
      @logger.info "yaaay"
      @logger.info :default, "booo"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: #7 yaaay\n") }
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: #7 booo\n") }
    end

    test 'supervisor type logs are not shown' do
      @logger.info :supervisor, "yaaay"
      assert{ !@log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end

    test 'worker0 type logs are not shown' do
      @logger.info :worker0, "yaaay"
      assert{ !@log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end
  end

  sub_test_case "standalone process type" do
    setup do
      dl_opts = {}
      dl_opts[:log_level] = ServerEngine::DaemonLogger::TRACE
      logdev = @log_device
      logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
      @logger = Fluent::Log.new(logger, process_type: :standalone, worker_id: 0)
    end

    test 'default type logs are shown w/o worker id' do
      @logger.info "yaaay"
      @logger.info :default, "booo"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: booo\n") }
    end

    test 'supervisor type logs are shown w/o worker id' do
      @logger.info :supervisor, "yaaay"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end

    test 'worker0 type logs are shown w/o worker id' do
      @logger.info :worker0, "yaaay"
      assert{ @log_device.logs.include?("#{@timestamp_str} [info]: yaaay\n") }
    end
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

    def test_optional_header
      assert_equal(@log.optional_header, @logger.optional_header)
      @log.optional_header = "optional_header"
      assert_equal(@log.optional_header, @logger.optional_header)
    end

    def test_optional_attrs
      assert_equal(@log.optional_attrs, @logger.optional_attrs)
      @log.optional_attrs = "optional_attrs"
      assert_equal(@log.optional_attrs, @logger.optional_attrs)
    end
  end
end

class PluginLoggerMixinTest < Test::Unit::TestCase
  class DummyPlugin < Fluent::Plugin::Input
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(DummyPlugin).configure(conf)
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

  def test_optional_header
    d = create_driver(%[@id myplugin])
    log = d.instance.log
    assert_equal("[myplugin] ", log.optional_header)
    assert_equal({}, log.optional_attrs)
  end

  def test_start
    plugin = DummyPlugin.new
    mock(plugin.log).should_receive(:reset).never
    plugin.start
  end

  def test_terminate
    plugin = DummyPlugin.new
    mock(plugin.log).reset
    plugin.terminate
  end
end

class LogDeviceIOTest < Test::Unit::TestCase
  test 'flush' do
    io = StringIO.new
    logdev = Fluent::LogDeviceIO.new(io)
    assert_equal io, logdev.flush

    io.instance_eval { undef :flush }
    logdev = Fluent::LogDeviceIO.new(io)
    assert_raise NoMethodError do
      logdev.flush
    end
  end

  test 'tty?' do
    io = StringIO.new
    logdev = Fluent::LogDeviceIO.new(io)
    assert_equal io.tty?, logdev.tty?

    io.instance_eval { undef :tty? }
    logdev = Fluent::LogDeviceIO.new(io)
    assert_raise NoMethodError do
      logdev.tty?
    end
  end

  test 'sync=' do
    io = StringIO.new
    logdev = Fluent::LogDeviceIO.new(io)
    assert_true logdev.sync = true

    io.instance_eval { undef :sync= }
    logdev = Fluent::LogDeviceIO.new(io)
    assert_raise NoMethodError do
      logdev.sync = true
    end
  end
end
