require_relative 'helper'
require 'fluent/supervisor'
require 'fileutils'

class LoggerInitializerTest < ::Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/logger_initializer#{ENV['TEST_ENV_NUMBER']}")

  setup do
    FileUtils.rm_rf(TMP_DIR) rescue nil
    @stored_global_logger = $log
  end

  teardown do
    $log = @stored_global_logger
  end

  test 'when path is given' do
    path = File.join(TMP_DIR, 'fluent_with_path.log')

    assert_false File.exist?(TMP_DIR)
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    mock.proxy(File).chmod(0o777, TMP_DIR).never

    assert_nothing_raised do
      logger.init(:supervisor, 0)
    end
    $log.out.close

    assert_true File.exist?(TMP_DIR)
  end

  test 'apply_options with log_dir_perm' do
    omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

    path = File.join(TMP_DIR, 'fluent_with_path.log')

    assert_false File.exist?(TMP_DIR)
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    mock.proxy(File).chmod(0o777, TMP_DIR).once

    assert_nothing_raised do
      logger.init(:supervisor, 0)
    end
    logger.apply_options(log_dir_perm: 0o777)
    $log.out.close

    assert_true File.exist?(TMP_DIR)
    assert_equal 0o777, (File.stat(TMP_DIR).mode & 0xFFF)
  end

  test 'rotate' do
    assert { not File.exist?(TMP_DIR) }

    path = File.join(TMP_DIR, 'fluent.log')
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5, log_rotate_size: 500)
    logger.init(:supervisor, 0)
    begin
      10.times.each do
        $log.info "This is test message. This is test message. This is test message."
      end
    ensure
      $log.out.close
    end

    assert { Dir.entries(TMP_DIR).size > 3 } # [".", "..", "logfile.log", ...]
  end

  test 'rotate to max age' do
    assert { not File.exist?(TMP_DIR) }

    path = File.join(TMP_DIR, 'fluent.log')
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5, log_rotate_size: 500)
    logger.init(:supervisor, 0)
    begin
      100.times.each do
        $log.info "This is test message. This is test message. This is test message."
      end
    ensure
      $log.out.close
    end

    assert { Dir.entries(TMP_DIR).size == 7 } # [".", "..", "logfile.log", ...]
  end

  test 'files for each process with rotate on Windows' do
    omit "Only for Windows." unless Fluent.windows?

    assert { not File.exist?(TMP_DIR) }

    path = File.join(TMP_DIR, 'fluent.log')
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:supervisor, 0)
    $log.out.close

    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:worker0, 0)
    $log.out.close

    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:workers, 1)
    $log.out.close

    assert { Dir.entries(TMP_DIR).size == 5 } # [".", "..", "logfile.log", ...]
  end

  test 'reopen!' do
    assert { not File.exist?(TMP_DIR) }

    path = File.join(TMP_DIR, 'fluent.log')
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    logger.init(:supervisor, 0)
    message = "This is test message."
    $log.info message
    logger.reopen!
    $log.info message
    $log.out.close

    assert { File.read(path).lines.select{ |line| line.include?(message) }.size == 2 }
  end

  test 'reopen! with rotate reopens the same file' do
    assert { not File.exist?(TMP_DIR) }

    path = File.join(TMP_DIR, 'fluent.log')
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:supervisor, 0)
    logger.reopen!
    $log.out.close

    assert { Dir.entries(TMP_DIR).size == 3 } # [".", "..", "logfile.log", ...]
  end
end
