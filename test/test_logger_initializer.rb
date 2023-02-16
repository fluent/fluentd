require_relative 'helper'
require 'fluent/supervisor'
require 'fileutils'
require 'pathname'

class LoggerInitializerTest < ::Test::Unit::TestCase
  def setup
    @stored_global_logger = $log
    Dir.mktmpdir do |tmp_dir|
      @tmp_dir = Pathname(tmp_dir)
      yield
    end
  end

  def teardown
    $log = @stored_global_logger
  end

  test 'when path is given' do
    path = @tmp_dir + 'log' + 'fluent_with_path.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    mock.proxy(File).chmod(0o777, path.parent.to_s).never

    assert_nothing_raised do
      logger.init(:supervisor, 0)
    end
    $log.out.close

    assert { path.parent.exist? }
  end

  test 'apply_options with log_dir_perm' do
    omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

    path = @tmp_dir + 'log' + 'fluent_with_path.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    mock.proxy(File).chmod(0o777, path.parent.to_s).once

    assert_nothing_raised do
      logger.init(:supervisor, 0)
    end
    logger.apply_options(log_dir_perm: 0o777)
    $log.out.close

    assert { path.parent.exist? }
    assert_equal 0o777, (File.stat(path.parent).mode & 0xFFF)
  end

  test 'rotate' do
    path = @tmp_dir + 'log' + 'fluent.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5, log_rotate_size: 500)
    logger.init(:supervisor, 0)
    begin
      10.times.each do
        $log.info "This is test message. This is test message. This is test message."
      end
    ensure
      $log.out.close
    end

    assert { path.parent.entries.size > 3 } # [".", "..", "logfile.log", ...]
  end

  test 'rotate to max age' do
    path = @tmp_dir + 'log' + 'fluent.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5, log_rotate_size: 500)
    logger.init(:supervisor, 0)
    begin
      100.times.each do
        $log.info "This is test message. This is test message. This is test message."
      end
    ensure
      $log.out.close
    end

    assert { path.parent.entries.size == 7 } # [".", "..", "logfile.log", ...]
  end

  test 'files for each process with rotate on Windows' do
    omit "Only for Windows." unless Fluent.windows?

    path = @tmp_dir + 'log' + 'fluent.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:supervisor, 0)
    $log.out.close

    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:worker0, 0)
    $log.out.close

    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:workers, 1)
    $log.out.close

    assert { path.parent.entries.size == 5 } # [".", "..", "logfile.log", ...]
  end

  test 'reopen!' do
    path = @tmp_dir + 'log' + 'fluent.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    logger.init(:supervisor, 0)
    message = "This is test message."
    $log.info message
    logger.reopen!
    $log.info message
    $log.out.close

    assert { path.read.lines.select{ |line| line.include?(message) }.size == 2 }
  end

  test 'reopen! with rotate reopens the same file' do
    path = @tmp_dir + 'log' + 'fluent.log'
    logger = Fluent::Supervisor::LoggerInitializer.new(path.to_s, Fluent::Log::LEVEL_DEBUG, nil, nil, {}, log_rotate_age: 5)
    logger.init(:supervisor, 0)
    logger.reopen!
    $log.out.close

    assert { path.parent.entries.size == 3 } # [".", "..", "logfile.log", ...]
  end
end
