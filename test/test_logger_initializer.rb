require_relative 'helper'
require 'fluent/supervisor'
require 'fileutils'

class LoggerInitializerTest < ::Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/logger_initializer#{ENV['TEST_ENV_NUMBER']}")

  teardown do
    begin
      FileUtils.rm_rf(TMP_DIR)
    rescue => _
    end
  end

  test 'when path is given' do
    path = File.join(TMP_DIR, 'fluent_with_path.log')

    assert_false File.exist?(TMP_DIR)
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    mock.proxy(File).chmod(Fluent::DEFAULT_DIR_PERMISSION, TMP_DIR).once

    assert_nothing_raised do
      logger.init(:supervisor, 0)
    end

    assert_true File.exist?(TMP_DIR)
  end

  test 'apply_options with log_dir_perm' do
    omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

    path = File.join(TMP_DIR, 'fluent_with_path.log')

    assert_false File.exist?(TMP_DIR)
    logger = Fluent::Supervisor::LoggerInitializer.new(path, Fluent::Log::LEVEL_DEBUG, nil, nil, {})
    mock.proxy(File).chmod(0o777, TMP_DIR).once
    mock.proxy(File).chmod(Fluent::DEFAULT_DIR_PERMISSION, TMP_DIR).once

    assert_nothing_raised do
      logger.init(:supervisor, 0)
    end

    logger.apply_options(log_dir_perm: 0o777)
    assert_true File.exist?(TMP_DIR)
    assert_equal 0o777, (File.stat(TMP_DIR).mode & 0xFFF)
  end
end
