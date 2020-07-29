require_relative 'helper'
require 'fluent/daemonizer'

class DaemonizerTest < ::Test::Unit::TestCase
  TMP_DIR = File.join(File.dirname(__FILE__), 'tmp', 'daemonizer')

  setup do
    FileUtils.mkdir_p(TMP_DIR)
  end

  teardown do
    FileUtils.rm_rf(TMP_DIR) rescue nil
  end

  test 'makes pid file' do
    pid_path = File.join(TMP_DIR, 'file.pid')

    mock(Process).daemon(anything, anything).once
    r = Fluent::Daemonizer.daemonize(pid_path) { 'ret' }
    assert_equal 'ret', r
    assert File.exist?(pid_path)
    assert Process.pid.to_s, File.read(pid_path).to_s
  end

  test 'in platforms which do not support fork' do
    pid_path = File.join(TMP_DIR, 'file.pid')

    mock(Process).daemon(anything, anything) { raise NotImplementedError }
    args = ['-c', 'test.conf']
    mock(Process).spawn(anything, *args) { Process.pid }

    Fluent::Daemonizer.daemonize(pid_path, args) { 'ret' }
    assert File.exist?(pid_path)
    assert Process.pid.to_s, File.read(pid_path).to_s
  end

  sub_test_case 'when pid file already exists' do
    test 'raise an error when process is running' do
      omit 'chmod of file does not affetct root user' if Process.uid.zero?
      pid_path = File.join(TMP_DIR, 'file.pid')
      File.write(pid_path, '1')

      mock(Process).daemon(anything, anything).never
      mock(Process).kill(0, 1).once

      assert_raise(Fluent::ConfigError.new('pid(1) is running')) do
        Fluent::Daemonizer.daemonize(pid_path) { 'ret' }
      end
    end

    test 'raise an error when file is not redable' do
      omit 'chmod of file does not affetct root user' if Process.uid.zero?
      not_readable_path = File.join(TMP_DIR, 'not_readable.pid')

      File.write(not_readable_path, '1')
      FileUtils.chmod(0333, not_readable_path)

      mock(Process).daemon(anything, anything).never
      assert_raise(Fluent::ConfigError.new("Cannot access pid file: #{File.absolute_path(not_readable_path)}")) do
        Fluent::Daemonizer.daemonize(not_readable_path) { 'ret' }
      end
    end

    test 'raise an error when file is not writable' do
      omit 'chmod of file does not affetct root user' if Process.uid.zero?
      not_writable_path = File.join(TMP_DIR, 'not_writable.pid')

      File.write(not_writable_path, '1')
      FileUtils.chmod(0555, not_writable_path)

      mock(Process).daemon(anything, anything).never
      assert_raise(Fluent::ConfigError.new("Cannot access pid file: #{File.absolute_path(not_writable_path)}")) do
        Fluent::Daemonizer.daemonize(not_writable_path) { 'ret' }
      end
    end

    test 'raise an error when directory is not writable' do
      omit 'chmod of file does not affetct root user' if Process.uid.zero?
      not_writable_dir = File.join(TMP_DIR, 'not_writable')
      pid_path = File.join(not_writable_dir, 'file.pid')

      FileUtils.mkdir_p(not_writable_dir)
      FileUtils.chmod(0555, not_writable_dir)

      mock(Process).daemon(anything, anything).never
      assert_raise(Fluent::ConfigError.new("Cannot access directory for pid file: #{File.absolute_path(not_writable_dir)}")) do
        Fluent::Daemonizer.daemonize(pid_path) { 'ret' }
      end
    end
  end
end
