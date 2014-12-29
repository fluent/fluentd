require_relative '../helper'
require 'fluent/plugin/file_util'
require 'fileutils'

class FileUtilTest < Test::Unit::TestCase
  def setup
    FileUtils.rm_rf(TEST_DIR)
    FileUtils.mkdir_p(TEST_DIR)
  end

  TEST_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/file_util")

  sub_test_case 'writable?' do
    test 'file exists and writable' do
      FileUtils.touch("#{TEST_DIR}/test_file")
      assert_true Fluent::FileUtil.writable?("#{TEST_DIR}/test_file")
    end

    test 'file exists and not writable' do
      FileUtils.touch("#{TEST_DIR}/test_file")
      File.chmod(0444, "#{TEST_DIR}/test_file")
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_file")
    end

    test 'file does not exist and directory is writable' do
      assert_true Fluent::FileUtil.writable?("#{TEST_DIR}/test_file")
    end

    test 'file does not exist and directory is not writable' do
      File.chmod(0444, TEST_DIR)
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_file")
    end

    test 'directory does not exist' do
      FileUtils.rm_rf(TEST_DIR)
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_file")
    end
  end
end
