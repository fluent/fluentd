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

    test 'directory exists' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_dir")
    end

    test 'file does not exist and parent directory is writable' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      assert_true Fluent::FileUtil.writable?("#{TEST_DIR}/test_dir/test_file")
    end

    test 'file does not exist and parent directory is not writable' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      File.chmod(0444, "#{TEST_DIR}/test_dir")
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_dir/test_file")
    end

    test 'parent directory does not exist' do
      FileUtils.rm_rf("#{TEST_DIR}/test_dir")
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_dir/test_file")
    end

    test 'parent file (not directory) exists' do
      FileUtils.touch("#{TEST_DIR}/test_file")
      assert_false Fluent::FileUtil.writable?("#{TEST_DIR}/test_file/foo")
    end
  end

  sub_test_case 'writable_p?' do
    test 'file exists and writable' do
      FileUtils.touch("#{TEST_DIR}/test_file")
      assert_true Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_file")
    end

    test 'file exists and not writable' do
      FileUtils.touch("#{TEST_DIR}/test_file")
      File.chmod(0444, "#{TEST_DIR}/test_file")
      assert_false Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_file")
    end

    test 'directory exists' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      assert_false Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_dir")
    end

    test 'parent directory exists and writable' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      assert_true Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_dir/test_file")
    end

    test 'parent directory exists and not writable' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      File.chmod(0555, "#{TEST_DIR}/test_dir")
      assert_false Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_dir/test_file")
    end

    test 'parent of parent (of parent ...) directory exists and writable' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      assert_true Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_dir/foo/bar/baz")
    end

    test 'parent of parent (of parent ...) directory exists and not writable' do
      FileUtils.mkdir_p("#{TEST_DIR}/test_dir")
      File.chmod(0555, "#{TEST_DIR}/test_dir")
      assert_false Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_dir/foo/bar/baz")
    end

    test 'parent of parent (of parent ...) file (not directory) exists' do
      FileUtils.touch("#{TEST_DIR}/test_file")
      assert_false Fluent::FileUtil.writable_p?("#{TEST_DIR}/test_file/foo/bar/baz")
    end
  end
end
