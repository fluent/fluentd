require_relative '../helper'
require 'fluent/plugin/file_wrapper'

class FileWrapperTest < Test::Unit::TestCase
  require 'windows/file'
  require 'windows/error'
  include Windows::File
  include Windows::Error

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/file_wrapper#{ENV['TEST_ENV_NUMBER']}"

  def setup
    FileUtils.mkdir_p(TMP_DIR)
  end

  def teardown
    FileUtils.rm_rf(TMP_DIR)
  end

  sub_test_case 'Win32Error' do
    test 'equal' do
      assert_equal(Fluent::Win32Error.new(ERROR_SHARING_VIOLATION),
                   Fluent::Win32Error.new(ERROR_SHARING_VIOLATION))
    end

    test 'different error code' do
      assert_not_equal(Fluent::Win32Error.new(ERROR_FILE_NOT_FOUND),
                       Fluent::Win32Error.new(ERROR_SHARING_VIOLATION))
    end

    test 'different class' do
      assert_not_equal(Errno::EPIPE,
                       Fluent::Win32Error.new(ERROR_SHARING_VIOLATION))
    end

    test 'ERROR_SHARING_VIOLATION message' do
      assert_equal(Fluent::Win32Error.new(ERROR_SHARING_VIOLATION).message,
                   "code: 32, The process cannot access the file because it is being used by another process.")
    end
  end

  sub_test_case 'WindowsFile exceptions' do
    test 'nothing raised' do
      begin
        path = "#{TMP_DIR}/test_windows_file.txt"
        file1 = file2 = nil
        file1 = File.open(path, "wb") do |f|
        end
        assert_nothing_raised do
          file2 = Fluent::WindowsFile.new(path)
        ensure
          file2.close
        end
      ensure
        file1.close if file1
      end
    end

    test 'Errno::ENOENT raised' do
      path = "#{TMP_DIR}/nofile.txt"
      file = nil
      assert_raise(Errno::ENOENT) do
        file = Fluent::WindowsFile.new(path)
      ensure
        file.close if file
      end
    end

    test 'ERROR_SHARING_VIOLATION raised' do
      begin
        path = "#{TMP_DIR}/test_windows_file.txt"
        file1 = file2 = nil
        file1 = File.open(path, "wb")
        assert_raise(Fluent::Win32Error.new(ERROR_SHARING_VIOLATION)) do
          file2 = Fluent::WindowsFile.new(path, 'r', FILE_SHARE_READ)
        ensure
          file2.close if file2
        end
      ensure
        file1.close if file1
      end
    end
  end
end if Fluent.windows?
