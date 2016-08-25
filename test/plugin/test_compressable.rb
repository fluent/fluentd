require_relative '../helper'
require 'fluent/plugin/compressable'

class CompressableTest < Test::Unit::TestCase
  include Fluent::Plugin::Compressable

  sub_test_case '#compress' do
    test 'compress data' do
      str = 'text data'
      compressed_str = compress(str)
      assert_not_equal compressed_str, str
    end
  end

  sub_test_case '#decompress' do
    test 'decompress compressed data' do
      str = 'text data'
      compressed_str = compress(str)
      assert_equal str, decompress(compressed_str)
    end

    test 'decompress multiple compressed data' do
      str1 = 'text data'
      str2 = 'text data2'
      compressed_str = compress(str1) + compress(str2)

      assert_equal str1 + str2, decompress(compressed_str)
    end
  end
end
