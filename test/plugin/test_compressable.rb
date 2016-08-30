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

    test 'write compressed data to IO with output_io option' do
      str = 'text data'
      compressed_str = compress(str)
      io = StringIO.new
      compress(str, output_io: io)
      assert_equal compressed_str, io.string
    end
  end

  sub_test_case '#decompress' do
    test 'decompress compressed data' do
      str = 'text data'
      compressed_str = compress(str)
      assert_equal str, decompress(compressed_str)
    end

    test 'write decompressed data to IO with output_io option' do
      str = 'text data'
      compressed_str = compress(str)
      io = StringIO.new
      decompress(compressed_str, output_io: io)
      assert_equal str, io.string
    end

    test 'decompress multiple compressed data' do
      str1 = 'text data'
      str2 = 'text data2'
      compressed_str = compress(str1) + compress(str2)

      assert_equal str1 + str2, decompress(compressed_str)
    end

    test 'decompress with input_io and output_io' do
      str = 'text data'
      input_io = StringIO.new(compress(str))
      output_io = StringIO.new

      decompress(input_io: input_io, output_io: output_io)
      assert_equal str, output_io.string
    end
  end
end
