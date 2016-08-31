require_relative '../helper'
require 'fluent/plugin/compressable'

class CompressableTest < Test::Unit::TestCase
  include Fluent::Plugin::Compressable

  sub_test_case '#compress' do
    setup do
      @src = 'text data for compressing' * 5
      @gzipped_src = compress(@src)
    end

    test 'compress data' do
      str = "this is a sample text" * 10
      compressed_str = "\x1F\x8B\b\x00Y;\xC6W\x00\x03+\xC9\xC8,V\x00\xA2D\x85\xE2\xC4\xDC\x82\x9CT\x85\x92\xD4\x8A\x92\x92\xA1,\b\x00\xEF}\x9C\xF1\xD2\x00\x00\x00"
      assert_not_equal compressed_str, compress(str)
      assert_not_equal @gzipped_src, @src
    end

    test 'write compressed data to IO with output_io option' do
      io = StringIO.new
      compress(@src, output_io: io)
      assert_equal @gzipped_src, io.string
    end
  end

  sub_test_case '#decompress' do
    setup do
      @src = 'text data'
      @gzipped_src = compress(@src)
    end

    test 'decompress compressed data' do
      assert_equal @src, decompress(@gzipped_src)
    end

    test 'write decompressed data to IO with output_io option' do
      io = StringIO.new
      decompress(@gzipped_src, output_io: io)
      assert_equal @src, io.string
    end

    test 'decompress multiple compressed data' do
      src1 = 'text data'
      src2 = 'text data2'
      gzipped_src = compress(src1) + compress(src2)

      assert_equal src1 + src2, decompress(gzipped_src)
    end

    test 'decompress with input_io and output_io' do
      input_io = StringIO.new(@gzipped_src)
      output_io = StringIO.new

      decompress(input_io: input_io, output_io: output_io)
      assert_equal @src, output_io.string
    end

    test 'decompress multiple compressed data with input_io and output_io' do
      src1 = 'text data'
      src2 = 'text data2'
      gzipped_src = compress(src1) + compress(src2)

      input_io = StringIO.new(gzipped_src)
      output_io = StringIO.new

      decompress(input_io: input_io, output_io: output_io)
      assert_equal src1 + src2, output_io.string
    end
  end
end
