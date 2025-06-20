require_relative '../helper'
require 'fluent/plugin/compressable'

class CompressableTest < Test::Unit::TestCase
  include Fluent::Plugin::Compressable

  def compress_assert_equal(expected, actual)
    e = Zlib::GzipReader.new(StringIO.new(expected)).read
    a = Zlib::GzipReader.new(StringIO.new(actual)).read
    assert_equal(e, a)
  end

  sub_test_case '#compress' do
    setup do
      @src = 'text data for compressing' * 5
      @gzipped_src = compress(@src)
    end

    test 'compress data' do
      assert compress(@src).size < @src.size
      assert_not_equal @gzipped_src, @src
    end

    test 'write compressed data to IO with output_io option' do
      io = StringIO.new
      compress(@src, output_io: io)
      compress_assert_equal @gzipped_src, io.string
    end
  end

  sub_test_case '#decompress' do
    setup do
      @src = 'text data for compressing' * 5
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

    test 'return decompressed string with output_io option' do
      io = StringIO.new(@gzipped_src)
      assert_equal @src, decompress(input_io: io)
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

    test 'return the received value as it is with empty string or nil' do
      assert_equal nil, decompress
      assert_equal nil, decompress(nil)
      assert_equal '', decompress('')
      assert_equal '', decompress('', output_io: StringIO.new)
    end

    test 'decompress large zstd compressed data' do
      src1 = SecureRandom.random_bytes(1024)
      src2 = SecureRandom.random_bytes(1024)
      src3 = SecureRandom.random_bytes(1024)

      zstd_compressed_data = compress(src1, type: :zstd) + compress(src2, type: :zstd) + compress(src3, type: :zstd)
      assert_equal src1 + src2 + src3, decompress(zstd_compressed_data, type: :zstd)
    end

    test 'decompress large zstd compressed data with input_io and output_io' do
      src1 = SecureRandom.random_bytes(1024)
      src2 = SecureRandom.random_bytes(1024)
      src3 = SecureRandom.random_bytes(1024)

      zstd_compressed_data = compress(src1, type: :zstd) + compress(src2, type: :zstd) + compress(src3, type: :zstd)

      input_io = StringIO.new(zstd_compressed_data)
      output_io = StringIO.new
      output_io.set_encoding(src1.encoding)

      decompress(input_io: input_io, output_io: output_io, type: :zstd)
      assert_equal src1 + src2 + src3, output_io.string
    end
  end
end
