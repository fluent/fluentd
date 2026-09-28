require_relative '../helper'
require 'fluent/plugin/extractor'
require 'stringio'
require 'zlib'
require 'zstd-ruby'

class ExtractorTest < Test::Unit::TestCase
  setup do
    @strict_limit = 1023
    @safe_limit = 1024
    @original_text = "A" * @safe_limit
  end

  def create_gzip(data)
    io = StringIO.new
    gz = Zlib::GzipWriter.new(io)
    gz.write(data)
    gz.close
    io.string
  end

  def create_multi_gzip(data1, data2)
    create_gzip(data1) + create_gzip(data2)
  end

  def create_zstd(data)
    Zstd.compress(data)
  end

  def create_multi_zstd(data1, data2)
    create_zstd(data1) + create_zstd(data2)
  end

  def create_deflate(data)
    Zlib::Deflate.deflate(data)
  end

  def incompressible_text(size)
    random = Random.new(0)
    Array.new(size) { random.rand(36).to_s(36) }.join
  end

  sub_test_case 'decompress_gzip' do
    test 'successfully decompresses data within the limit' do
      compressed = create_gzip(@original_text)
      result = Fluent::Plugin::Extractor.decompress_gzip(compressed, limit: @safe_limit)
      assert_equal @original_text, result
    end

    test 'successfully decompresses multi-member gzip data' do
      compressed = create_multi_gzip("Hello ", "World!")
      result = Fluent::Plugin::Extractor.decompress_gzip(compressed, limit: @safe_limit)
      assert_equal "Hello World!", result
    end

    test 'raises an error when decompressed data exceeds the limit' do
      compressed = create_gzip(@original_text)
      err = assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.decompress_gzip(compressed, limit: @strict_limit)
      end
      assert_equal "Decompressed data exceeds limit of #{@strict_limit} bytes", err.message
    end
  end

  sub_test_case 'decompress_zstd' do
    test 'successfully decompresses data within the limit' do
      compressed = create_zstd(@original_text)
      result = Fluent::Plugin::Extractor.decompress_zstd(compressed, limit: @safe_limit)
      assert_equal @original_text, result
    end

    test 'successfully decompresses multi-member zstd data' do
      compressed = create_multi_zstd("Hello ", "World!")
      result = Fluent::Plugin::Extractor.decompress_zstd(compressed, limit: @safe_limit)
      assert_equal "Hello World!", result
    end

    test 'raises an error when decompressed data exceeds the limit' do
      compressed = create_zstd(@original_text)
      err = assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.decompress_zstd(compressed, limit: @strict_limit)
      end
      assert_equal "Decompressed data exceeds limit of #{@strict_limit} bytes", err.message
    end
  end

  sub_test_case 'decompress_deflate' do
    test 'successfully decompresses data within the limit' do
      compressed = create_deflate(@original_text)
      result = Fluent::Plugin::Extractor.decompress_deflate(compressed, limit: @safe_limit)
      assert_equal @original_text, result
    end

    test 'raises an error when decompressed data exceeds the limit' do
      compressed = create_deflate(@original_text)
      err = assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.decompress_deflate(compressed, limit: @strict_limit)
      end
      assert_equal "Decompressed data exceeds limit of #{@strict_limit} bytes", err.message
    end
  end

  sub_test_case 'io_decompress_gzip' do
    test 'successfully decompresses data within the limit' do
      input = StringIO.new(create_gzip(@original_text))
      output = StringIO.new
      Fluent::Plugin::Extractor.io_decompress_gzip(input, output, limit: @safe_limit)
      assert_equal @original_text, output.string
    end

    test 'successfully decompresses multi-member gzip data' do
      input = StringIO.new(create_multi_gzip("Hello ", "World!"))
      output = StringIO.new
      Fluent::Plugin::Extractor.io_decompress_gzip(input, output, limit: @safe_limit)
      assert_equal "Hello World!", output.string
    end

    test 'raises an error when decompressed data exceeds the limit' do
      input = StringIO.new(create_gzip(@original_text))
      output = StringIO.new
      err = assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.io_decompress_gzip(input, output, limit: @strict_limit)
      end
      assert_equal "Decompressed data exceeds limit of #{@strict_limit} bytes", err.message
    end

    test 'counts the size across gzip members' do
      half = "A" * (@safe_limit / 2)
      input = StringIO.new(create_multi_gzip(half, half))
      output = StringIO.new
      assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.io_decompress_gzip(input, output, limit: @strict_limit)
      end
    end
  end

  sub_test_case 'io_decompress_zstd' do
    test 'successfully decompresses data within the limit' do
      input = StringIO.new(create_zstd(@original_text))
      output = StringIO.new
      Fluent::Plugin::Extractor.io_decompress_zstd(input, output, limit: @safe_limit)
      assert_equal @original_text, output.string
    end

    test 'successfully decompresses multi-member zstd data' do
      input = StringIO.new(create_multi_zstd("Hello ", "World!"))
      output = StringIO.new
      Fluent::Plugin::Extractor.io_decompress_zstd(input, output, limit: @safe_limit)
      assert_equal "Hello World!", output.string
    end

    test 'raises an error when decompressed data exceeds the limit' do
      input = StringIO.new(create_zstd(@original_text))
      output = StringIO.new
      err = assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.io_decompress_zstd(input, output, limit: @strict_limit)
      end
      assert_equal "Decompressed data exceeds limit of #{@strict_limit} bytes", err.message
    end

    test 'counts the size across zstd members' do
      # each member must be larger than ZSTD_BYTES_TO_READ compressed and smaller than the limit
      # decompressed, so that the limit can only be reached by accumulating across members
      member = incompressible_text(2048)
      compressed = create_multi_zstd(member, member)
      assert_operator Zstd.compress(member).bytesize, :>, Fluent::Plugin::Extractor::ZSTD_BYTES_TO_READ

      input = StringIO.new(compressed)
      output = StringIO.new
      assert_raise(Fluent::Plugin::Extractor::SizeLimitError) do
        Fluent::Plugin::Extractor.io_decompress_zstd(input, output, limit: 3000)
      end
    end
  end
end
