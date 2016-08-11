require_relative '../helper'
require 'time'
require 'fileutils'
require 'fluent/test'
require 'fluent/event'
require 'fluent/plugin/buffer'
require 'fluent/plugin/out_secondary_file'
require 'fluent/plugin/buffer/memory_chunk'

class FileOutputSecondaryTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/out_file#{ENV['TEST_ENV_NUMBER']}")

  CONFIG = %[
    path #{TMP_DIR}/out_file_test
    compress gz
  ]

  def create_driver(conf = CONFIG)
    primary = Class.new
    c = Fluent::Test::OutputTestDriver.new(Fluent::Plugin::SecondaryFileOutput)
    c.instance.acts_as_secondary(primary)
    c.configure(conf)
  end

  sub_test_case 'configture' do
    test 'should b econfigurable' do
      d = create_driver %[
      path test_path
      compress gz
    ]
      assert_equal 'test_path', d.instance.path
      assert_equal :gz, d.instance.compress
    end

    test 'should only use in secondary' do
      c = Fluent::Test::OutputTestDriver.new(Fluent::Plugin::SecondaryFileOutput)
      assert_raise do
        c.configure(conf)
      end
    end

    test 'should receive a file path' do
      assert_raise(Fluent::ConfigError) do
        create_driver %[
        compress gz
      ]
      end
    end

    test 'should be writable direcotry' do
      assert_nothing_raised do
        create_driver %[path #{TMP_DIR}/test_path]
      end

      assert_nothing_raised do
        FileUtils.mkdir_p("#{TMP_DIR}/test_dir")
        File.chmod(0777, "#{TMP_DIR}/test_dir")
        create_driver %[path #{TMP_DIR}/test_dir/foo/bar/baz]
      end

      assert_raise(Fluent::ConfigError) do
        FileUtils.mkdir_p("#{TMP_DIR}/test_dir")
        File.chmod(0555, "#{TMP_DIR}/test_dir")
        create_driver %[path #{TMP_DIR}/test_dir/foo/bar/baz]
      end
    end
  end

  def check_gzipped_result(path, expect)
    # Zlib::GzipReader has a bug of concatenated file: https://bugs.ruby-lang.org/issues/9790
    # Following code from https://www.ruby-forum.com/topic/971591#979520
    result = ""
    File.open(path, "rb") { |io|
      loop do
        gzr = Zlib::GzipReader.new(io)
        result << gzr.read
        unused = gzr.unused
        gzr.finish
        break if unused.nil?
        io.pos -= unused.length
      end
    }

    assert_equal expect, result
  end

  class DummyMemoryChunk < Fluent::Plugin::Buffer::MemoryChunk; end

  def create_metadata(timekey=nil, tag=nil, variables=nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end

  def create_es_chunk(metadata, es)
    DummyMemoryChunk.new(metadata).tap do |c|
      c.concat(es.to_msgpack_stream, es.size) # to_msgpack_stream is standard_format
      c.commit
    end
  end

  sub_test_case 'write' do
    setup do
      @record = { "key" => "vlaue"}
      @time = Time.parse("2011-01-02 13:14:15 UTC").to_i
      @es = Fluent::OneEventStream.new(@time, @record)
    end

    test 'should be output with standarad format' do
      d = create_driver
      c = create_es_chunk(create_metadata, @es)
      path = d.instance.write(c)

      assert_equal "#{TMP_DIR}/out_file_test.#{c.chunk_id}_0.log.gz", path
      check_gzipped_result(path, @es.to_msgpack_stream.force_encoding('ASCII-8BIT'))
    end

    test 'should be output unzip file' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
      ]
      c = create_es_chunk(create_metadata, @es)
      path = d.instance.write(c)

      assert_equal "#{TMP_DIR}/out_file_test.#{c.chunk_id}_0.log", path
      assert_equal File.read(path), @es.to_msgpack_stream.force_encoding('ASCII-8BIT')
    end

    test 'path should be increment without append' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        compress gz
      # ]
      c = create_es_chunk(create_metadata, @es)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      5.times do |i|
        path = d.instance.write(c)
        assert_equal "#{TMP_DIR}/out_file_test.#{c.chunk_id}_#{i}.log.gz", path
        check_gzipped_result(path, packed_value )
      end
    end

    test 'path should be same with append' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        compress gz
        append true
      # ]
      c = create_es_chunk(create_metadata, @es)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      [*1..5].each do |i|
        path = d.instance.write(c)
        assert_equal "#{TMP_DIR}/out_file_test.#{c.chunk_id}.log.gz", path
        check_gzipped_result(path, packed_value * i)
      end
    end
  end

  sub_test_case 'path tempalte' do
    # test that uses generate_temaplte
  end

  # sub_test_case 'path' do
  #   test 'normal' do
  #     d = create_driver(%[
  #       path #{TMP_DIR}/out_file_test
  #       time_slice_format %Y-%m-%d-%H
  #       utc true
  #     ])
  #     time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #     d.emit({"a"=>1}, time)
  #     # FileOutput#write returns path
  #     paths = d.run
  #     assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13_0.log"], paths
  #   end

  #   test 'normal with append' do
  #     d = create_driver(%[
  #       path #{TMP_DIR}/out_file_test
  #       time_slice_format %Y-%m-%d-%H
  #       utc true
  #       append true
  #     ])
  #     time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #     d.emit({"a"=>1}, time)
  #     paths = d.run
  #     assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13.log"], paths
  #   end

  #   test '*' do
  #     d = create_driver(%[
  #       path #{TMP_DIR}/out_file_test.*.txt
  #       time_slice_format %Y-%m-%d-%H
  #       utc true
  #     ])
  #     time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #     d.emit({"a"=>1}, time)
  #     paths = d.run
  #     assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13_0.txt"], paths
  #   end

  #   test '* with append' do
  #     d = create_driver(%[
  #       path #{TMP_DIR}/out_file_test.*.txt
  #       time_slice_format %Y-%m-%d-%H
  #       utc true
  #       append true
  #     ])
  #     time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #     d.emit({"a"=>1}, time)
  #     paths = d.run
  #     assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13.txt"], paths
  #   end
  # end
end
