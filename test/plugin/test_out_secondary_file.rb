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

  class DummyOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end

    def register_value(name, value)
      instance_variable_set("@#{name}", value)
    end

    def write(chunk); end
  end

  def create_driver(conf = CONFIG, primary = DummyOutput.new)
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
      @record = { key: "vlaue" }
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
       ]
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
       ]
      c = create_es_chunk(create_metadata, @es)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      [*1..5].each do |i|
        path = d.instance.write(c)
        assert_equal "#{TMP_DIR}/out_file_test.#{c.chunk_id}.log.gz", path
        check_gzipped_result(path, packed_value * i)
      end
    end
  end

  sub_test_case 'path' do
    setup do
      @record = { key: "vlaue" }
      @time = Time.parse("2011-01-02 13:14:15 UTC").to_i
      @es = Fluent::OneEventStream.new(@time, @record)
      @c = create_es_chunk(create_metadata, @es)
    end

    test 'normal path' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        compress gz
      ]
      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/out_file_test.#{@c.chunk_id}_0.log.gz", path
    end

    test 'path includes *' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test/*.log
        compress gz
      ]

      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/out_file_test/#{@c.chunk_id}_0.log.gz", path
    end

    data(
      invalid_tag: ["tag", "#{TMP_DIR}/${tag}"],
      invalid_tag0: ["tag[0]", "#{TMP_DIR}/${tag[0]}"],
      invalid_variable: ["dummy", "#{TMP_DIR}/${dummy}"],
      invalid_timeformat: ["Time", "#{TMP_DIR}/%Y%m%d"],
    )
    test 'path includes impcompatible placeholder' do |(expected_message, invalid_path)|
      c = Fluent::Test::OutputTestDriver.new(Fluent::Plugin::SecondaryFileOutput)
      c.instance.acts_as_secondary(DummyOutput.new)

      assert_raise_message("BUG: file path has imcompatible placeholder: #{expected_message}") do
        c.configure(%[
          path #{invalid_path}
          compress gz
        ])
      end
    end

    test 'path includes tag' do
      primary = DummyOutput.new.tap do |e|
        e.register_value('chunk_key_tag', true)
        e.register_value('chunk_keys', [])
      end

      d = create_driver(%[
        path #{TMP_DIR}/out_file_test/cool_${tag}
        compress gz
      ], primary)

      c = create_es_chunk(create_metadata(nil, "test.dummy"), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/out_file_test/cool_test.dummy_0.log.gz", path
    end

    test 'path includes /tag[\d+]/' do
      primary = DummyOutput.new.tap do |e|
        e.register_value('chunk_key_tag', true)
        e.register_value('chunk_keys', [])
      end

      d = create_driver(%[
        path #{TMP_DIR}/out_file_test/cool_${tag[0]}_${tag[1]}
        compress gz
      ], primary)

      c = create_es_chunk(create_metadata(nil, "test.dummy"), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/out_file_test/cool_test_dummy_0.log.gz", path
    end

    test 'path includes time format' do
      primary = DummyOutput.new.tap do |e|
        e.register_value('chunk_key_time', true)
        e.register_value('chunk_keys', [])
      end

      d = create_driver(%[
        path #{TMP_DIR}/out_file_test/cool_%Y%m%d%H
        compress gz
      ], primary)

      c = create_es_chunk(create_metadata(Time.parse("2011-01-02 13:14:15 JST")), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/out_file_test/cool_2011010213_0.log.gz", path
    end

    test 'path includes time format and with `timekey_use_utc`' do
      primary = DummyOutput.new.tap do |e|
        e.register_value('chunk_key_time', true)
        e.register_value('chunk_keys', [])
      end

      d = create_driver(%[
        path #{TMP_DIR}/out_file_test/cool_%Y%m%d%H
        compress gz
      ], primary)

      c = create_es_chunk(create_metadata(Time.parse("2011-01-02 13:14:15 JST")), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/out_file_test/cool_2011010204_0.log.gz", path
    end

    test 'path includes variable' do
      pairs = { "test1".to_sym => "dummy" }
      primary = DummyOutput.new.tap do |e|
        e.register_value('chunk_keys', pairs.keys)
      end

      d = create_driver(%[
        path #{TMP_DIR}/out_file_test/cool_${test1}
        compress gz
      ], primary)

      c = create_es_chunk(create_metadata(nil, nil, pairs), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/out_file_test/cool_dummy_0.log.gz", path
    end

    test 'path include tag, time format, variables' do
      pairs = { "test1".to_sym => "dummy" }
      primary = DummyOutput.new.tap do |e|
        e.register_value('chunk_keys', pairs.keys)
        e.register_value('chunk_key_time', true)
        e.register_value('chunk_key_tag', true)
      end

      d = create_driver(%[
        path #{TMP_DIR}/out_file_test/cool_%Y%m%d%H_${tag}_${test1}
        compress gz
      ], primary)

      metadata = create_metadata(Time.parse("2011-01-02 13:14:15 JST"), 'test.tag', pairs)
      c = create_es_chunk(metadata, @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/out_file_test/cool_2011010213_test.tag_dummy_0.log.gz", path
    end
  end
end
