require_relative '../helper'
require 'time'
require 'fileutils'
require 'fluent/event'
require 'fluent/unique_id'
require 'fluent/plugin/buffer'
require 'fluent/plugin/out_secondary_file'
require 'fluent/plugin/buffer/memory_chunk'
require 'fluent/test/driver/output'

class FileOutputSecondaryTest < Test::Unit::TestCase
  include Fluent::UniqueId::Mixin

  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/out_secondary_file#{ENV['TEST_ENV_NUMBER']}")

  CONFIG = %[
    directory #{TMP_DIR}
    basename out_file_test
    compress gzip
  ]

  class DummyOutput < Fluent::Plugin::Output
    def write(chunk); end
  end

  def create_primary(buffer_cofig = config_element('buffer'))
    DummyOutput.new.configure(config_element('ROOT','',{}, [buffer_cofig]))
  end

  def create_driver(conf = CONFIG, primary = create_primary)
    c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
    c.instance.acts_as_secondary(primary)
    c.configure(conf)
  end

  sub_test_case 'configture' do
    test 'default configuration' do
      d = create_driver %[directory #{TMP_DIR}]
      assert_equal 'dump.bin', d.instance.basename
      assert_equal TMP_DIR, d.instance.directory
      assert_equal :text, d.instance.compress
    end

    test 'should be configurable' do
      d = create_driver
      assert_equal 'out_file_test', d.instance.basename
      assert_equal TMP_DIR, d.instance.directory
      assert_equal :gzip, d.instance.compress
    end

    test 'should only use in secondary' do
      c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
      assert_raise do
        c.configure(conf)
      end
    end

    test 'directory should be writable' do
      assert_nothing_raised do
        create_driver %[directory #{TMP_DIR}/test_dir/foo/bar/]
      end

      assert_nothing_raised do
        FileUtils.mkdir_p("#{TMP_DIR}/test_dir")
        File.chmod(0777, "#{TMP_DIR}/test_dir")
        create_driver %[directory #{TMP_DIR}/test_dir/foo/bar/]
      end

      assert_raise Fluent::ConfigError do
        FileUtils.mkdir_p("#{TMP_DIR}/test_dir")
        File.chmod(0555, "#{TMP_DIR}/test_dir")
        create_driver %[directory #{TMP_DIR}/test_dir/foo/bar/]
      end
    end

    test 'should be passed directory' do
      assert_raise Fluent::ConfigError do
        create_driver %[]
      end

      assert_nothing_raised do
        create_driver %[directory #{TMP_DIR}/test_dir/foo/bar/]
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

    test 'should output compressed file with gzip option' do
      d = create_driver
      c = create_es_chunk(create_metadata, @es)
      path = d.instance.write(c)

      assert_equal "#{TMP_DIR}/out_file_test.0.gz", path
      check_gzipped_result(path, @es.to_msgpack_stream.force_encoding('ASCII-8BIT'))
    end

    test 'should be output plain text without gzip option' do
      d = create_driver %[
        directory #{TMP_DIR}/
        basename out_file_test
      ]
      c = create_es_chunk(create_metadata, @es)
      path = d.instance.write(c)

      assert_equal "#{TMP_DIR}/out_file_test.0", path
      assert_equal File.read(path), @es.to_msgpack_stream.force_encoding('ASCII-8BIT')
    end

    test 'path should be incremental without append option' do
      d = create_driver
      c = create_es_chunk(create_metadata, @es)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      5.times do |i|
        path = d.instance.write(c)
        assert_equal "#{TMP_DIR}/out_file_test.#{i}.gz", path
        check_gzipped_result(path, packed_value)
      end
    end

    test 'path should be the same with append option' do
      d = create_driver CONFIG + %[append true]
      c = create_es_chunk(create_metadata, @es)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      [*1..5].each do |i|
        path = d.instance.write(c)
        assert_equal "#{TMP_DIR}/out_file_test.gz", path
        check_gzipped_result(path, packed_value * i)
      end
    end
  end

  sub_test_case 'placeholder regex' do
    data(
      tag: '${tag}',
      tag_index: '${tag[0]}',
      tag_index1: '${tag[10]}',
      variable: '${key1}',
      variable2: '${key@value}',
      variable3: '${key_value}',
      variable4: '${key.value}',
      variable5: '${key-value}',
      variable6: '${KEYVALUE}',
      variable7: '${tags}'
    )
    test 'matches with a valid placeholder' do |path|
      assert Fluent::Plugin::SecondaryFileOutput::PLACEHOLDER_REGEX.match(path)
    end

    data(
      invalid_tag: 'tag',
      invalid_tag2: '{tag}',
      invalid_tag3: '${tag',
      invalid_variable: '${key[0]}',
    )
    test "doesn't match with an invalid placeholder" do |path|
      assert !Fluent::Plugin::SecondaryFileOutput::PLACEHOLDER_REGEX.match(path)
    end
  end

  sub_test_case 'path' do
    setup do
      @record = { key: 'value' }
      @time = Time.parse('2011-01-02 13:14:15 UTC').to_i
      @es = Fluent::OneEventStream.new(@time, @record)
      @c = create_es_chunk(create_metadata, @es)
    end

    test 'normal path with gzip option' do
      d = create_driver
      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/out_file_test.0.gz", path
    end

    test 'normal path without gzip option' do
      d = create_driver %[
        directory #{TMP_DIR}
        basename out_file_test
      ]
      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/out_file_test.0", path
    end

    data(
      invalid_tag: [/tag/, '${tag}'],
      invalid_tag0: [/tag\[0\]/, '${tag[0]}'],
      invalid_variable: [/dummy/, '${dummy}'],
      invalid_timeformat: [/time/, '%Y%m%d'],
    )
    test 'raise an error when basename includes incompatible placeholder' do |(expected_message, invalid_basename)|
      c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
      c.instance.acts_as_secondary(DummyOutput.new)

      assert_raise_message(expected_message) do
        c.configure %[
          directory #{TMP_DIR}/
          basename #{invalid_basename}
          compress gzip
        ]
      end
    end

    data(
      invalid_tag: [/tag/, '${tag}'],
      invalid_tag0: [/tag\[0\]/, '${tag[0]}'],
      invalid_variable: [/dummy/, '${dummy}'],
      invalid_timeformat: [/time/, '%Y%m%d'],
    )
    test 'raise an error when directory includes incompatible placeholder' do |(expected_message, invalid_directory)|
      c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
      c.instance.acts_as_secondary(DummyOutput.new)

      assert_raise_message(expected_message) do
        c.configure %[
          directory #{invalid_directory}/
          compress gzip
        ]
      end
    end

    test 'basename includes tag' do
      primary = create_primary(config_element('buffer', 'tag'))

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_${tag}
        compress gzip
      ], primary)

      c = create_es_chunk(create_metadata(nil, 'test.dummy'), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/cool_test.dummy.0.gz", path
    end

    test 'basename includes /tag[\d+]/' do
      primary = create_primary(config_element('buffer', 'tag'))

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_${tag[0]}_${tag[1]}
        compress gzip
      ], primary)

      c = create_es_chunk(create_metadata(nil, 'test.dummy'), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/cool_test_dummy.0.gz", path
    end

    test 'basename includes time format' do
      primary = create_primary(
        config_element('buffer', 'time', { 'timekey_zone' => '+0900', 'timekey' => 1 })
      )

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_%Y%m%d%H
        compress gzip
      ], primary)

      c = create_es_chunk(create_metadata(Time.parse("2011-01-02 13:14:15 UTC")), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/cool_2011010222.0.gz", path
    end

    test 'basename includes time format with timekey_use_utc option' do
      primary = create_primary(
        config_element('buffer', 'time', { 'timekey_zone' => '+0900', 'timekey' => 1, 'timekey_use_utc' => true })
      )

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_%Y%m%d%H
        compress gzip
      ], primary)

      c = create_es_chunk(create_metadata(Time.parse("2011-01-02 13:14:15 UTC")), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/cool_2011010213.0.gz", path
    end

    test 'basename includes variable' do
      primary = create_primary(config_element('buffer', 'test1'))

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_${test1}
        compress gzip
      ], primary)

      c = create_es_chunk(create_metadata(nil, nil, { "test1".to_sym => "dummy" }), @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/cool_dummy.0.gz", path
    end

    test 'basename includes unnecessary variable' do
      primary = create_primary(config_element('buffer', 'test1'))
      c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
      c.instance.acts_as_secondary(primary)

      assert_raise_message(/test2/) do
        c.configure %[
          directory #{TMP_DIR}/
          basename ${test1}_${test2}
          compress gzip
        ]
      end
    end

    test 'basename include tag, time format, and variables' do
      primary = create_primary(
        config_element('buffer', 'time,tag,test1', { 'timekey_zone' => '+0000', 'timekey' => 1 })
      )

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_%Y%m%d%H_${tag}_${test1}
        compress gzip
      ], primary)

      metadata = create_metadata(Time.parse("2011-01-02 13:14:15 UTC"), 'test.tag', { "test1".to_sym => "dummy" })
      c = create_es_chunk(metadata, @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/cool_2011010213_test.tag_dummy.0.gz", path
    end

    test 'directory includes tag, time format, and variables' do
      primary = create_primary(
        config_element('buffer', 'time,tag,test1', { 'timekey_zone' => '+0000', 'timekey' => 1 })
      )

      d = create_driver(%[
        directory #{TMP_DIR}/%Y%m%d%H/${tag}/${test1}
        compress gzip
      ], primary)

      metadata = create_metadata(Time.parse("2011-01-02 13:14:15 UTC"), 'test.tag', { "test1".to_sym => "dummy" })
      c = create_es_chunk(metadata, @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/2011010213/test.tag/dummy/dump.bin.0.gz", path
    end
  end
end
