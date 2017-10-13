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
      assert_equal false, d.instance.append
    end

    test 'should be configurable' do
      d = create_driver %[
         directory #{TMP_DIR}
         basename out_file_test
         compress gzip
         append true
      ]
      assert_equal 'out_file_test', d.instance.basename
      assert_equal TMP_DIR, d.instance.directory
      assert_equal :gzip, d.instance.compress
      assert_equal true, d.instance.append
    end

    test 'should only use in secondary' do
      c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
      assert_raise Fluent::ConfigError.new("This plugin can only be used in the <secondary> section") do
        c.configure(CONFIG)
      end
    end

    test 'basename should not include `/`' do
      assert_raise Fluent::ConfigError.new("basename should not include `/`") do
        create_driver %[
          directory #{TMP_DIR}
          basename out/file
        ]
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

      assert_raise Fluent::ConfigError.new("out_secondary_file: `#{TMP_DIR}/test_dir/foo/bar/` should be writable") do
        FileUtils.mkdir_p("#{TMP_DIR}/test_dir")
        File.chmod(0555, "#{TMP_DIR}/test_dir")
        create_driver %[directory #{TMP_DIR}/test_dir/foo/bar/]
      end
    end

    test 'should be passed directory' do
      assert_raise Fluent::ConfigError do
        i = Fluent::Plugin::SecondaryFileOutput.new
        i.acts_as_secondary(create_primary)
        i.configure(config_element())
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
    waiting(10) do
      # we can expect that GzipReader#read can wait unflushed raw data of `io` on disk
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
    end

    assert_equal expect, result
  end

  def create_chunk(primary, metadata, es)
    primary.buffer.generate_chunk(metadata).tap do |c|
      c.concat(es.to_msgpack_stream, es.size) # to_msgpack_stream is standard_format
      c.commit
    end
  end

  sub_test_case 'write' do
    setup do
      @record = { 'key' => 'value' }
      @time = event_time
      @es = Fluent::OneEventStream.new(@time, @record)
      @primary = create_primary
      metadata = @primary.buffer.new_metadata
      @chunk = create_chunk(@primary, metadata, @es)
    end

    test 'should output compressed file when compress option is gzip' do
      d = create_driver(CONFIG, @primary)
      path = d.instance.write(@chunk)

      assert_equal "#{TMP_DIR}/out_file_test.0.gz", path
      check_gzipped_result(path, @es.to_msgpack_stream.force_encoding('ASCII-8BIT'))
    end

    test 'should output plain text when compress option is default(text)' do
      d = create_driver(%[
        directory #{TMP_DIR}/
        basename out_file_test
      ], @primary)

      msgpack_binary = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      path = d.instance.write(@chunk)
      assert_equal "#{TMP_DIR}/out_file_test.0", path
      waiting(5) do
        sleep 0.1 until File.stat(path).size == msgpack_binary.size
      end

      assert_equal msgpack_binary, File.binread(path)
    end

    test 'path should be incremental when append option is false' do
      d = create_driver(CONFIG, @primary)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      5.times do |i|
        path = d.instance.write(@chunk)
        assert_equal "#{TMP_DIR}/out_file_test.#{i}.gz", path
        check_gzipped_result(path, packed_value)
      end
    end

    test 'path should be unchanged when append option is true' do
      d = create_driver(CONFIG + %[append true], @primary)
      packed_value = @es.to_msgpack_stream.force_encoding('ASCII-8BIT')

      [*1..5].each do |i|
        path = d.instance.write(@chunk)
        assert_equal "#{TMP_DIR}/out_file_test.gz", path
        check_gzipped_result(path, packed_value * i)
      end
    end
  end

  sub_test_case 'Syntax of placeholders' do
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
      variable7: '${tags}',
      variable8: '${tag${key}', # matched ${key}
    )
    test 'matches with a valid placeholder' do |path|
      assert Fluent::Plugin::SecondaryFileOutput::PLACEHOLDER_REGEX.match(path)
    end

    data(
      invalid_tag: 'tag',
      invalid_tag2: '{tag}',
      invalid_tag3: '${tag',
      invalid_tag4: '${tag0]}',
      invalid_tag5: '${tag[]]}',
      invalid_variable: '${key[0]}',
      invalid_variable2: '${key{key2}}',
    )
    test "doesn't match with an invalid placeholder" do |path|
      assert !Fluent::Plugin::SecondaryFileOutput::PLACEHOLDER_REGEX.match(path)
    end
  end

  sub_test_case 'path' do
    setup do
      @record = { 'key' => 'value' }
      @time = event_time
      @es = Fluent::OneEventStream.new(@time, @record)
      primary = create_primary
      m = primary.buffer.new_metadata
      @c = create_chunk(primary, m, @es)
    end

    test 'normal path when compress option is gzip' do
      d = create_driver
      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/out_file_test.0.gz", path
    end

    test 'normal path when compress option is default' do
      d = create_driver %[
        directory #{TMP_DIR}
        basename out_file_test
      ]
      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/out_file_test.0", path
    end

    test 'normal path when append option is true' do
      d = create_driver %[
        directory #{TMP_DIR}
        append true
      ]
      path = d.instance.write(@c)
      assert_equal "#{TMP_DIR}/dump.bin", path
    end

    test 'path with ${chunk_id}' do
      d = create_driver %[
        directory #{TMP_DIR}
        basename out_file_chunk_id_${chunk_id}
      ]
      path = d.instance.write(@c)
      if File.basename(path) =~ /out_file_chunk_id_([-_.@a-zA-Z0-9].*).0/
        unique_id = Fluent::UniqueId.hex(Fluent::UniqueId.generate)
        assert_equal unique_id.size, $1.size, "chunk_id size is mismatched"
      else
        flunk "chunk_id is not included in the path"
      end
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

      m = primary.buffer.new_metadata(tag: 'test.dummy')
      c = create_chunk(primary, m, @es)

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

      m = primary.buffer.new_metadata(tag: 'test.dummy')
      c = create_chunk(primary, m, @es)

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

      m = primary.buffer.new_metadata(timekey: event_time("2011-01-02 13:14:15 UTC"))
      c = create_chunk(primary, m, @es)

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

      m = primary.buffer.new_metadata(timekey: event_time("2011-01-02 13:14:15 UTC"))
      c = create_chunk(primary, m, @es)

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

      m = primary.buffer.new_metadata(variables: { "test1".to_sym => "dummy" })
      c = create_chunk(primary, m, @es)

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

    test 'basename includes tag, time format, and variables' do
      primary = create_primary(
        config_element('buffer', 'time,tag,test1', { 'timekey_zone' => '+0000', 'timekey' => 1 })
      )

      d = create_driver(%[
        directory #{TMP_DIR}/
        basename cool_%Y%m%d%H_${tag}_${test1}
        compress gzip
      ], primary)

      m = primary.buffer.new_metadata(
        timekey: event_time("2011-01-02 13:14:15 UTC"),
        tag: 'test.tag',
        variables: { "test1".to_sym => "dummy" }
      )

      c = create_chunk(primary, m, @es)

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

      m = primary.buffer.new_metadata(
        timekey: event_time("2011-01-02 13:14:15 UTC"),
        tag: 'test.tag',
        variables: { "test1".to_sym => "dummy" }
      )
      c = create_chunk(primary, m, @es)

      path = d.instance.write(c)
      assert_equal "#{TMP_DIR}/2011010213/test.tag/dummy/dump.bin.0.gz", path
    end
  end
end
