require_relative '../helper'
require 'time'
require 'fileutils'
require 'fluent/test'
require 'fluent/event'
require 'fluent/plugin/buffer'
require 'fluent/plugin/out_secondary_file'
require 'fluent/plugin/buffer/memory_chunk'

class FileOutputTest < Test::Unit::TestCase
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

  def test_configure
    d = create_driver %[
      path test_path
      compress gz
    ]
    assert_equal 'test_path', d.instance.path
    assert_equal :gz, d.instance.compress
  end

  def test_asts_as_secondary
    c = Fluent::Test::OutputTestDriver.new(Fluent::Plugin::SecondaryFileOutput)
    assert_raise do
      c.configure(conf)
    end
  end

  def test_recieved_path
    assert_raise(Fluent::ConfigError) do
      create_driver %[
        compress gz
      ]
    end
  end

  def test_path_writable
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


  def check_gzipped_result(path, expect)
    # Zlib::GzipReader has a bug of concatenated file: https://bugs.ruby-lang.org/issues/9790
    # Following code from https://www.ruby-forum.com/topic/971591#979520
    result = ''
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
      c.concat(es.to_msgpack_stream, es.size) # standard_format
      c.commit
    end
  end

  RECORD_VALUE = { "key" => "vlaue"}
  MESSAGE_PACKED_RECORD_VALUE = "\x92\xCEM z'\x81\xA3key\xA5vlaue".force_encoding('ASCII-8BIT')

  def test_write
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    es = Fluent::OneEventStream.new(time, RECORD_VALUE)
    c = create_es_chunk(create_metadata, es)
    path = d.instance.write(c)

    assert_equal "#{TMP_DIR}/out_file_test.#{c.unique_id}_0.log.gz", path
    check_gzipped_result(path,  MESSAGE_PACKED_RECORD_VALUE)
  end

  # TODO
  # write temaplte test if needed

  # class TestWithSystem < self
  #   TMP_DIR_WITH_SYSTEM = File.expand_path(File.dirname(__FILE__) + "/../tmp/out_file_system#{ENV['TEST_ENV_NUMBER']}")
  #   # 0750 interprets as "488". "488".to_i(8) # => 4. So, it makes wrong permission. Umm....
  #   OVERRIDE_DIR_PERMISSION = 750
  #   OVERRIDE_FILE_PERMISSION = 0620
  #   CONFIG_WITH_SYSTEM = %[
  #     path #{TMP_DIR_WITH_SYSTEM}/out_file_test
  #     compress gz
  #     utc
  #     <system>
  #       file_permission #{OVERRIDE_FILE_PERMISSION}
  #       dir_permission #{OVERRIDE_DIR_PERMISSION}
  #     </system>
  #   ]

  #   def setup
  #     omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?
  #     FileUtils.rm_rf(TMP_DIR_WITH_SYSTEM)
  #   end

  #   def parse_system(text)
  #     basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
  #     Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
  #   end

  #   def test_write_with_system
  #     system_conf = parse_system(CONFIG_WITH_SYSTEM)
  #     sc = Fluent::SystemConfig.new(system_conf)
  #     Fluent::Engine.init(sc)
  #     d = create_driver CONFIG_WITH_SYSTEM

  #     time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #     d.emit({"a"=>1}, time)
  #     d.emit({"a"=>2}, time)

  #     # FileOutput#write returns path
  #     paths = d.run
  #     expect_paths = ["#{TMP_DIR_WITH_SYSTEM}/out_file_test.20110102_0.log.gz"]
  #     assert_equal expect_paths, paths

  #     check_gzipped_result(paths[0], %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n])
  #     dir_mode = "%o" % File::stat(TMP_DIR_WITH_SYSTEM).mode
  #     assert_equal(OVERRIDE_DIR_PERMISSION, dir_mode[-3, 3].to_i)
  #     file_mode = "%o" % File::stat(paths[0]).mode
  #     assert_equal(OVERRIDE_FILE_PERMISSION, file_mode[-3, 3].to_i)
  #   end
  # end

  # def test_write_with_format_json
  #   d = create_driver [CONFIG, 'format json', 'include_time_key true', 'time_as_epoch'].join("\n")

  #   time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #   d.emit({"a"=>1}, time)
  #   d.emit({"a"=>2}, time)

  #   # FileOutput#write returns path
  #   paths = d.run
  #   check_gzipped_result(paths[0], %[#{Yajl.dump({"a" => 1, 'time' => time})}\n] + %[#{Yajl.dump({"a" => 2, 'time' => time})}\n])
  # end

  # def test_write_with_format_ltsv
  #   d = create_driver [CONFIG, 'format ltsv', 'include_time_key true'].join("\n")

  #   time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #   d.emit({"a"=>1}, time)
  #   d.emit({"a"=>2}, time)

  #   # FileOutput#write returns path
  #   paths = d.run
  #   check_gzipped_result(paths[0], %[a:1\ttime:2011-01-02T13:14:15Z\n] + %[a:2\ttime:2011-01-02T13:14:15Z\n])
  # end

  # def test_write_with_format_single_value
  #   d = create_driver [CONFIG, 'format single_value', 'message_key a'].join("\n")

  #   time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #   d.emit({"a"=>1}, time)
  #   d.emit({"a"=>2}, time)

  #   # FileOutput#write returns path
  #   paths = d.run
  #   check_gzipped_result(paths[0], %[1\n] + %[2\n])
  # end

  # def test_write_path_increment
  #   time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #   formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

  #   write_once = ->(){
  #     d = create_driver
  #     d.emit({"a"=>1}, time)
  #     d.emit({"a"=>2}, time)
  #     d.run
  #   }

  #   assert !File.exist?("#{TMP_DIR}/out_file_test.20110102_0.log.gz")

  #   # FileOutput#write returns path
  #   paths = write_once.call
  #   assert_equal ["#{TMP_DIR}/out_file_test.20110102_0.log.gz"], paths
  #   check_gzipped_result(paths[0], formatted_lines)
  #   assert_equal 1, Dir.glob("#{TMP_DIR}/out_file_test.*").size

  #   paths = write_once.call
  #   assert_equal ["#{TMP_DIR}/out_file_test.20110102_1.log.gz"], paths
  #   check_gzipped_result(paths[0], formatted_lines)
  #   assert_equal 2, Dir.glob("#{TMP_DIR}/out_file_test.*").size

  #   paths = write_once.call
  #   assert_equal ["#{TMP_DIR}/out_file_test.20110102_2.log.gz"], paths
  #   check_gzipped_result(paths[0], formatted_lines)
  #   assert_equal 3, Dir.glob("#{TMP_DIR}/out_file_test.*").size
  # end

  # def test_write_with_append
  #   time = Time.parse("2011-01-02 13:14:15 UTC").to_i
  #   formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

  #   write_once = ->(){
  #     d = create_driver %[
  #       path #{TMP_DIR}/out_file_test
  #       compress gz
  #       utc
  #       append true
  #     ]
  #     d.emit({"a"=>1}, time)
  #     d.emit({"a"=>2}, time)
  #     d.run
  #   }

  #   # FileOutput#write returns path
  #   paths = write_once.call
  #   assert_equal ["#{TMP_DIR}/out_file_test.20110102.log.gz"], paths
  #   check_gzipped_result(paths[0], formatted_lines)
  #   paths = write_once.call
  #   assert_equal ["#{TMP_DIR}/out_file_test.20110102.log.gz"], paths
  #   check_gzipped_result(paths[0], formatted_lines * 2)
  #   paths = write_once.call
  #   assert_equal ["#{TMP_DIR}/out_file_test.20110102.log.gz"], paths
  #   check_gzipped_result(paths[0], formatted_lines * 3)
  # end

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
