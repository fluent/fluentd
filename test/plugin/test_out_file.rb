require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_file'
require 'fileutils'
require 'time'

class FileOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/out_file#{ENV['TEST_ENV_NUMBER']}")
  SYMLINK_PATH = File.expand_path("#{TMP_DIR}/current")

  CONFIG = %[
    path #{TMP_DIR}/out_file_test
    compress gz
    utc
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::FileOutput).configure(conf)
  end

  def test_configure
    d = create_driver %[
      path test_path
      compress gz
    ]
    assert_equal 'test_path', d.instance.path
    assert_equal :gz, d.instance.compress
  end

  sub_test_case "path writable" do
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
  end

  def test_default_localtime
    d = create_driver(%[path #{TMP_DIR}/out_file_test])
    time = event_time("2011-01-02 13:14:15 UTC")

    with_timezone(Fluent.windows? ? 'NST-8' : 'Asia/Taipei') do
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
      end
      assert_equal([%[2011-01-02T21:14:15+08:00\ttest\t{"a":1}\n]], d.formatted)
    end
  end

  def test_format
    d = create_driver

    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: "test") do
      d.feed(time, {"a"=>1})
      d.feed(time, {"a"=>2})
    end

    expected = [
      %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n],
      %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]
    ]
    assert_equal(expected, d.formatted)
  end

  sub_test_case "timezone" do
    data("Asia/Taipei" => ["Asia/Taipei", "2011-01-02T21:14:15+08:00"],
         "-03:30" => ["-03:30", "2011-01-02T09:44:15-03:30"])
    def test_flat(data)
      timezone, expected_time = data
      conf = config_element(
        "ROOT", "",
        { "path" => "#{TMP_DIR}/out_file_test", "timezone" => timezone })
      d = create_driver(conf)

      time = event_time("2011-01-02 13:14:15 UTC")

      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
      end
      assert_equal([%[#{expected_time}\ttest\t{"a":1}\n]], d.formatted)
    end

    def test_invalid
      assert_raise(Fluent::ConfigError) do
        create_driver %[
          path #{TMP_DIR}/out_file_test
          timezone Invalid/Invalid
        ]
      end
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

  def test_write
    time = event_time("2011-01-02 13:14:15 UTC")
    mock(Time).now.at_least(2) { Time.at(time.to_r) }

    d = create_driver

    d.run(default_tag: "test") do
      d.feed(time, {"a"=>1})
      d.feed(time, {"a"=>2})
    end

    paths = d.instance._paths
    expect_paths = ["#{TMP_DIR}/out_file_test.20110102_0.log.gz"]
    assert_equal expect_paths, paths

    check_gzipped_result(paths[0], %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n])
  end

  class TestWithSystem < self
    TMP_DIR_WITH_SYSTEM = File.expand_path(File.dirname(__FILE__) + "/../tmp/out_file_system#{ENV['TEST_ENV_NUMBER']}")
    # 0750 interprets as "488". "488".to_i(8) # => 4. So, it makes wrong permission. Umm....
    OVERRIDE_DIR_PERMISSION = 750
    OVERRIDE_FILE_PERMISSION = 0620
    CONFIG_WITH_SYSTEM = %[
      path #{TMP_DIR_WITH_SYSTEM}/out_file_test
      compress gz
      utc
      <system>
        file_permission #{OVERRIDE_FILE_PERMISSION}
        dir_permission #{OVERRIDE_DIR_PERMISSION}
      </system>
    ]

    def setup
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?
      FileUtils.rm_rf(TMP_DIR_WITH_SYSTEM)
    end

    def parse_system(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
    end

    def test_write_with_system
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      system_conf = parse_system(CONFIG_WITH_SYSTEM)
      sc = Fluent::SystemConfig.new(system_conf)
      Fluent::Engine.init(sc)

      d = create_driver CONFIG_WITH_SYSTEM

      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      paths = d.instance._paths
      expect_paths = ["#{TMP_DIR_WITH_SYSTEM}/out_file_test.20110102_0.log.gz"]
      assert_equal expect_paths, paths

      check_gzipped_result(paths[0], %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n])
      dir_mode = "%o" % File::stat(TMP_DIR_WITH_SYSTEM).mode
      assert_equal(OVERRIDE_DIR_PERMISSION, dir_mode[-3, 3].to_i)
      file_mode = "%o" % File::stat(paths[0]).mode
      assert_equal(OVERRIDE_FILE_PERMISSION, file_mode[-3, 3].to_i)
    end
  end

  sub_test_case "format" do
    def test_write_with_format_json
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver [CONFIG, 'format json', 'include_time_key true', 'time_as_epoch true'].join("\n")

      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      paths = d.instance._paths
      check_gzipped_result(paths[0], %[#{Yajl.dump({"a" => 1, 'time' => time})}\n] + %[#{Yajl.dump({"a" => 2, 'time' => time})}\n])
    end

    def test_write_with_format_ltsv
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver [CONFIG, 'format ltsv', 'include_time_key true'].join("\n")

      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      paths = d.instance._paths
      check_gzipped_result(paths[0], %[a:1\ttime:2011-01-02T13:14:15Z\n] + %[a:2\ttime:2011-01-02T13:14:15Z\n])
    end

    def test_write_with_format_single_value
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver [CONFIG, 'format single_value', 'message_key a'].join("\n")

      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      paths = d.instance._paths
      check_gzipped_result(paths[0], %[1\n] + %[2\n])
    end
  end

  def test_write_path_increment
    time = event_time("2011-01-02 13:14:15 UTC")
    mock(Time).now.at_least(2) { Time.at(time.to_r) }
    formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    write_once = ->(){
      d = create_driver
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end
      d.instance._paths
    }

    assert !File.exist?("#{TMP_DIR}/out_file_test.20110102_0.log.gz")

    paths = write_once.call
    assert_equal ["#{TMP_DIR}/out_file_test.20110102_0.log.gz"], paths
    check_gzipped_result(paths[0], formatted_lines)
    assert_equal 1, Dir.glob("#{TMP_DIR}/out_file_test.*").size

    paths = write_once.call
    assert_equal ["#{TMP_DIR}/out_file_test.20110102_1.log.gz"], paths
    check_gzipped_result(paths[0], formatted_lines)
    assert_equal 2, Dir.glob("#{TMP_DIR}/out_file_test.*").size

    paths = write_once.call
    assert_equal ["#{TMP_DIR}/out_file_test.20110102_2.log.gz"], paths
    check_gzipped_result(paths[0], formatted_lines)
    assert_equal 3, Dir.glob("#{TMP_DIR}/out_file_test.*").size
  end

  def test_write_with_append
    time = event_time("2011-01-02 13:14:15 UTC")
    mock(Time).now.at_least(2) { Time.at(time.to_r) }
    formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    write_once = ->(){
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        compress gz
        utc
        append true
      ]
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end
      d.instance._paths
    }

    paths = write_once.call
    assert_equal ["#{TMP_DIR}/out_file_test.20110102.log.gz"], paths
    check_gzipped_result(paths[0], formatted_lines)
    paths = write_once.call
    assert_equal ["#{TMP_DIR}/out_file_test.20110102.log.gz"], paths
    check_gzipped_result(paths[0], formatted_lines * 2)
    paths = write_once.call
    assert_equal ["#{TMP_DIR}/out_file_test.20110102.log.gz"], paths
    check_gzipped_result(paths[0], formatted_lines * 3)
  end

  def test_write_with_symlink
    omit "Windows doesn't support symlink" if Fluent.windows?
    conf = CONFIG + %[
      symlink_path #{SYMLINK_PATH}
    ]
    symlink_path = SYMLINK_PATH

    d = create_driver(conf)

    d.run(default_tag: "tag") do
      es = Fluent::OneEventStream.new(event_time("2011-01-02 13:14:15 UTC"), {"a"=>1})
      d.feed(es)
      sleep(0.1) until File.exist?(symlink_path)
      assert(File.symlink?(symlink_path))

      es = Fluent::OneEventStream.new(event_time("2011-01-03 14:15:16 UTC"), {"a"=>2})
      d.feed(es)
      sleep(0.1) until File.exist?(symlink_path)
      assert(File.symlink?(symlink_path))

      meta = d.instance.metadata("tag", event_time("2011-01-03 14:15:16 UTC"), {})
      assert_equal(d.instance.buffer.instance_eval { @stage[meta].path }, File.readlink(symlink_path))
    end
  ensure
    FileUtils.rm_rf(symlink_path)
  end

  sub_test_case 'path' do
    test 'normal' do
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test
        time_slice_format %Y-%m-%d-%H
        utc true
      ])
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
      end
      paths = d.instance._paths
      assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13_0.log"], paths
    end

    test 'normal with append' do
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test
        time_slice_format %Y-%m-%d-%H
        utc true
        append true
      ])
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
      end
      paths = d.instance._paths
      assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13.log"], paths
    end

    test '*' do
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test.*.txt
        time_slice_format %Y-%m-%d-%H
        utc true
      ])
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
      end
      paths = d.instance._paths
      assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13_0.txt"], paths
    end

    test '* with append' do
      time = event_time("2011-01-02 13:14:15 UTC")
      mock(Time).now.at_least(2) { Time.at(time.to_r) }
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test.*.txt
        time_slice_format %Y-%m-%d-%H
        utc true
        append true
      ])
      d.run(default_tag: "test") do
        d.feed(time, {"a"=>1})
      end
      paths = d.instance._paths
      assert_equal ["#{TMP_DIR}/out_file_test.2011-01-02-13.txt"], paths
    end
  end
end

