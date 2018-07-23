require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_file'
require 'fileutils'
require 'time'
require 'timecop'
require 'zlib'

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
    <buffer>
      timekey_use_utc true
    </buffer>
  ]

  def create_driver(conf = CONFIG, opts = {})
    Fluent::Test::Driver::Output.new(Fluent::Plugin::FileOutput, opts: opts).configure(conf)
  end

  sub_test_case 'configuration' do
    test 'basic configuration' do
      d = create_driver %[
        path test_path
        compress gz
      ]
      assert_equal 'test_path', d.instance.path
      assert_equal :gz, d.instance.compress
      assert_equal :gzip, d.instance.instance_eval{ @compress_method }
    end

    test 'using root_dir for buffer path' do
      system_conf_opts = {'root_dir' => File.join(TMP_DIR, 'testrootdir')}
      buf_conf = config_element('buffer', '', {'flush_interval' => '1s'})
      conf = config_element('match', '**', {'@id' => 'myout', 'path' => 'test_path', 'append' => 'true'}, [buf_conf])
      d = create_driver(conf, system_conf_opts)

      assert_equal 'test_path', d.instance.path
      assert d.instance.append

      assert d.instance.buffer.respond_to?(:path) # file buffer
      assert_equal 1, d.instance.buffer_config.flush_interval

      assert_equal File.join(TMP_DIR, 'testrootdir', 'worker0', 'myout'), d.instance.plugin_root_dir

      buffer_path_under_root_dir = File.join(TMP_DIR, 'testrootdir', 'worker0', 'myout', 'buffer', 'buffer.*.log')
      assert_equal buffer_path_under_root_dir, d.instance.buffer.path
    end

    test 'path should be writable' do
      assert_raise(Fluent::ConfigError.new("'path' parameter is required")) do
        create_driver ""
      end

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

    test 'default timezone is localtime' do
      d = create_driver(%[path #{TMP_DIR}/out_file_test])
      time = event_time("2011-01-02 13:14:15 UTC")

      with_timezone(Fluent.windows? ? 'NST-8' : 'Asia/Taipei') do
        d.run(default_tag: 'test') do
          d.feed(time, {"a"=>1})
        end
      end
      assert_equal 1, d.formatted.size
      assert_equal %[2011-01-02T21:14:15+08:00\ttest\t{"a":1}\n], d.formatted[0]
    end

    test 'no configuration error raised for basic configuration using "*" (v0.12 style)' do
      conf = config_element('match', '**', {
          'path' => "#{TMP_DIR}/test_out.*.log",
          'time_slice_format' => '%Y%m%d',
      })
      assert_nothing_raised do
        create_driver(conf)
      end
    end

    test 'configuration error raised if specified directory via template is not writable' do
      Timecop.freeze(Time.parse("2016-10-04 21:33:27 UTC")) do
        conf = config_element('match', '**', {
            'path' => "#{TMP_DIR}/prohibited/${tag}/file.%Y%m%d.log",
          }, [ config_element('buffer', 'time,tag', {'timekey' => 86400, 'timekey_zone' => '+0000'}) ])
        FileUtils.mkdir_p("#{TMP_DIR}/prohibited")
        File.chmod(0555, "#{TMP_DIR}/prohibited")
        assert_raise Fluent::ConfigError.new("out_file: `#{TMP_DIR}/prohibited/a/file.20161004.log_**.log` is not writable") do
          create_driver(conf)
        end
      end
    end

    test 'configuration using inject/format/buffer sections fully' do
      conf = config_element('match', '**', {
          'path' => "#{TMP_DIR}/${tag}/${type}/conf_test.%Y%m%d.%H%M.log",
          'add_path_suffix' => 'false',
          'append' => "true",
          'symlink_path' => "#{TMP_DIR}/conf_test.current.log",
          'compress' => 'gzip',
          'recompress' => 'true',
        }, [
          config_element('inject', '', {
              'hostname_key' => 'hostname',
              'hostname' => 'testing.local',
              'tag_key' => 'tag',
              'time_key' => 'time',
              'time_type' => 'string',
              'time_format' => '%Y/%m/%d %H:%M:%S %z',
              'timezone' => '+0900',
          }),
          config_element('format', '', {
              '@type' => 'out_file',
              'include_tag' => 'true',
              'include_time' => 'true',
              'delimiter' => 'COMMA',
              'time_type' => 'string',
              'time_format' => '%Y-%m-%d %H:%M:%S %z',
              'utc' => 'true',
          }),
          config_element('buffer', 'time,tag,type', {
              '@type' => 'file',
              'timekey' => '15m',
              'timekey_wait' => '5s',
              'timekey_zone' => '+0000',
              'path' => "#{TMP_DIR}/buf_conf_test",
              'chunk_limit_size' => '50m',
              'total_limit_size' => '1g',
              'compress' => 'gzip',
          }),
      ])
      assert_nothing_raised do
        create_driver(conf)
      end
    end

    test 'configured as secondary with primary using chunk_key_tag and not using chunk_key_time' do
      require 'fluent/plugin/out_null'
      conf = config_element('match', '**', {
        }, [
          config_element('buffer', 'tag', {
          }),
          config_element('secondary', '', {
              '@type' => 'file',
              'path' => "#{TMP_DIR}/testing_to_dump_by_out_file",
          }),
      ])
      assert_nothing_raised do
        Fluent::Test::Driver::Output.new(Fluent::Plugin::NullOutput).configure(conf)
      end
    end
  end

  sub_test_case 'fully configured output' do
    setup do
      Timecop.freeze(Time.parse("2016-10-03 23:58:00 UTC"))
      conf = config_element('match', '**', {
          'path' => "#{TMP_DIR}/${tag}/${type}/full.%Y%m%d.%H%M.log",
          'add_path_suffix' => 'false',
          'append' => "true",
          'symlink_path' => "#{TMP_DIR}/full.current.log",
          'compress' => 'gzip',
          'recompress' => 'true',
        }, [
          config_element('inject', '', {
              'hostname_key' => 'hostname',
              'hostname' => 'testing.local',
              'tag_key' => 'tag',
              'time_key' => 'time',
              'time_type' => 'string',
              'time_format' => '%Y/%m/%d %H:%M:%S %z',
              'timezone' => '+0900',
          }),
          config_element('format', '', {
              '@type' => 'out_file',
              'include_tag' => 'true',
              'include_time' => 'true',
              'delimiter' => 'COMMA',
              'time_type' => 'string',
              'time_format' => '%Y-%m-%d %H:%M:%S %z',
              'utc' => 'true',
          }),
          config_element('buffer', 'time,tag,type', {
              '@type' => 'file',
              'timekey' => '15m',
              'timekey_wait' => '5s',
              'timekey_zone' => '+0000',
              'path' => "#{TMP_DIR}/buf_full",
              'chunk_limit_size' => '50m',
              'total_limit_size' => '1g',
              'compress' => 'gzip',
          }),
      ])
      @d = create_driver(conf)
    end

    teardown do
      FileUtils.rm_rf("#{TMP_DIR}/buf_full")
      FileUtils.rm_rf("#{TMP_DIR}/my.data")
      FileUtils.rm_rf("#{TMP_DIR}/your.data")
      FileUtils.rm_rf("#{TMP_DIR}/full.current.log")
      Timecop.return
    end

    test 'can format/write data correctly' do
      d = @d

      assert_equal 50*1024*1024, d.instance.buffer.chunk_limit_size
      assert_equal 1*1024*1024*1024, d.instance.buffer.total_limit_size

      assert !(File.symlink?("#{TMP_DIR}/full.current.log"))

      t1 = event_time("2016-10-03 23:58:09 UTC")
      t2 = event_time("2016-10-03 23:59:33 UTC")
      t3 = event_time("2016-10-03 23:59:57 UTC")
      t4 = event_time("2016-10-04 00:00:17 UTC")
      t5 = event_time("2016-10-04 00:01:59 UTC")

      Timecop.freeze(Time.parse("2016-10-03 23:58:30 UTC"))

      d.run(start: true, flush: false, shutdown: false) do
        d.feed('my.data', t1, {"type" => "a", "message" => "data raw content"})
        d.feed('my.data', t2, {"type" => "a", "message" => "data raw content"})
        d.feed('your.data', t3, {"type" => "a", "message" => "data raw content"})
      end

      assert_equal 3, d.formatted.size

      assert Dir.exist?("#{TMP_DIR}/buf_full")
      assert !(Dir.exist?("#{TMP_DIR}/my.data/a"))
      assert !(Dir.exist?("#{TMP_DIR}/your.data/a"))
      buffer_files = Dir.entries("#{TMP_DIR}/buf_full").reject{|e| e =~ /^\.+$/ }
      assert_equal 2, buffer_files.select{|n| n.end_with?('.meta') }.size
      assert_equal 2, buffer_files.select{|n| !n.end_with?('.meta') }.size

      m1 = d.instance.metadata('my.data', t1, {"type" => "a"})
      m2 = d.instance.metadata('your.data', t3, {"type" => "a"})

      assert_equal 2, d.instance.buffer.stage.size
      b1_path = d.instance.buffer.stage[m1].path
      b1_size = File.lstat(b1_path).size

      unless Fluent.windows?
        assert File.symlink?("#{TMP_DIR}/full.current.log")
        assert_equal d.instance.buffer.stage[m2].path, File.readlink("#{TMP_DIR}/full.current.log")
      end

      Timecop.freeze(Time.parse("2016-10-04 00:00:06 UTC"))

      d.run(start: false, flush: true, shutdown: true) do
        d.feed('my.data', t4, {"type" => "a", "message" => "data raw content"})
        d.feed('your.data', t5, {"type" => "a", "message" => "data raw content"})
      end

      assert Dir.exist?("#{TMP_DIR}/buf_full")
      assert Dir.exist?("#{TMP_DIR}/my.data/a")
      assert Dir.exist?("#{TMP_DIR}/your.data/a")

      buffer_files = Dir.entries("#{TMP_DIR}/buf_full").reject{|e| e =~ /^\.+$/ }
      assert_equal 0, buffer_files.size

      assert File.exist?("#{TMP_DIR}/my.data/a/full.20161003.2345.log.gz")
      assert File.exist?("#{TMP_DIR}/my.data/a/full.20161004.0000.log.gz")
      assert File.exist?("#{TMP_DIR}/your.data/a/full.20161003.2345.log.gz")
      assert File.exist?("#{TMP_DIR}/your.data/a/full.20161004.0000.log.gz")

      assert{ File.lstat("#{TMP_DIR}/my.data/a/full.20161003.2345.log.gz").size < b1_size } # recompress

      assert_equal 5, d.formatted.size

      r1 = %!2016-10-03 23:58:09 +0000,my.data,{"type":"a","message":"data raw content","hostname":"testing.local","tag":"my.data","time":"2016/10/04 08:58:09 +0900"}\n!
      r2 = %!2016-10-03 23:59:33 +0000,my.data,{"type":"a","message":"data raw content","hostname":"testing.local","tag":"my.data","time":"2016/10/04 08:59:33 +0900"}\n!
      r3 = %!2016-10-03 23:59:57 +0000,your.data,{"type":"a","message":"data raw content","hostname":"testing.local","tag":"your.data","time":"2016/10/04 08:59:57 +0900"}\n!
      r4 = %!2016-10-04 00:00:17 +0000,my.data,{"type":"a","message":"data raw content","hostname":"testing.local","tag":"my.data","time":"2016/10/04 09:00:17 +0900"}\n!
      r5 = %!2016-10-04 00:01:59 +0000,your.data,{"type":"a","message":"data raw content","hostname":"testing.local","tag":"your.data","time":"2016/10/04 09:01:59 +0900"}\n!
      assert_equal r1, d.formatted[0]
      assert_equal r2, d.formatted[1]
      assert_equal r3, d.formatted[2]
      assert_equal r4, d.formatted[3]
      assert_equal r5, d.formatted[4]

      read_gunzip = ->(path){
        File.open(path){ |fio|
          Zlib::GzipReader.new(StringIO.new(fio.read)).read
        }
      }
      assert_equal r1 + r2, read_gunzip.call("#{TMP_DIR}/my.data/a/full.20161003.2345.log.gz")
      assert_equal r3, read_gunzip.call("#{TMP_DIR}/your.data/a/full.20161003.2345.log.gz")
      assert_equal r4, read_gunzip.call("#{TMP_DIR}/my.data/a/full.20161004.0000.log.gz")
      assert_equal r5, read_gunzip.call("#{TMP_DIR}/your.data/a/full.20161004.0000.log.gz")
    end
  end

  sub_test_case 'format' do
    test 'timezone UTC specified' do
      d = create_driver

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end
      assert_equal 2, d.formatted.size
      assert_equal %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n], d.formatted[0]
      assert_equal %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n], d.formatted[1]
    end

    test 'time formatted with specified timezone, using area name' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        timezone Asia/Taipei
      ]

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
      assert_equal 1, d.formatted.size
      assert_equal %[2011-01-02T21:14:15+08:00\ttest\t{"a":1}\n], d.formatted[0]
    end

    test 'time formatted with specified timezone, using offset' do
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        timezone -03:30
      ]

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
      assert_equal 1, d.formatted.size
      assert_equal %[2011-01-02T09:44:15-03:30\ttest\t{"a":1}\n], d.formatted[0]
    end

    test 'configuration error raised for invalid timezone' do
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
        gzr = Zlib::GzipReader.new(StringIO.new(io.read))
        result << gzr.read
        unused = gzr.unused
        gzr.finish
        break if unused.nil?
        io.pos -= unused.length
      end
    }

    assert_equal expect, result
  end

  sub_test_case 'write' do
    test 'basic case' do
      d = create_driver

      assert_false File.exist?("#{TMP_DIR}/out_file_test.20110102_0.log.gz")

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      assert File.exist?("#{TMP_DIR}/out_file_test.20110102_0.log.gz")
      check_gzipped_result("#{TMP_DIR}/out_file_test.20110102_0.log.gz", %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n])
    end
  end

  sub_test_case 'file/directory permissions' do
    TMP_DIR_WITH_SYSTEM = File.expand_path(File.dirname(__FILE__) + "/../tmp/out_file_system#{ENV['TEST_ENV_NUMBER']}")
    # 0750 interprets as "488". "488".to_i(8) # => 4. So, it makes wrong permission. Umm....
    OVERRIDE_DIR_PERMISSION = 750
    OVERRIDE_FILE_PERMISSION = 0620
    CONFIG_WITH_SYSTEM = %[
      path #{TMP_DIR_WITH_SYSTEM}/out_file_test
      compress gz
      utc
      <buffer>
        timekey_use_utc true
      </buffer>
      <system>
        file_permission #{OVERRIDE_FILE_PERMISSION}
        dir_permission #{OVERRIDE_DIR_PERMISSION}
      </system>
    ]

    setup do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?
      FileUtils.rm_rf(TMP_DIR_WITH_SYSTEM)
    end

    def parse_system(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
    end

    test 'write to file with permission specifications' do
      system_conf = parse_system(CONFIG_WITH_SYSTEM)
      sc = Fluent::SystemConfig.new(system_conf)
      Fluent::Engine.init(sc)
      d = create_driver CONFIG_WITH_SYSTEM

      assert_false File.exist?("#{TMP_DIR_WITH_SYSTEM}/out_file_test.20110102_0.log.gz")

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      assert File.exist?("#{TMP_DIR_WITH_SYSTEM}/out_file_test.20110102_0.log.gz")

      check_gzipped_result("#{TMP_DIR_WITH_SYSTEM}/out_file_test.20110102_0.log.gz", %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n])
      dir_mode = "%o" % File::stat(TMP_DIR_WITH_SYSTEM).mode
      assert_equal(OVERRIDE_DIR_PERMISSION, dir_mode[-3, 3].to_i)
      file_mode = "%o" % File::stat("#{TMP_DIR_WITH_SYSTEM}/out_file_test.20110102_0.log.gz").mode
      assert_equal(OVERRIDE_FILE_PERMISSION, file_mode[-3, 3].to_i)
    end
  end

  sub_test_case 'format specified' do
    test 'json' do
      d = create_driver [CONFIG, 'format json', 'include_time_key true', 'time_as_epoch'].join("\n")

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      path = d.instance.last_written_path
      check_gzipped_result(path, %[#{Yajl.dump({"a" => 1, 'time' => time.to_i})}\n] + %[#{Yajl.dump({"a" => 2, 'time' => time.to_i})}\n])
    end

    test 'ltsv' do
      d = create_driver [CONFIG, 'format ltsv', 'include_time_key true'].join("\n")

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      path = d.instance.last_written_path
      check_gzipped_result(path, %[a:1\ttime:2011-01-02T13:14:15Z\n] + %[a:2\ttime:2011-01-02T13:14:15Z\n])
    end

    test 'single_value' do
      d = create_driver [CONFIG, 'format single_value', 'message_key a'].join("\n")

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      end

      path = d.instance.last_written_path
      check_gzipped_result(path, %[1\n] + %[2\n])
    end
  end

  test 'path with index number' do
    time = event_time("2011-01-02 13:14:15 UTC")
    formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    write_once = ->(){
      d = create_driver
      d.run(default_tag: 'test'){
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      }
      d.instance.last_written_path
    }

    assert !File.exist?("#{TMP_DIR}/out_file_test.20110102_0.log.gz")

    path = write_once.call
    assert_equal "#{TMP_DIR}/out_file_test.20110102_0.log.gz", path
    check_gzipped_result(path, formatted_lines)
    assert_equal 1, Dir.glob("#{TMP_DIR}/out_file_test.*").size

    path = write_once.call
    assert_equal "#{TMP_DIR}/out_file_test.20110102_1.log.gz", path
    check_gzipped_result(path, formatted_lines)
    assert_equal 2, Dir.glob("#{TMP_DIR}/out_file_test.*").size

    path = write_once.call
    assert_equal "#{TMP_DIR}/out_file_test.20110102_2.log.gz", path
    check_gzipped_result(path, formatted_lines)
    assert_equal 3, Dir.glob("#{TMP_DIR}/out_file_test.*").size
  end

  test 'append' do
    time = event_time("2011-01-02 13:14:15 UTC")
    formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    write_once = ->(){
      d = create_driver %[
        path #{TMP_DIR}/out_file_test
        compress gz
        utc
        append true
        <buffer>
          timekey_use_utc true
        </buffer>
      ]
      d.run(default_tag: 'test'){
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      }
      d.instance.last_written_path
    }

    path = write_once.call
    assert_equal "#{TMP_DIR}/out_file_test.20110102.log.gz", path
    check_gzipped_result(path, formatted_lines)

    path = write_once.call
    assert_equal "#{TMP_DIR}/out_file_test.20110102.log.gz", path
    check_gzipped_result(path, formatted_lines * 2)

    path = write_once.call
    assert_equal "#{TMP_DIR}/out_file_test.20110102.log.gz", path
    check_gzipped_result(path, formatted_lines * 3)
  end

  test 'append when JST' do
    with_timezone(Fluent.windows? ? "JST-9" : "Asia/Tokyo") do
      time = event_time("2011-01-02 03:14:15+09:00")
      formatted_lines = %[2011-01-02T03:14:15+09:00\ttest\t{"a":1}\n] + %[2011-01-02T03:14:15+09:00\ttest\t{"a":2}\n]

      write_once = ->(){
        d = create_driver %[
          path #{TMP_DIR}/out_file_test
          compress gz
          append true
          <buffer>
            timekey_use_utc false
            timekey_zone Asia/Tokyo
          </buffer>
        ]
        d.run(default_tag: 'test'){
          d.feed(time, {"a"=>1})
          d.feed(time, {"a"=>2})
        }
        d.instance.last_written_path
      }

      path = write_once.call
      assert_equal "#{TMP_DIR}/out_file_test.20110102.log.gz", path
      check_gzipped_result(path, formatted_lines)

      path = write_once.call
      assert_equal "#{TMP_DIR}/out_file_test.20110102.log.gz", path
      check_gzipped_result(path, formatted_lines * 2)

      path = write_once.call
      assert_equal "#{TMP_DIR}/out_file_test.20110102.log.gz", path
      check_gzipped_result(path, formatted_lines * 3)
    end
  end

  test 'append when UTC-02 but timekey_zone is +0900' do
    with_timezone("UTC-02") do # +0200
      time = event_time("2011-01-02 17:14:15+02:00")
      formatted_lines = %[2011-01-02T17:14:15+02:00\ttest\t{"a":1}\n] + %[2011-01-02T17:14:15+02:00\ttest\t{"a":2}\n]

      write_once = ->(){
        d = create_driver %[
          path #{TMP_DIR}/out_file_test
          compress gz
          append true
          <buffer>
            timekey_use_utc false
            timekey_zone +0900
          </buffer>
        ]
        d.run(default_tag: 'test'){
          d.feed(time, {"a"=>1})
          d.feed(time, {"a"=>2})
        }
        d.instance.last_written_path
      }

      path = write_once.call
      # Rotated at 2011-01-02 17:00:00+02:00
      assert_equal "#{TMP_DIR}/out_file_test.20110103.log.gz", path
      check_gzipped_result(path, formatted_lines)

      path = write_once.call
      assert_equal "#{TMP_DIR}/out_file_test.20110103.log.gz", path
      check_gzipped_result(path, formatted_lines * 2)

      path = write_once.call
      assert_equal "#{TMP_DIR}/out_file_test.20110103.log.gz", path
      check_gzipped_result(path, formatted_lines * 3)
    end
  end

  test '${chunk_id}' do
    time = event_time("2011-01-02 13:14:15 UTC")
    formatted_lines = %[2011-01-02T13:14:15Z\ttest\t{"a":1}\n] + %[2011-01-02T13:14:15Z\ttest\t{"a":2}\n]

    write_once = ->(){
      d = create_driver %[
        path #{TMP_DIR}/out_file_chunk_id_${chunk_id}
        utc
        append true
        <buffer>
          timekey_use_utc true
        </buffer>
      ]
      d.run(default_tag: 'test'){
        d.feed(time, {"a"=>1})
        d.feed(time, {"a"=>2})
      }
      d.instance.last_written_path
    }

    path = write_once.call
    if File.basename(path) =~ /out_file_chunk_id_([-_.@a-zA-Z0-9].*).20110102.log/
      unique_id = Fluent::UniqueId.hex(Fluent::UniqueId.generate)
      assert_equal unique_id.size, $1.size, "chunk_id size is mismatched"
    else
      flunk "chunk_id is not included in the path"
    end
  end

  test 'symlink' do
    omit "Windows doesn't support symlink" if Fluent.windows?
    conf = CONFIG + %[
      symlink_path #{SYMLINK_PATH}
    ]
    symlink_path = "#{SYMLINK_PATH}"

    d = create_driver(conf)
    begin
      d.run(default_tag: 'tag') do
        es = Fluent::OneEventStream.new(event_time("2011-01-02 13:14:15 UTC"), {"a"=>1})
        d.feed(es)

        assert File.symlink?(symlink_path)
        assert File.exist?(symlink_path) # This checks dest of symlink exists or not.

        es = Fluent::OneEventStream.new(event_time("2011-01-03 14:15:16 UTC"), {"a"=>2})
        d.feed(es)

        assert File.symlink?(symlink_path)
        assert File.exist?(symlink_path)

        meta = d.instance.metadata('tag', event_time("2011-01-03 14:15:16 UTC"), {})
        assert_equal d.instance.buffer.instance_eval{ @stage[meta].path }, File.readlink(symlink_path)
      end
    ensure
      FileUtils.rm_rf(symlink_path)
    end
  end

  sub_test_case 'path' do
    test 'normal' do
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test
        time_slice_format %Y-%m-%d-%H
        utc true
      ])
      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
      path = d.instance.last_written_path
      assert_equal "#{TMP_DIR}/out_file_test.2011-01-02-13_0.log", path
    end

    test 'normal with append' do
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test
        time_slice_format %Y-%m-%d-%H
        utc true
        append true
      ])
      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
      path = d.instance.last_written_path
      assert_equal "#{TMP_DIR}/out_file_test.2011-01-02-13.log", path
     end

     test '*' do
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test.*.txt
        time_slice_format %Y-%m-%d-%H
        utc true
      ])
      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
      path = d.instance.last_written_path
      assert_equal "#{TMP_DIR}/out_file_test.2011-01-02-13_0.txt", path
    end

    test '* with append' do
      d = create_driver(%[
        path #{TMP_DIR}/out_file_test.*.txt
        time_slice_format %Y-%m-%d-%H
        utc true
        append true
      ])
      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {"a"=>1})
      end
      path = d.instance.last_written_path
      assert_equal "#{TMP_DIR}/out_file_test.2011-01-02-13.txt", path
    end
  end

  sub_test_case '#timekey_to_timeformat' do
    setup do
      @d = create_driver
      @i = @d.instance
    end

    test 'returns empty string for nil' do
      assert_equal '', @i.timekey_to_timeformat(nil)
    end

    test 'returns timestamp string with seconds for timekey smaller than 60' do
      assert_equal '%Y%m%d%H%M%S', @i.timekey_to_timeformat(1)
      assert_equal '%Y%m%d%H%M%S', @i.timekey_to_timeformat(30)
      assert_equal '%Y%m%d%H%M%S', @i.timekey_to_timeformat(59)
    end

    test 'returns timestamp string with minutes for timekey smaller than 3600' do
      assert_equal '%Y%m%d%H%M', @i.timekey_to_timeformat(60)
      assert_equal '%Y%m%d%H%M', @i.timekey_to_timeformat(180)
      assert_equal '%Y%m%d%H%M', @i.timekey_to_timeformat(1800)
      assert_equal '%Y%m%d%H%M', @i.timekey_to_timeformat(3599)
    end

    test 'returns timestamp string with hours for timekey smaller than 86400 (1 day)' do
      assert_equal '%Y%m%d%H', @i.timekey_to_timeformat(3600)
      assert_equal '%Y%m%d%H', @i.timekey_to_timeformat(7200)
      assert_equal '%Y%m%d%H', @i.timekey_to_timeformat(86399)
    end

    test 'returns timestamp string with days for timekey equal or greater than 86400' do
      assert_equal '%Y%m%d', @i.timekey_to_timeformat(86400)
      assert_equal '%Y%m%d', @i.timekey_to_timeformat(1000000)
      assert_equal '%Y%m%d', @i.timekey_to_timeformat(1000000000)
    end
  end

  sub_test_case '#compression_suffix' do
    setup do
      @i = create_driver.instance
    end

    test 'returns empty string for nil (no compression method specified)' do
      assert_equal '', @i.compression_suffix(nil)
    end

    test 'returns .gz for gzip' do
      assert_equal '.gz', @i.compression_suffix(:gzip)
    end
  end

  sub_test_case '#generate_path_template' do
    setup do
      @i = create_driver.instance
    end

    data(
      'day' => [86400, '%Y%m%d', '%Y-%m-%d'],
      'hour' => [3600, '%Y%m%d%H', '%Y-%m-%d_%H'],
      'minute' => [60, '%Y%m%d%H%M', '%Y-%m-%d_%H%M'],
    )
    test 'generates path with timestamp placeholder for original path with tailing star with timekey' do |data|
      timekey, placeholder, time_slice_format = data
      # with index placeholder, without compression suffix when append disabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}_**",    @i.generate_path_template('/path/to/file.*', timekey, false, nil)
      # with index placeholder, with .gz suffix when append disabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}_**.gz", @i.generate_path_template('/path/to/file.*', timekey, false, :gzip)
      # without index placeholder, without compression suffix when append enabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}",       @i.generate_path_template('/path/to/file.*', timekey, true, nil)
      # without index placeholder, with .gz suffix when append disabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}.gz",    @i.generate_path_template('/path/to/file.*', timekey, true, :gzip)

      # time_slice_format will used instead of computed placeholder if specified
      assert_equal "/path/to/file.#{time_slice_format}_**",    @i.generate_path_template('/path/to/file.*', timekey, false, nil, time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}_**.gz", @i.generate_path_template('/path/to/file.*', timekey, false, :gzip, time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}",       @i.generate_path_template('/path/to/file.*', timekey, true, nil, time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}.gz",    @i.generate_path_template('/path/to/file.*', timekey, true, :gzip, time_slice_format: time_slice_format)
    end

    data(
      'day' => [86400 * 2, '%Y%m%d', '%Y-%m-%d'],
      'hour' => [7200, '%Y%m%d%H', '%Y-%m-%d_%H'],
      'minute' => [180, '%Y%m%d%H%M', '%Y-%m-%d_%H%M'],
    )
    test 'generates path with timestamp placeholder for original path with star and suffix with timekey' do |data|
      timekey, placeholder, time_slice_format = data
      # with index placeholder, without compression suffix when append disabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}_**.data",    @i.generate_path_template('/path/to/file.*.data', timekey, false, nil)
      # with index placeholder, with .gz suffix when append disabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}_**.data.gz", @i.generate_path_template('/path/to/file.*.data', timekey, false, :gzip)
      # without index placeholder, without compression suffix when append enabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}.data",       @i.generate_path_template('/path/to/file.*.data', timekey, true, nil)
      # without index placeholder, with .gz suffix when append disabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}.data.gz",    @i.generate_path_template('/path/to/file.*.data', timekey, true, :gzip)

      # time_slice_format will used instead of computed placeholder if specified
      assert_equal "/path/to/file.#{time_slice_format}_**.data",    @i.generate_path_template('/path/to/file.*.data', timekey, false, nil, time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}_**.data.gz", @i.generate_path_template('/path/to/file.*.data', timekey, false, :gzip, time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}.data",       @i.generate_path_template('/path/to/file.*.data', timekey, true, nil, time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}.data.gz",    @i.generate_path_template('/path/to/file.*.data', timekey, true, :gzip, time_slice_format: time_slice_format)
    end

    test 'raise error to show it is a bug when path including * specified without timekey' do
      assert_raise RuntimeError.new("BUG: configuration error must be raised for path including '*' without timekey") do
        @i.generate_path_template('/path/to/file.*.log', nil, false, nil)
      end
    end

    data(
      'day' => [86400 * 7, '%Y%m%d', '%Y-%m-%d'],
      'hour' => [3600 * 6, '%Y%m%d%H', '%Y-%m-%d_%H'],
      'minute' => [60 * 15, '%Y%m%d%H%M', '%Y-%m-%d_%H%M'],
    )
    test 'generates path with timestamp placeholder for original path without time placeholders & star with timekey, and path_suffix configured' do |data|
      timekey, placeholder, time_slice_format = data
      # with index placeholder, without compression suffix when append disabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}_**.log",    @i.generate_path_template('/path/to/file', timekey, false, nil, path_suffix: '.log')
      # with index placeholder, with .gz suffix when append disabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}_**.log.gz", @i.generate_path_template('/path/to/file', timekey, false, :gzip, path_suffix: '.log')
      # without index placeholder, without compression suffix when append enabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}.log",       @i.generate_path_template('/path/to/file', timekey, true, nil, path_suffix: '.log')
      # without index placeholder, with compression suffix when append enabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}.log.gz",    @i.generate_path_template('/path/to/file', timekey, true, :gzip, path_suffix: '.log')

      # time_slice_format will be appended always if it's specified
      assert_equal "/path/to/file.#{time_slice_format}_**.log",    @i.generate_path_template('/path/to/file', timekey, false, nil, path_suffix: '.log', time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}_**.log.gz", @i.generate_path_template('/path/to/file', timekey, false, :gzip, path_suffix: '.log', time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}.log",       @i.generate_path_template('/path/to/file', timekey, true, nil, path_suffix: '.log', time_slice_format: time_slice_format)
      assert_equal "/path/to/file.#{time_slice_format}.log.gz",    @i.generate_path_template('/path/to/file', timekey, true, :gzip, path_suffix: '.log', time_slice_format: time_slice_format)
    end

    data(
      'day' => [86400, '%Y%m%d'],
      'hour' => [3600, '%Y%m%d%H'],
      'minute' => [60, '%Y%m%d%H%M'],
    )
    test 'generates path with timestamp placeholder for original path without star with timekey, and path_suffix not configured' do |data|
      timekey, placeholder = data
      # with index placeholder, without compression suffix when append disabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}_**",    @i.generate_path_template('/path/to/file', timekey, false, nil)
      # with index placeholder, with .gz suffix when append disabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}_**.gz", @i.generate_path_template('/path/to/file', timekey, false, :gzip)
      # without index placeholder, without compression suffix when append enabled and compression disabled
      assert_equal "/path/to/file.#{placeholder}",       @i.generate_path_template('/path/to/file', timekey, true, nil)
      # without index placeholder, with compression suffix when append enabled and gzip compression enabled
      assert_equal "/path/to/file.#{placeholder}.gz",    @i.generate_path_template('/path/to/file', timekey, true, :gzip)
    end

    test 'generates path without adding timestamp placeholder part if original path has enough placeholders for specified timekey' do
      assert_equal "/path/to/file.%Y%m%d", @i.generate_path_template('/path/to/file.%Y%m%d', 86400, true, nil)
      assert_equal "/path/to/%Y%m%d/file", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, true, nil)

      assert_equal "/path/to/%Y%m%d/file_**", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, false, nil)

      assert_raise Fluent::ConfigError.new("insufficient timestamp placeholders in path") do
        @i.generate_path_template('/path/to/%Y%m/file', 86400, true, nil)
      end
      assert_raise Fluent::ConfigError.new("insufficient timestamp placeholders in path") do
        @i.generate_path_template('/path/to/file.%Y%m%d.log', 3600, true, nil)
      end

      assert_equal "/path/to/file.%Y%m%d_%H_**.log.gz", @i.generate_path_template('/path/to/file.%Y%m%d_%H', 7200, false, :gzip, path_suffix: '.log')
      assert_equal "/path/to/${tag}/file.%Y%m%d_%H_**.log.gz", @i.generate_path_template('/path/to/${tag}/file.%Y%m%d_%H', 7200, false, :gzip, path_suffix: '.log')
    end

    test 'generates path with specified time_slice_format appended even if path has sufficient timestamp placeholders' do
      assert_equal "/path/to/%Y%m%d/file.%Y-%m-%d_%H_**", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, false, nil, time_slice_format: '%Y-%m-%d_%H')
      assert_equal "/path/to/%Y%m%d/file.%Y-%m-%d_%H", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, true, nil, time_slice_format: '%Y-%m-%d_%H')
      assert_equal "/path/to/%Y%m%d/file.%Y-%m-%d_%H_**.log", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, false, nil, time_slice_format: '%Y-%m-%d_%H', path_suffix: '.log')
      assert_equal "/path/to/%Y%m%d/file.%Y-%m-%d_%H.log", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, true, nil, time_slice_format: '%Y-%m-%d_%H', path_suffix: '.log')
      assert_equal "/path/to/%Y%m%d/file.%Y-%m-%d_%H.log.gz", @i.generate_path_template('/path/to/%Y%m%d/file', 86400, true, :gzip, time_slice_format: '%Y-%m-%d_%H', path_suffix: '.log')
    end

    test 'generates path without timestamp placeholder when path does not include * and timekey not specified' do
      assert_equal '/path/to/file.log', @i.generate_path_template('/path/to/file.log', nil, true, nil)
      assert_equal '/path/to/file.log_**', @i.generate_path_template('/path/to/file.log', nil, false, nil)
      assert_equal '/path/to/file.${tag}.log_**', @i.generate_path_template('/path/to/file.${tag}.log', nil, false, nil)
      assert_equal '/path/to/file.${tag}_**.log', @i.generate_path_template('/path/to/file.${tag}', nil, false, nil, path_suffix: '.log')
    end
  end

  sub_test_case '#find_filepath_available' do
    setup do
      @tmp = File.join(TMP_DIR, 'find_filepath_test')
      FileUtils.mkdir_p @tmp
      @i = create_driver.instance
    end

    teardown do
      FileUtils.rm_rf @tmp
    end

    test 'raise error if argument path does not include index placeholder' do
      assert_raise RuntimeError.new("BUG: index placeholder not found in path: #{@tmp}/myfile") do
        @i.find_filepath_available("#{@tmp}/myfile") do |path|
          # ...
        end
      end
    end

    data(
      'without suffix' => ['myfile_0', 'myfile_**'],
      'with timestamp' => ['myfile_20161003_0', 'myfile_20161003_**'],
      'with base suffix' => ['myfile_0.log', 'myfile_**.log'],
      'with compression suffix' => ['myfile_0.log.gz', 'myfile_**.log.gz'],
    )
    test 'returns filepath with _0 at first' do |data|
      expected, argument = data
      @i.find_filepath_available(File.join(@tmp, argument)) do |path|
        assert_equal File.join(@tmp, expected), path
      end
    end

    test 'returns filepath with index which does not exist yet' do
      5.times do |i|
        File.open(File.join(@tmp, "exist_#{i}.log"), 'a'){|f| } # open(create) and close
      end
      @i.find_filepath_available(File.join(@tmp, "exist_**.log")) do |path|
        assert_equal File.join(@tmp, "exist_5.log"), path
      end
    end

    test 'creates lock directory when with_lock is true to exclude operations of other worker process' do
      5.times do |i|
        File.open(File.join(@tmp, "exist_#{i}.log"), 'a')
      end
      Dir.mkdir(File.join(@tmp, "exist_5.log.lock"))
      @i.find_filepath_available(File.join(@tmp, "exist_**.log"), with_lock: true) do |path|
        assert Dir.exist?(File.join(@tmp, "exist_6.log.lock"))
        assert_equal File.join(@tmp, "exist_6.log"), path
      end
    end
  end
end
