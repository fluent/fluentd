require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_tail'
require 'fluent/plugin/buffer'
require 'fluent/system_config'
require 'net/http'
require 'flexmock/test_unit'
require 'timecop'
require 'tmpdir'
require 'securerandom'

class TailInputTest < Test::Unit::TestCase
  include FlexMock::TestCase

  def setup
    Fluent::Test.setup
    cleanup_directory(TMP_DIR)
  end

  def teardown
    super
    cleanup_directory(TMP_DIR)
    Fluent::Engine.stop
  end

  def cleanup_directory(path)
    unless Dir.exist?(path)
      FileUtils.mkdir_p(path)
      return
    end

    if Fluent.windows?
      Dir.glob("*", base: path).each do |name|
        begin
          cleanup_file(File.join(path, name))
        rescue
          # expect test driver block release already owned file handle.
        end
      end
    else
      begin
        FileUtils.rm_f(path, secure:true)
      rescue ArgumentError
        FileUtils.rm_f(path) # For Ruby 2.6 or before.
      end
      if File.exist?(path)
        FileUtils.remove_entry_secure(path, true)
      end
    end
    FileUtils.mkdir_p(path)
  end

  def cleanup_file(path)
    if Fluent.windows?
      # On Windows, when the file or directory is removed and created
      # frequently, there is a case that creating file or directory will
      # fail. This situation is caused by pending file or directory
      # deletion which is mentioned on win32 API document [1]
      # As a workaround, execute rename and remove method.
      #
      # [1] https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea#files
      #
      file = File.join(Dir.tmpdir, SecureRandom.hex(10))
      begin
        FileUtils.mv(path, file)
        FileUtils.rm_rf(file, secure: true)
      rescue ArgumentError
        FileUtils.rm_rf(file) # For Ruby 2.6 or before.
      end
      if File.exist?(file)
        # ensure files are closed for Windows, on which deleted files
        # are still visible from filesystem
        GC.start(full_mark: true, immediate_mark: true, immediate_sweep: true)
        FileUtils.remove_entry_secure(file, true)
      end
    else
      begin
        FileUtils.rm_f(path, secure: true)
      rescue ArgumentError
        FileUtils.rm_f(path) # For Ruby 2.6 or before.
      end
      if File.exist?(path)
        FileUtils.remove_entry_secure(path, true)
      end
    end
  end

  def create_target_info(path)
    Fluent::Plugin::TailInput::TargetInfo.new(path, Fluent::FileWrapper.stat(path).ino)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail#{ENV['TEST_ENV_NUMBER']}"

  CONFIG = config_element("ROOT", "", {
                            "path" => "#{TMP_DIR}/tail.txt",
                            "tag" => "t1",
                            "rotate_wait" => "2s",
                            "refresh_interval" => "1s"
                          })
  COMMON_CONFIG = CONFIG + config_element("", "", { "pos_file" => "#{TMP_DIR}/tail.pos" })
  CONFIG_READ_FROM_HEAD = config_element("", "", { "read_from_head" => true })
  CONFIG_ENABLE_WATCH_TIMER = config_element("", "", { "enable_watch_timer" => false })
  CONFIG_DISABLE_STAT_WATCHER = config_element("", "", { "enable_stat_watcher" => false })
  CONFIG_OPEN_ON_EVERY_UPDATE = config_element("", "", { "open_on_every_update" => true })
  COMMON_FOLLOW_INODE_CONFIG = config_element("ROOT", "", {
                                                "path" => "#{TMP_DIR}/tail.txt*",
                                                "pos_file" => "#{TMP_DIR}/tail.pos",
                                                "tag" => "t1",
                                                "refresh_interval" => "1s",
                                                "read_from_head" => "true",
                                                "format" => "none",
                                                "rotate_wait" => "1s",
                                                "follow_inodes" => "true"
                                              })
  SINGLE_LINE_CONFIG = config_element("", "", { "format" => "/(?<message>.*)/" })
  PARSE_SINGLE_LINE_CONFIG = config_element("", "", {}, [config_element("parse", "", { "@type" => "/(?<message>.*)/" })])
  MULTILINE_CONFIG = config_element(
    "", "", {
      "format" => "multiline",
      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/",
      "format_firstline" => "/^[s]/"
    })
  PARSE_MULTILINE_CONFIG = config_element(
    "", "", {},
    [config_element("parse", "", {
                      "@type" => "multiline",
                      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/",
                      "format_firstline" => "/^[s]/"
                    })
    ])

  MULTILINE_CONFIG_WITH_NEWLINE = config_element(
    "", "", {
      "format" => "multiline",
      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.[^\\n]+))?/",
      "format_firstline" => "/^[s]/"
    })
  PARSE_MULTILINE_CONFIG_WITH_NEWLINE = config_element(
    "", "", {},
    [config_element("parse", "", {
                      "@type" => "multiline",
                      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.[^\\n]+))?/",
                      "format_firstline" => "/^[s]/"
                    })
    ])

  def create_driver(conf = SINGLE_LINE_CONFIG, use_common_conf = true)
    config = use_common_conf ? COMMON_CONFIG + conf : conf
    Fluent::Test::Driver::Input.new(Fluent::Plugin::TailInput).configure(config)
  end

  sub_test_case "configure" do
    test "plain single line" do
      d = create_driver
      assert_equal ["#{TMP_DIR}/tail.txt"], d.instance.paths
      assert_equal "t1", d.instance.tag
      assert_equal 2, d.instance.rotate_wait
      assert_equal "#{TMP_DIR}/tail.pos", d.instance.pos_file
      assert_equal 1000, d.instance.read_lines_limit
      assert_equal -1, d.instance.read_bytes_limit_per_second
      assert_equal false, d.instance.ignore_repeated_permission_error
      assert_nothing_raised do
        d.instance.have_read_capability?
      end
    end

    data("empty" => config_element,
         "w/o @type" => config_element("", "", {}, [config_element("parse", "", {})]))
    test "w/o parse section" do |conf|
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
      end
    end

    test "multi paths with path_delimiter" do
      c = config_element("ROOT", "", { "path" => "tail.txt|test2|tmp,dev", "tag" => "t1", "path_delimiter" => "|" })
      d = create_driver(c + PARSE_SINGLE_LINE_CONFIG, false)
      assert_equal ["tail.txt", "test2", "tmp,dev"], d.instance.paths
    end

    test "multi paths with same path configured twice" do
      c = config_element("ROOT", "", { "path" => "test1.txt,test2.txt,test1.txt", "tag" => "t1", "path_delimiter" => "," })
      d = create_driver(c + PARSE_SINGLE_LINE_CONFIG, false)
      assert_equal ["test2.txt","test1.txt"].sort, d.instance.paths.sort
    end

    test "multi paths with invaid path_delimiter" do
      c = config_element("ROOT", "", { "path" => "tail.txt|test2|tmp,dev", "tag" => "t1", "path_delimiter" => "*" })
      assert_raise(Fluent::ConfigError) do
        create_driver(c + PARSE_SINGLE_LINE_CONFIG, false)
      end
    end

    test "follow_inodes w/o pos file" do
      assert_raise(Fluent::ConfigError) do
        create_driver(CONFIG + config_element('', '', {'follow_inodes' => 'true'}))
      end
    end

    sub_test_case "log throttling per file" do
      test "w/o watcher timer is invalid" do
        conf = CONFIG_ENABLE_WATCH_TIMER + config_element("ROOT", "", {"read_bytes_limit_per_second" => "8k"})
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "valid" do
        conf = config_element("ROOT", "", {"read_bytes_limit_per_second" => "8k"})
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
    end

    test "both enable_watch_timer and enable_stat_watcher are false" do
      assert_raise(Fluent::ConfigError) do
        create_driver(CONFIG_ENABLE_WATCH_TIMER + CONFIG_DISABLE_STAT_WATCHER + PARSE_SINGLE_LINE_CONFIG)
      end
    end

    sub_test_case "encoding" do
      test "valid" do
        conf = SINGLE_LINE_CONFIG + config_element("", "", { "encoding" => "utf-8" })
        d = create_driver(conf)
        assert_equal Encoding::UTF_8, d.instance.encoding
      end

      test "invalid" do
        conf = SINGLE_LINE_CONFIG + config_element("", "", { "encoding" => "no-such-encoding" })
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
    end

    sub_test_case "from_encoding" do
      test "only specified from_encoding raise ConfigError" do
        conf = SINGLE_LINE_CONFIG + config_element("", "", { "from_encoding" => "utf-8" })
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "valid" do
        conf = SINGLE_LINE_CONFIG + config_element("", "", {
                                                     "from_encoding" => "utf-8",
                                                     "encoding" => "utf-8"
                                                   })
        d = create_driver(conf)
        assert_equal(Encoding::UTF_8, d.instance.from_encoding)
      end

      test "invalid" do
        conf = SINGLE_LINE_CONFIG + config_element("", "", {
                                                     "from_encoding" => "no-such-encoding",
                                                     "encoding" => "utf-8"
                                                   })
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
    end
  end

  sub_test_case "singleline" do
    data(flat: SINGLE_LINE_CONFIG,
         parse: PARSE_SINGLE_LINE_CONFIG)
    def test_emit(data)
      config = data
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver(config)

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3\ntest4"
        }
      end

      events = d.events
      assert_equal(true, events.length > 0)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
      assert(events[0][1].is_a?(Fluent::EventTime))
      assert(events[1][1].is_a?(Fluent::EventTime))
      assert_equal(1, d.emit_count)
    end

    def test_emit_with_emit_unmatched_lines_true
      config = config_element("", "", { "format" => "/^(?<message>test.*)/", "emit_unmatched_lines" => true })
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test line 1"
          f.puts "test line 2"
          f.puts "bad line 1"
          f.puts "test line 3"
        }
      end

      events = d.events
      assert_equal(4, events.length)
      assert_equal({"message" => "test line 1"}, events[0][2])
      assert_equal({"message" => "test line 2"}, events[1][2])
      assert_equal({"unmatched_line" => "bad line 1"}, events[2][2])
      assert_equal({"message" => "test line 3"}, events[3][2])
    end

    data('flat 1' => [:flat, 1, 2],
         'flat 10' => [:flat, 10, 1],
         'parse 1' => [:parse, 1, 2],
         'parse 10' => [:parse, 10, 1])
    def test_emit_with_read_lines_limit(data)
      config_style, limit, num_events = data
      case config_style
      when :flat
        config = CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG + config_element("", "", { "read_lines_limit" => limit })
      when :parse
        config = CONFIG_READ_FROM_HEAD + config_element("", "", { "read_lines_limit" => limit }) + PARSE_SINGLE_LINE_CONFIG
      end
      d = create_driver(config)
      msg = 'test' * 2000 # in_tail reads 8192 bytes at once.

      d.run(expect_emits: num_events, timeout: 2) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts msg
          f.puts msg
        }
      end

      events = d.events
      assert_equal(true, events.length > 0)
      assert_equal({"message" => msg}, events[0][2])
      assert_equal({"message" => msg}, events[1][2])
      assert num_events <= d.emit_count
    end

    sub_test_case "log throttling per file" do
      teardown do
        cleanup_file("#{TMP_DIR}/tail.txt")
      end

      sub_test_case "reads_bytes_per_second w/o throttled" do
        data("flat 8192 bytes, 2 events"        => [:flat, 100, 8192, 2],
             "flat 8192 bytes, 2 events w/o stat watcher" => [:flat_without_stat, 100, 8192, 2],
             "flat #{8192*10} bytes, 20 events"  => [:flat, 100, (8192 * 10), 20],
             "flat #{8192*10} bytes, 20 events w/o stat watcher"  => [:flat_without_stat, 100, (8192 * 10), 20],
             "parse #{8192*4} bytes, 8 events"  => [:parse, 100, (8192 * 4), 8],
             "parse #{8192*4} bytes, 8 events w/o stat watcher"  => [:parse_without_stat, 100, (8192 * 4), 8],
             "parse #{8192*10} bytes, 20 events" => [:parse, 100, (8192 * 10), 20],
             "parse #{8192*10} bytes, 20 events w/o stat watcher" => [:parse_without_stat, 100, (8192 * 10), 20],
             "flat 8k bytes with unit, 2 events" => [:flat, 100, "8k", 2])
        def test_emit_with_read_bytes_limit_per_second(data)
          config_style, limit, limit_bytes, num_events = data
          case config_style
          when :flat
            config = CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG + config_element("", "", { "read_lines_limit" => limit, "read_bytes_limit_per_second" => limit_bytes })
          when :parse
            config = CONFIG_READ_FROM_HEAD + config_element("", "", { "read_lines_limit" => limit, "read_bytes_limit_per_second" => limit_bytes }) + PARSE_SINGLE_LINE_CONFIG
          when :flat_without_stat
            config = CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG + CONFIG_DISABLE_STAT_WATCHER + config_element("", "", { "read_lines_limit" => limit, "read_bytes_limit_per_second" => limit_bytes })
          when :parse_without_stat
            config = CONFIG_READ_FROM_HEAD + CONFIG_DISABLE_STAT_WATCHER + config_element("", "", { "read_lines_limit" => limit, "read_bytes_limit_per_second" => limit_bytes }) + PARSE_SINGLE_LINE_CONFIG
          end

          msg = 'test' * 2000 # in_tail reads 8192 bytes at once.
          start_time = Fluent::Clock.now

          d = create_driver(config)
          d.run(expect_emits: 2) do
            File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
              100.times do
                f.puts msg
              end
            }
          end

          assert_true(Fluent::Clock.now - start_time > 1)
          assert_equal(num_events.times.map { {"message" => msg} },
                       d.events.collect { |event| event[2] })
        end

        def test_read_bytes_limit_precede_read_lines_limit
          config = CONFIG_READ_FROM_HEAD +
                   SINGLE_LINE_CONFIG +
                   config_element("", "", {
                                    "read_lines_limit" => 1000,
                                    "read_bytes_limit_per_second" => 8192
                                  })
          msg = 'abc'
          start_time = Fluent::Clock.now
          d = create_driver(config)
          d.run(expect_emits: 2) do
            File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
              8000.times do
                f.puts msg
              end
            }
          end

          assert_true(Fluent::Clock.now - start_time > 1)
          assert_equal(4096.times.map { {"message" => msg} },
                       d.events.collect { |event| event[2] })
        end
      end

      sub_test_case "reads_bytes_per_second w/ throttled already" do
        data("flat 8192 bytes"  => [:flat, 100, 8192],
             "parse 8192 bytes" => [:parse, 100, 8192])
        def test_emit_with_read_bytes_limit_per_second(data)
          config_style, limit, limit_bytes = data
          case config_style
          when :flat
            config = CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG + config_element("", "", { "read_lines_limit" => limit, "read_bytes_limit_per_second" => limit_bytes })
          when :parse
            config = CONFIG_READ_FROM_HEAD + config_element("", "", { "read_lines_limit" => limit, "read_bytes_limit_per_second" => limit_bytes }) + PARSE_SINGLE_LINE_CONFIG
          end
          d = create_driver(config)
          msg = 'test' * 2000 # in_tail reads 8192 bytes at once.

          mock.proxy(d.instance).io_handler(anything, anything) do |io_handler|
            require 'fluent/config/types'
            limit_bytes_value = Fluent::Config.size_value(limit_bytes)
            io_handler.instance_variable_set(:@number_bytes_read, limit_bytes_value)
            if Fluent.linux?
              mock.proxy(io_handler).handle_notify.at_least(5)
            else
              mock.proxy(io_handler).handle_notify.twice
            end
            io_handler
          end

          File.open("#{TMP_DIR}/tail.txt", "ab") do |f|
            100.times do
              f.puts msg
            end
          end

          # We should not do shutdown here due to hard timeout.
          d.run do
            start_time = Fluent::Clock.now
            while Fluent::Clock.now - start_time < 0.8 do
              File.open("#{TMP_DIR}/tail.txt", "ab") do |f|
                f.puts msg
                f.flush
              end
              sleep 0.05
            end
          end

          assert_equal([], d.events)
        end
      end

      sub_test_case "EOF with reads_bytes_per_second" do
        def test_longer_than_rotate_wait
          limit_bytes = 8192
          num_lines = 1024 * 3
          msg = "08bytes"

          File.open("#{TMP_DIR}/tail.txt", "wb") do |f|
            f.write("#{msg}\n" * num_lines)
          end

          config = CONFIG_READ_FROM_HEAD +
                   SINGLE_LINE_CONFIG +
                   config_element("", "", {
                                    "read_bytes_limit_per_second" => limit_bytes,
                                    "rotate_wait" => 0.1,
                                    "refresh_interval" => 0.5,
                                  })

          rotated = false
          d = create_driver(config)
          d.run(timeout: 10) do
            while d.events.size < num_lines do
              if d.events.size > 0 && !rotated
                cleanup_file("#{TMP_DIR}/tail.txt")
                FileUtils.touch("#{TMP_DIR}/tail.txt")
                rotated = true
              end
              sleep 0.3
            end
          end

          assert_equal(num_lines,
                       d.events.count do |event|
                         event[2]["message"] == msg
                       end)
        end

        def test_shorter_than_rotate_wait
          limit_bytes = 8192
          num_lines = 1024 * 2
          msg = "08bytes"

          File.open("#{TMP_DIR}/tail.txt", "wb") do |f|
            f.write("#{msg}\n" * num_lines)
          end

          config = CONFIG_READ_FROM_HEAD +
                   SINGLE_LINE_CONFIG +
                   config_element("", "", {
                                    "read_bytes_limit_per_second" => limit_bytes,
                                    "rotate_wait" => 2,
                                    "refresh_interval" => 0.5,
                                  })

          start_time = Fluent::Clock.now
          rotated = false
          detached = false
          d = create_driver(config)
          mock.proxy(d.instance).setup_watcher(anything, anything) do |tw|
            mock.proxy(tw).detach(anything) do |v|
              detached = true
              v
            end
            tw
          end.twice

          d.run(timeout: 10) do
            until detached do
              if d.events.size > 0 && !rotated
                cleanup_file("#{TMP_DIR}/tail.txt")
                FileUtils.touch("#{TMP_DIR}/tail.txt")
                rotated = true
              end
              sleep 0.3
            end
          end

          assert_true(Fluent::Clock.now - start_time > 2)
          assert_equal(num_lines,
                       d.events.count do |event|
                         event[2]["message"] == msg
                       end)
        end
      end
    end

    data(flat: CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG,
         parse: CONFIG_READ_FROM_HEAD + PARSE_SINGLE_LINE_CONFIG)
    def test_emit_with_read_from_head(data)
      config = data
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver(config)

      d.run(expect_emits: 2) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
      end

      events = d.events
      assert(events.length > 0)
      assert_equal({"message" => "test1"}, events[0][2])
      assert_equal({"message" => "test2"}, events[1][2])
      assert_equal({"message" => "test3"}, events[2][2])
      assert_equal({"message" => "test4"}, events[3][2])
    end

    data(flat: CONFIG_ENABLE_WATCH_TIMER + SINGLE_LINE_CONFIG,
         parse: CONFIG_ENABLE_WATCH_TIMER + PARSE_SINGLE_LINE_CONFIG)
    def test_emit_with_enable_watch_timer(data)
      config = data
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver(config)

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
        # according to cool.io's stat_watcher.c, systems without inotify will use
        # an "automatic" value, typically around 5 seconds
      end

      events = d.events
      assert(events.length > 0)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
    end

    data(flat: CONFIG_DISABLE_STAT_WATCHER + SINGLE_LINE_CONFIG,
         parse: CONFIG_DISABLE_STAT_WATCHER + PARSE_SINGLE_LINE_CONFIG)
    def test_emit_with_disable_stat_watcher(data)
      config = data
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver(config)

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
      end

      events = d.events
      assert(events.length > 0)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
    end
  end

  class TestWithSystem < self
    include Fluent::SystemConfig::Mixin

    OVERRIDE_FILE_PERMISSION = 0620
    CONFIG_SYSTEM = %[
      <system>
        file_permission #{OVERRIDE_FILE_PERMISSION}
      </system>
    ]

    def setup
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?
      # Store default permission
      @default_permission = system_config.instance_variable_get(:@file_permission)
    end

    def teardown
      # Restore default permission
      system_config.instance_variable_set(:@file_permission, @default_permission)
    end

    def parse_system(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
    end

    def test_emit_with_system
      system_conf = parse_system(CONFIG_SYSTEM)
      sc = Fluent::SystemConfig.new(system_conf)
      Fluent::Engine.init(sc)
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
      end

      events = d.events
      assert_equal(true, events.length > 0)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
      assert(events[0][1].is_a?(Fluent::EventTime))
      assert(events[1][1].is_a?(Fluent::EventTime))
      assert_equal(1, d.emit_count)
      pos = d.instance.instance_variable_get(:@pf_file)
      mode = "%o" % File.stat(pos).mode
      assert_equal OVERRIDE_FILE_PERMISSION, mode[-3, 3].to_i
    end
  end

  sub_test_case "rotate file" do
    data(flat: SINGLE_LINE_CONFIG,
         parse: PARSE_SINGLE_LINE_CONFIG)
    def test_rotate_file(data)
      config = data
      events = sub_test_rotate_file(config, expect_emits: 2)
      assert_equal(4, events.length)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
      assert_equal({"message" => "test5"}, events[2][2])
      assert_equal({"message" => "test6"}, events[3][2])
    end

    data(flat: CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG,
         parse: CONFIG_READ_FROM_HEAD + PARSE_SINGLE_LINE_CONFIG)
    def test_rotate_file_with_read_from_head(data)
      config = data
      events = sub_test_rotate_file(config, expect_records: 6)
      assert_equal(6, events.length)
      assert_equal({"message" => "test1"}, events[0][2])
      assert_equal({"message" => "test2"}, events[1][2])
      assert_equal({"message" => "test3"}, events[2][2])
      assert_equal({"message" => "test4"}, events[3][2])
      assert_equal({"message" => "test5"}, events[4][2])
      assert_equal({"message" => "test6"}, events[5][2])
    end

    data(flat: CONFIG_OPEN_ON_EVERY_UPDATE + CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG,
         parse: CONFIG_OPEN_ON_EVERY_UPDATE + CONFIG_READ_FROM_HEAD + PARSE_SINGLE_LINE_CONFIG)
    def test_rotate_file_with_open_on_every_update(data)
      config = data
      events = sub_test_rotate_file(config, expect_records: 6)
      assert_equal(6, events.length)
      assert_equal({"message" => "test1"}, events[0][2])
      assert_equal({"message" => "test2"}, events[1][2])
      assert_equal({"message" => "test3"}, events[2][2])
      assert_equal({"message" => "test4"}, events[3][2])
      assert_equal({"message" => "test5"}, events[4][2])
      assert_equal({"message" => "test6"}, events[5][2])
    end

    data(flat: SINGLE_LINE_CONFIG,
         parse: PARSE_SINGLE_LINE_CONFIG)
    def test_rotate_file_with_write_old(data)
      config = data
      events = sub_test_rotate_file(config, expect_emits: 3) { |rotated_file|
        File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }
        rotated_file.puts "test7"
        rotated_file.puts "test8"
        rotated_file.flush

        sleep 1
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "test5"
          f.puts "test6"
        }
      }
      # This test sometimes fails and it shows a potential bug of in_tail
      # https://github.com/fluent/fluentd/issues/1434
      assert_equal(6, events.length)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
      assert_equal({"message" => "test7"}, events[2][2])
      assert_equal({"message" => "test8"}, events[3][2])
      assert_equal({"message" => "test5"}, events[4][2])
      assert_equal({"message" => "test6"}, events[5][2])
    end

    data(flat: SINGLE_LINE_CONFIG,
         parse: PARSE_SINGLE_LINE_CONFIG)
    def test_rotate_file_with_write_old_and_no_new_file(data)
      config = data
      events = sub_test_rotate_file(config, expect_emits: 2) { |rotated_file|
        rotated_file.puts "test7"
        rotated_file.puts "test8"
        rotated_file.flush
      }
      assert_equal(4, events.length)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
      assert_equal({"message" => "test7"}, events[2][2])
      assert_equal({"message" => "test8"}, events[3][2])
    end

    def sub_test_rotate_file(config = nil, expect_emits: nil, expect_records: nil, timeout: nil)
      file = Fluent::FileWrapper.open("#{TMP_DIR}/tail.txt", "wb")
      file.puts "test1"
      file.puts "test2"
      file.flush

      d = create_driver(config)
      d.run(expect_emits: expect_emits, expect_records: expect_records, timeout: timeout) do
        size = d.emit_count
        file.puts "test3"
        file.puts "test4"
        file.flush
        sleep(0.1) until d.emit_count >= size + 1
        size = d.emit_count

        if Fluent.windows?
          file.close
          FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt", force: true)
          file = File.open("#{TMP_DIR}/tail.txt", "ab")
        else
          FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt")
        end
        if block_given?
          yield file
        else
          File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }
          sleep 1

          File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
            f.puts "test5"
            f.puts "test6"
          }
        end
      end

      d.events
    ensure
      file.close if file && !file.closed?
    end
  end

  def test_truncate_file
    omit "Permission denied error happen on Windows. Need fix" if Fluent.windows?

    config = SINGLE_LINE_CONFIG
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
      f.puts "test1"
      f.puts "test2"
      f.flush
    }

    d = create_driver(config)

    d.run(expect_emits: 2) do
      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "test3\ntest4"
        f.flush
      }
      waiting(2) { sleep 0.1 until d.events.length == 2 }
      File.truncate("#{TMP_DIR}/tail.txt", 6)
    end

    expected = {
      emit_count: 2,
      events: [
        [Fluent::EventTime, {"message" => "test3"}],
        [Fluent::EventTime, {"message" => "test4"}],
        [Fluent::EventTime, {"message" => "test1"}],
      ]
    }
    actual = {
      emit_count: d.emit_count,
      events: d.events.collect{|event| [event[1].class, event[2]]}
    }
    assert_equal(expected, actual)
  end

  def test_move_truncate_move_back
    omit "Permission denied error happen on Windows. Need fix" if Fluent.windows?

    config = SINGLE_LINE_CONFIG
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver(config)

    d.run(expect_emits: 1) do
      if Fluent.windows?
        FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt", force: true)
      else
        FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt")
      end
      sleep(1)
      File.truncate("#{TMP_DIR}/tail2.txt", 6)
      sleep(1)
      if Fluent.windows?
        FileUtils.mv("#{TMP_DIR}/tail2.txt", "#{TMP_DIR}/tail.txt", force: true)
      else
        FileUtils.mv("#{TMP_DIR}/tail2.txt", "#{TMP_DIR}/tail.txt")
      end
    end

    events = d.events
    assert_equal(1, events.length)
    assert_equal({"message" => "test1"}, events[0][2])
    assert(events[0][1].is_a?(Fluent::EventTime))
    assert_equal(1, d.emit_count)
  end

  def test_lf
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f| }

    d = create_driver

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.print "test3"
      }
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "test4"
      }
    end

    events = d.events
    assert_equal(true, events.length > 0)
    assert_equal({"message" => "test3test4"}, events[0][2])
  end

  def test_whitespace
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f| }

    d = create_driver

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "    "		# 4 spaces
        f.puts "    4 spaces"
        f.puts "4 spaces    "
        f.puts "	"	# tab
        f.puts "	tab"
        f.puts "tab	"
      }
    end

    events = d.events
    assert_equal(true, events.length > 0)
    assert_equal({"message" => "    "}, events[0][2])
    assert_equal({"message" => "    4 spaces"}, events[1][2])
    assert_equal({"message" => "4 spaces    "}, events[2][2])
    assert_equal({"message" => "	"}, events[3][2])
    assert_equal({"message" => "	tab"}, events[4][2])
    assert_equal({"message" => "tab	"}, events[5][2])
  end

  data(
    'flat default encoding' => [SINGLE_LINE_CONFIG, Encoding::ASCII_8BIT],
    'flat explicit encoding config' => [SINGLE_LINE_CONFIG + config_element("", "", { "encoding" => "utf-8" }), Encoding::UTF_8],
    'parse default encoding' => [PARSE_SINGLE_LINE_CONFIG, Encoding::ASCII_8BIT],
    'parse explicit encoding config' => [PARSE_SINGLE_LINE_CONFIG + config_element("", "", { "encoding" => "utf-8" }), Encoding::UTF_8])
  def test_encoding(data)
    encoding_config, encoding = data

    d = create_driver(CONFIG_READ_FROM_HEAD + encoding_config)

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test"
      }
    end

    events = d.events
    assert_equal(encoding, events[0][2]['message'].encoding)
  end

  def test_from_encoding
    conf = config_element(
      "", "", {
        "format" => "/(?<message>.*)/",
        "read_from_head" => "true",
        "from_encoding" => "cp932",
        "encoding" => "utf-8"
      })
    d = create_driver(conf)
    cp932_message = "\x82\xCD\x82\xEB\x81\x5B\x82\xED\x81\x5B\x82\xE9\x82\xC7".force_encoding(Encoding::CP932)
    utf8_message = cp932_message.encode(Encoding::UTF_8)

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "w:cp932") {|f|
        f.puts cp932_message
      }
    end

    events = d.events
    assert_equal(utf8_message, events[0][2]['message'])
    assert_equal(Encoding::UTF_8, events[0][2]['message'].encoding)
  end

  def test_from_encoding_utf16
    conf = config_element(
      "", "", {
        "format" => "/(?<message>.*)/",
        "read_from_head" => "true",
        "from_encoding" => "utf-16le",
        "encoding" => "utf-8"
      })
    d = create_driver(conf)
    utf16_message = "\u306F\u308D\u30FC\u308F\u30FC\u308B\u3069\n".encode(Encoding::UTF_16LE)
    utf8_message = utf16_message.encode(Encoding::UTF_8).strip

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "w:utf-16le") { |f|
        f.write utf16_message
      }
    end

    events = d.events
    assert_equal(utf8_message, events[0][2]['message'])
    assert_equal(Encoding::UTF_8, events[0][2]['message'].encoding)
  end

  def test_encoding_with_bad_character
    conf = config_element(
      "", "", {
        "format" => "/(?<message>.*)/",
        "read_from_head" => "true",
        "from_encoding" => "ASCII-8BIT",
        "encoding" => "utf-8"
      })
    d = create_driver(conf)

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "w") { |f|
        f.write "te\x86st\n"
      }
    end

    events = d.events
    assert_equal("te\uFFFDst", events[0][2]['message'])
    assert_equal(Encoding::UTF_8, events[0][2]['message'].encoding)
  end

  sub_test_case "multiline" do
    data(flat: MULTILINE_CONFIG,
         parse: PARSE_MULTILINE_CONFIG)
    def test_multiline(data)
      config = data
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "f test1"
          f.puts "s test2"
          f.puts "f test3"
          f.puts "f test4"
          f.puts "s test5"
          f.puts "s test6"
          f.puts "f test7"
          f.puts "s test8"
        }
      end

      events = d.events
      assert_equal(4, events.length)
      assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, events[0][2])
      assert_equal({"message1" => "test5"}, events[1][2])
      assert_equal({"message1" => "test6", "message2" => "test7"}, events[2][2])
      assert_equal({"message1" => "test8"}, events[3][2])
    end

    data(flat: MULTILINE_CONFIG,
         parse: PARSE_MULTILINE_CONFIG)
    def test_multiline_with_emit_unmatched_lines_true(data)
      config = data + config_element("", "", { "emit_unmatched_lines" => true })
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "f test1"
          f.puts "s test2"
          f.puts "f test3"
          f.puts "f test4"
          f.puts "s test5"
          f.puts "s test6"
          f.puts "f test7"
          f.puts "s test8"
        }
      end

      events = d.events
      assert_equal(5, events.length)
      assert_equal({"unmatched_line" => "f test1"}, events[0][2])
      assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, events[1][2])
      assert_equal({"message1" => "test5"}, events[2][2])
      assert_equal({"message1" => "test6", "message2" => "test7"}, events[3][2])
      assert_equal({"message1" => "test8"}, events[4][2])
    end

    data(
      flat: MULTILINE_CONFIG_WITH_NEWLINE,
      parse: PARSE_MULTILINE_CONFIG_WITH_NEWLINE)
    def test_multiline_with_emit_unmatched_lines2(data)
      config = data + config_element("", "", { "emit_unmatched_lines" => true })
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      d = create_driver(config)
      d.run(expect_emits: 0, timeout: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "s test0"
          f.puts "f test1"
          f.puts "f test2"

          f.puts "f test3"

          f.puts "s test4"
          f.puts "f test5"
          f.puts "f test6"
        }
      end

      events = d.events
      assert_equal({"message1" => "test0", "message2" => "test1", "message3" => "test2"}, events[0][2])
      assert_equal({ 'unmatched_line' => "f test3" }, events[1][2])
      assert_equal({"message1" => "test4", "message2" => "test5", "message3" => "test6"}, events[2][2])
    end

    data(flat: MULTILINE_CONFIG,
         parse: PARSE_MULTILINE_CONFIG)
    def test_multiline_with_flush_interval(data)
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      config = data + config_element("", "", { "multiline_flush_interval" => "2s" })
      d = create_driver(config)

      assert_equal 2, d.instance.multiline_flush_interval

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "f test1"
          f.puts "s test2"
          f.puts "f test3"
          f.puts "f test4"
          f.puts "s test5"
          f.puts "s test6"
          f.puts "f test7"
          f.puts "s test8"
        }
      end

      events = d.events
      assert_equal(4, events.length)
      assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, events[0][2])
      assert_equal({"message1" => "test5"}, events[1][2])
      assert_equal({"message1" => "test6", "message2" => "test7"}, events[2][2])
      assert_equal({"message1" => "test8"}, events[3][2])
    end

    data(
      'flat default encoding' => [MULTILINE_CONFIG, Encoding::ASCII_8BIT],
      'flat explicit encoding config' => [MULTILINE_CONFIG + config_element("", "", { "encoding" => "utf-8" }), Encoding::UTF_8],
      'parse default encoding' => [PARSE_MULTILINE_CONFIG, Encoding::ASCII_8BIT],
      'parse explicit encoding config' => [PARSE_MULTILINE_CONFIG + config_element("", "", { "encoding" => "utf-8" }), Encoding::UTF_8])
    def test_multiline_encoding_of_flushed_record(data)
      encoding_config, encoding = data

      config = config_element("", "", {
                                "multiline_flush_interval" => "2s",
                                "read_from_head" => "true",
                              })
      d = create_driver(config + encoding_config)

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "wb") { |f|
          f.puts "s test"
        }
      end
      events = d.events
      assert_equal(1, events.length)
      assert_equal(encoding, events[0][2]['message1'].encoding)
    end

    def test_multiline_from_encoding_of_flushed_record
      conf = MULTILINE_CONFIG + config_element(
               "", "",
               {
                 "multiline_flush_interval" => "1s",
                 "read_from_head" => "true",
                 "from_encoding" => "cp932",
                 "encoding" => "utf-8"
               })
      d = create_driver(conf)

      cp932_message = "s \x82\xCD\x82\xEB\x81\x5B\x82\xED\x81\x5B\x82\xE9\x82\xC7".force_encoding(Encoding::CP932)
      utf8_message = "\x82\xCD\x82\xEB\x81\x5B\x82\xED\x81\x5B\x82\xE9\x82\xC7".encode(Encoding::UTF_8, Encoding::CP932)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "w:cp932") { |f|
          f.puts cp932_message
        }
      end

      events = d.events
      assert_equal(1, events.length)
      assert_equal(utf8_message, events[0][2]['message1'])
      assert_equal(Encoding::UTF_8, events[0][2]['message1'].encoding)
    end

    data(flat: config_element(
           "", "", {
             "format" => "multiline",
             "format1" => "/^s (?<message1>[^\\n]+)\\n?/",
             "format2" => "/(f (?<message2>[^\\n]+)\\n?)?/",
             "format3" => "/(f (?<message3>.*))?/",
             "format_firstline" => "/^[s]/"
           }),
         parse: config_element(
           "", "", {},
           [config_element("parse", "", {
                             "@type" => "multiline",
                             "format1" => "/^s (?<message1>[^\\n]+)\\n?/",
                             "format2" => "/(f (?<message2>[^\\n]+)\\n?)?/",
                             "format3" => "/(f (?<message3>.*))?/",
                             "format_firstline" => "/^[s]/"
                           })
           ])
        )
    def test_multiline_with_multiple_formats(data)
      config = data
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "f test1"
          f.puts "s test2"
          f.puts "f test3"
          f.puts "f test4"
          f.puts "s test5"
          f.puts "s test6"
          f.puts "f test7"
          f.puts "s test8"
        }
      end

      events = d.events
      assert(events.length > 0)
      assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, events[0][2])
      assert_equal({"message1" => "test5"}, events[1][2])
      assert_equal({"message1" => "test6", "message2" => "test7"}, events[2][2])
      assert_equal({"message1" => "test8"}, events[3][2])
    end

    data(flat: config_element(
           "", "", {
             "format" => "multiline",
             "format1" => "/^[s|f] (?<message>.*)/",
             "format_firstline" => "/^[s]/"
           }),
         parse: config_element(
           "", "", {},
           [config_element("parse", "", {
                             "@type" => "multiline",
                             "format1" => "/^[s|f] (?<message>.*)/",
                             "format_firstline" => "/^[s]/"
                           })
           ])
        )
    def test_multilinelog_with_multiple_paths(data)
      files = ["#{TMP_DIR}/tail1.txt", "#{TMP_DIR}/tail2.txt"]
      files.each { |file| File.open(file, "wb") { |f| } }

      config = data + config_element("", "", {
                                       "path" => "#{files[0]},#{files[1]}",
                                       "tag" => "t1",
                                     })
      d = create_driver(config, false)
      d.run(expect_emits: 2) do
        files.each do |file|
          File.open(file, 'ab') { |f|
            f.puts "f #{file} line should be ignored"
            f.puts "s test1"
            f.puts "f test2"
            f.puts "f test3"
            f.puts "s test4"
          }
        end
      end

      events = d.events
      assert_equal({"message" => "test1\nf test2\nf test3"}, events[0][2])
      assert_equal({"message" => "test1\nf test2\nf test3"}, events[1][2])
      # "test4" events are here because these events are flushed at shutdown phase
      assert_equal({"message" => "test4"}, events[2][2])
      assert_equal({"message" => "test4"}, events[3][2])
    end

    data(flat: config_element("", "", {
                                "format" => "multiline",
                                "format1" => "/(?<var1>foo \\d)\\n/",
                                "format2" => "/(?<var2>bar \\d)\\n/",
                                "format3" => "/(?<var3>baz \\d)/"
                              }),
         parse: config_element(
           "", "", {},
           [config_element("parse", "", {
                             "@type" => "multiline",
                             "format1" => "/(?<var1>foo \\d)\\n/",
                             "format2" => "/(?<var2>bar \\d)\\n/",
                             "format3" => "/(?<var3>baz \\d)/"
                           })
           ])
        )
    def test_multiline_without_firstline(data)
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      config = data
      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "foo 1"
          f.puts "bar 1"
          f.puts "baz 1"
          f.puts "foo 2"
          f.puts "bar 2"
          f.puts "baz 2"
        }
      end

      events = d.events
      assert_equal(2, events.length)
      assert_equal({"var1" => "foo 1", "var2" => "bar 1", "var3" => "baz 1"}, events[0][2])
      assert_equal({"var1" => "foo 2", "var2" => "bar 2", "var3" => "baz 2"}, events[1][2])
    end
  end

  sub_test_case "path" do
    # * path test
    # TODO: Clean up tests
    EX_ROTATE_WAIT = 0
    EX_FOLLOW_INODES = false

    EX_CONFIG = config_element("", "", {
                                 "tag" => "tail",
                                 "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                 "format" => "none",
                                 "pos_file" => "#{TMP_DIR}/tail.pos",
                                 "read_from_head" => true,
                                 "refresh_interval" => 30,
                                 "rotate_wait" => "#{EX_ROTATE_WAIT}s",
                                 "follow_inodes" => "#{EX_FOLLOW_INODES}",
                               })
    def test_expand_paths
      ex_paths = [
        create_target_info('test/plugin/data/2010/01/20100102-030405.log'),
        create_target_info('test/plugin/data/log/foo/bar.log'),
        create_target_info('test/plugin/data/log/test.log')
      ]
      plugin = create_driver(EX_CONFIG, false).instance
      flexstub(Time) do |timeclass|
        timeclass.should_receive(:now).with_no_args.and_return(Time.new(2010, 1, 2, 3, 4, 5))
        assert_equal ex_paths, plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
      end

      # Test exclusion
      exclude_config = EX_CONFIG + config_element("", "", { "exclude_path" => %Q(["#{ex_paths.last.path}"]) })
      plugin = create_driver(exclude_config, false).instance
      assert_equal ex_paths - [ex_paths.last], plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
    end

    def test_expand_paths_with_duplicate_configuration
      expanded_paths = [
        create_target_info('test/plugin/data/log/foo/bar.log'),
        create_target_info('test/plugin/data/log/test.log')
      ]
      duplicate_config = EX_CONFIG.dup
      duplicate_config["path"]="test/plugin/data/log/**/*.log, test/plugin/data/log/**/*.log"
      plugin = create_driver(EX_CONFIG, false).instance
      assert_equal expanded_paths, plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
    end

    def test_expand_paths_with_timezone
      ex_paths = [
        create_target_info('test/plugin/data/2010/01/20100102-030405.log'),
        create_target_info('test/plugin/data/log/foo/bar.log'),
        create_target_info('test/plugin/data/log/test.log')
      ]
      ['Asia/Taipei', '+08'].each do |tz_type|
        taipei_config = EX_CONFIG + config_element("", "", {"path_timezone" => tz_type})
        plugin = create_driver(taipei_config, false).instance

        # Test exclude
        exclude_config = taipei_config + config_element("", "", { "exclude_path" => %Q(["test/plugin/**/%Y%m%d-%H%M%S.log"]) })
        exclude_plugin = create_driver(exclude_config, false).instance

        with_timezone('utc') do
          flexstub(Time) do |timeclass|
            # env : 2010-01-01 19:04:05 (UTC), tail path : 2010-01-02 03:04:05 (Asia/Taipei)
            timeclass.should_receive(:now).with_no_args.and_return(Time.new(2010, 1, 1, 19, 4, 5))

            assert_equal ex_paths, plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
            assert_equal ex_paths - [ex_paths.first], exclude_plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
          end
        end
      end
    end

    def test_log_file_without_extension
      expected_files = [
        create_target_info('test/plugin/data/log/bar'),
        create_target_info('test/plugin/data/log/foo/bar.log'),
        create_target_info('test/plugin/data/log/foo/bar2'),
        create_target_info('test/plugin/data/log/test.log')
      ]

      config = config_element("", "", {
        "tag" => "tail",
        "path" => "test/plugin/data/log/**/*",
        "format" => "none",
        "pos_file" => "#{TMP_DIR}/tail.pos"
      })

      plugin = create_driver(config, false).instance
      assert_equal expected_files, plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
    end

    def test_unwatched_files_should_be_removed
      config = config_element("", "", {
                                "tag" => "tail",
                                "path" => "#{TMP_DIR}/*.txt",
                                "format" => "none",
                                "pos_file" => "#{TMP_DIR}/tail.pos",
                                "read_from_head" => true,
                                "refresh_interval" => 1,
                              })
      d = create_driver(config, false)
      d.end_if { d.instance.instance_variable_get(:@tails).keys.size >= 1 }
      d.run(expect_emits: 1, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f| f.puts "test3\n" }
      end

      cleanup_directory(TMP_DIR)
      waiting(20) { sleep 0.1 until Dir.glob("#{TMP_DIR}/*.txt").size == 0 } # Ensure file is deleted on Windows
      waiting(5) { sleep 0.1 until d.instance.instance_variable_get(:@tails).keys.size <= 0 }

      assert_equal 0, d.instance.instance_variable_get(:@tails).keys.size

      d.instance_shutdown
    end

    def count_timer_object
      num = 0
      ObjectSpace.each_object(Fluent::PluginHelper::Timer::TimerWatcher) { |obj|
        num += 1
      }
      num
    end
  end

  sub_test_case "path w/ Linux capability" do
    def capability_enabled?
      if Fluent.linux?
        begin
          require 'capng'
          true
        rescue LoadError
          false
        end
      else
        false
      end
    end

    setup do
      omit "This environment is not enabled Linux capability handling feature" unless capability_enabled?

      @capng = CapNG.new(:current_process)
      flexstub(Fluent::Capability) do |klass|
        klass.should_receive(:new).with(:current_process).and_return(@capng)
      end
    end

    data("dac_read_search" => [:dac_read_search, true,  1],
         "dac_override"    => [:dac_override,    true,  1],
         "chown"           => [:chown,           false, 0],
        )
    test "with partially elevated privileges" do |data|
      cap, result, readable_paths = data
      @capng.update(:add, :effective, cap)

      d = create_driver(
        config_element("ROOT", "", {
                            "path" => "/var/log/ker*.log", # Use /var/log/kern.log
                            "tag" => "t1",
                            "rotate_wait" => "2s"
                       }) + PARSE_SINGLE_LINE_CONFIG, false)

      assert_equal readable_paths, d.instance.expand_paths.length
      assert_equal result, d.instance.have_read_capability?
    end
  end

  def test_pos_file_dir_creation
    config = config_element("", "", {
      "tag" => "tail",
      "path" => "#{TMP_DIR}/*.txt",
      "format" => "none",
      "pos_file" => "#{TMP_DIR}/pos/tail.pos",
      "read_from_head" => true,
      "refresh_interval" => 1
    })

    assert_path_not_exist("#{TMP_DIR}/pos")
    d = create_driver(config, false)
    d.run
    assert_path_exist("#{TMP_DIR}/pos")
    assert_equal '755', File.stat("#{TMP_DIR}/pos").mode.to_s(8)[-3, 3]
  ensure
    cleanup_directory(TMP_DIR)
  end

  def test_pos_file_dir_creation_with_system_dir_permission
    config = config_element("", "", {
      "tag" => "tail",
      "path" => "#{TMP_DIR}/*.txt",
      "format" => "none",
      "pos_file" => "#{TMP_DIR}/pos/tail.pos",
      "read_from_head" => true,
      "refresh_interval" => 1
    })

    assert_path_not_exist("#{TMP_DIR}/pos")

    Fluent::SystemConfig.overwrite_system_config({ "dir_permission" => "744" }) do
      d = create_driver(config, false)
      d.run
    end

    assert_path_exist("#{TMP_DIR}/pos")
    if Fluent.windows?
      assert_equal '755', File.stat("#{TMP_DIR}/pos").mode.to_s(8)[-3, 3]
    else
      assert_equal '744', File.stat("#{TMP_DIR}/pos").mode.to_s(8)[-3, 3]
    end
  ensure
    cleanup_directory(TMP_DIR)
  end

  def test_z_refresh_watchers
    ex_paths = [
      create_target_info('test/plugin/data/2010/01/20100102-030405.log'),
      create_target_info('test/plugin/data/log/foo/bar.log'),
      create_target_info('test/plugin/data/log/test.log'),
    ]
    plugin = create_driver(EX_CONFIG, false).instance
    sio = StringIO.new
    plugin.instance_eval do
      @pf = Fluent::Plugin::TailInput::PositionFile.load(sio, EX_FOLLOW_INODES, {}, logger: $log)
      @loop = Coolio::Loop.new
      @total_rotated_file_metrics = Fluent::Plugin::LocalMetrics.new
      @total_rotated_file_metrics.configure(config_element('metrics', '', {}))
    end

    Timecop.freeze(2010, 1, 2, 3, 4, 5) do
      ex_paths.each do |target_info|
        mock.proxy(Fluent::Plugin::TailInput::TailWatcher).new(target_info, anything, anything, true, false, anything, nil, anything, anything).once
      end

      plugin.refresh_watchers
    end

    path = 'test/plugin/data/2010/01/20100102-030405.log'
    target_info = Fluent::Plugin::TailInput::TargetInfo.new(path, Fluent::FileWrapper.stat(path).ino)
    mock.proxy(plugin).detach_watcher_after_rotate_wait(plugin.instance_variable_get(:@tails)[target_info], target_info.ino)

    Timecop.freeze(2010, 1, 2, 3, 4, 6) do
      path = "test/plugin/data/2010/01/20100102-030406.log"
      inode = Fluent::FileWrapper.stat(path).ino
      target_info = Fluent::Plugin::TailInput::TargetInfo.new(path, inode)
      mock.proxy(Fluent::Plugin::TailInput::TailWatcher).new(target_info, anything, anything, true, false, anything, nil, anything, anything).once
      plugin.refresh_watchers

      flexstub(Fluent::Plugin::TailInput::TailWatcher) do |watcherclass|
        watcherclass.should_receive(:new).never
        plugin.refresh_watchers
      end
    end
  end

  sub_test_case "refresh of pos file" do
    test 'type of pos_file_compaction_interval is time' do
      tail = {
        "tag" => "tail",
        "path" => "#{TMP_DIR}/*.txt",
        "format" => "none",
        "pos_file" => "#{TMP_DIR}/pos/tail.pos",
        "refresh_interval" => 1,
        "read_from_head" => true,
        'pos_file_compaction_interval' => '24h',
      }
      config = config_element("", "", tail)
      d = create_driver(config, false)
      mock(d.instance).timer_execute(:in_tail_refresh_watchers, 1.0).once
      mock(d.instance).timer_execute(:in_tail_refresh_compact_pos_file, 60 * 60 * 24).once
      d.run                     # call start
    end
  end

  sub_test_case "receive_lines" do
    DummyWatcher = Struct.new("DummyWatcher", :tag)

    def test_tag
      d = create_driver(EX_CONFIG, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('tail', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    def test_tag_with_only_star
      config = config_element("", "", {
                                "tag" => "*",
                                "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                "format" => "none",
                                "read_from_head" => true
                              })
      d = create_driver(config, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('foo.bar.log', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    def test_tag_prefix
      config = config_element("", "", {
                                "tag" => "pre.*",
                                "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                "format" => "none",
                                "read_from_head" => true
                              })
      d = create_driver(config, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('pre.foo.bar.log', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    def test_tag_suffix
      config = config_element("", "", {
                                "tag" => "*.post",
                                "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                "format" => "none",
                                "read_from_head" => true
                              })
      d = create_driver(config, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('foo.bar.log.post', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    def test_tag_prefix_and_suffix
      config = config_element("", "", {
                                "tag" => "pre.*.post",
                                "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                "format" => "none",
                                "read_from_head" => true
                              })
      d = create_driver(config, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('pre.foo.bar.log.post', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    def test_tag_prefix_and_suffix_ignore
      config = config_element("", "", {
                                "tag" => "pre.*.post*ignore",
                                "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                "format" => "none",
                                "read_from_head" => true
                              })
      d = create_driver(config, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('pre.foo.bar.log.post', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end
  end

  # Ensure that no fatal exception is raised when a file is missing and that
  # files that do exist are still tailed as expected.
  def test_missing_file
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    # Try two different configs - one with read_from_head and one without,
    # since their interactions with the filesystem differ.
    config1 = config_element("", "", {
                               "tag" => "t1",
                               "path" => "#{TMP_DIR}/non_existent_file.txt,#{TMP_DIR}/tail.txt",
                               "format" => "none",
                               "rotate_wait" => "2s",
                               "pos_file" => "#{TMP_DIR}/tail.pos"
                             })
    config2 = config1 + config_element("", "", { "read_from_head" => true })
    [config1, config2].each do |config|
      d = create_driver(config, false)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
      end
      # This test sometimes fails and it shows a potential bug of in_tail
      # https://github.com/fluent/fluentd/issues/1434
      events = d.events
      assert_equal(2, events.length)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
    end
  end

  sub_test_case 'inode_processing' do
    def test_should_delete_file_pos_entry_for_non_existing_file_with_follow_inodes
      config = COMMON_FOLLOW_INODE_CONFIG

      path = "#{TMP_DIR}/tail.txt"
      ino = 1
      pos = 1234
      File.open("#{TMP_DIR}/tail.pos", "wb") {|f|
        f.puts ("%s\t%016x\t%016x\n" % [path, pos, ino])
      }

      d = create_driver(config, false)
      d.run

      pos_file = File.open("#{TMP_DIR}/tail.pos", "r")
      pos_file.pos = 0

      assert_raise(EOFError) do
        pos_file.readline
      end
    end

    def test_should_write_latest_offset_after_rotate_wait
      config = COMMON_FOLLOW_INODE_CONFIG
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver(config, false)
      d.run(expect_emits: 2, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
        FileUtils.move("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail.txt" + "1")
        sleep 1
        File.open("#{TMP_DIR}/tail.txt" + "1", "ab") {|f| f.puts "test4\n"}
      end

      pos_file = File.open("#{TMP_DIR}/tail.pos", "r")
      pos_file.pos = 0
      line_parts = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(pos_file.readline)
      waiting(5) {
        while line_parts[2].to_i(16) != 24
          sleep(0.1)
          pos_file.pos = 0
          line_parts = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(pos_file.readline)
        end
      }
      assert_equal(24, line_parts[2].to_i(16))
      d.instance_shutdown
    end

    def test_should_remove_deleted_file
      config = config_element("", "", {"format" => "none"})

      path = "#{TMP_DIR}/tail.txt"
      ino = 1
      pos = 1234
      File.open("#{TMP_DIR}/tail.pos", "wb") {|f|
        f.puts ("%s\t%016x\t%016x\n" % [path, pos, ino])
      }

      d = create_driver(config)
      d.run do
        pos_file = File.open("#{TMP_DIR}/tail.pos", "r")
        pos_file.pos = 0
        assert_equal([], pos_file.readlines)
      end
    end

    def test_should_mark_file_unwatched_after_limit_recently_modified_and_rotate_wait
      config = config_element("ROOT", "", {
          "path" => "#{TMP_DIR}/tail.txt*",
          "pos_file" => "#{TMP_DIR}/tail.pos",
          "tag" => "t1",
          "rotate_wait" => "1s",
          "refresh_interval" => "1s",
          "limit_recently_modified" => "1s",
          "read_from_head" => "true",
          "format" => "none",
          "follow_inodes" => "true",
      })

      d = create_driver(config, false)

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }
      target_info = create_target_info("#{TMP_DIR}/tail.txt")

      d.run(expect_emits: 1, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
      end


      Timecop.travel(Time.now + 10) do
        waiting(5) {
          # @pos will be reset as 0 when UNWATCHED_POSITION is specified.
          sleep 0.1 until d.instance.instance_variable_get(:@pf)[target_info].read_pos == 0
        }
      end

      assert_equal(0, d.instance.instance_variable_get(:@pf)[target_info].read_pos)

      d.instance_shutdown
    end

    def test_should_read_from_head_on_file_renaming_with_star_in_pattern
      config = config_element("ROOT", "", {
          "path" => "#{TMP_DIR}/tail.txt*",
          "pos_file" => "#{TMP_DIR}/tail.pos",
          "tag" => "t1",
          "rotate_wait" => "10s",
          "refresh_interval" => "1s",
          "limit_recently_modified" => "60s",
          "read_from_head" => "true",
          "format" => "none",
          "follow_inodes" => "true"
      })

      d = create_driver(config, false)

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d.run(expect_emits: 2, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
        FileUtils.move("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail.txt1")
      end

      events = d.events
      assert_equal(3, events.length)
      d.instance_shutdown
    end

    def test_should_not_read_from_head_on_rotation_when_watching_inodes
      config = COMMON_FOLLOW_INODE_CONFIG

      d = create_driver(config, false)

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d.run(expect_emits: 1, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
      end

      FileUtils.move("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail.txt1")
      Timecop.travel(Time.now + 10) do
        sleep 2
        events = d.events
        assert_equal(3, events.length)
      end

      d.instance_shutdown
    end

    def test_should_mark_file_unwatched_if_same_name_file_created_with_different_inode
      config = COMMON_FOLLOW_INODE_CONFIG

      d = create_driver(config, false)

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }
      target_info = create_target_info("#{TMP_DIR}/tail.txt")

      d.run(expect_emits: 2, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
        cleanup_file("#{TMP_DIR}/tail.txt")
        File.open("#{TMP_DIR}/tail.txt", "wb") {|f| f.puts "test4\n"}
      end

      new_target_info = create_target_info("#{TMP_DIR}/tail.txt")

      pos_file = d.instance.instance_variable_get(:@pf)

      waiting(10) {
        # @pos will be reset as 0 when UNWATCHED_POSITION is specified.
        sleep 0.1 until pos_file[target_info].read_pos == 0
      }
      new_position = pos_file[new_target_info].read_pos
      assert_equal(6, new_position)

      d.instance_shutdown
    end

    def test_should_close_watcher_after_rotate_wait
      now = Time.now
      config = COMMON_FOLLOW_INODE_CONFIG + config_element('', '', {"rotate_wait" => "1s", "limit_recently_modified" => "1s"})

      d = create_driver(config, false)
      d.instance.instance_eval do
        @total_rotated_file_metrics = Fluent::Plugin::LocalMetrics.new
        @total_rotated_file_metrics.configure(config_element('metrics', '', {}))
      end

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }
      target_info = create_target_info("#{TMP_DIR}/tail.txt")
      mock.proxy(Fluent::Plugin::TailInput::TailWatcher).new(target_info, anything, anything, true, true, anything, nil, anything, anything).once
      d.run(shutdown: false)
      assert d.instance.instance_variable_get(:@tails)[target_info]

      Timecop.travel(now + 10) do
        d.instance.instance_eval do
          sleep 0.1 until @tails[target_info] == nil
        end
        assert_nil d.instance.instance_variable_get(:@tails)[target_info]
      end
      d.instance_shutdown
    end

    def test_should_create_new_watcher_for_new_file_with_same_name
      now = Time.now
      config = COMMON_FOLLOW_INODE_CONFIG + config_element('', '', {"limit_recently_modified" => "2s"})

      d = create_driver(config, false)

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }
      path_ino = create_target_info("#{TMP_DIR}/tail.txt")

      d.run(expect_emits: 1, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
      end

      cleanup_file("#{TMP_DIR}/tail.txt")
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test3"
        f.puts "test4"
      }
      new_path_ino = create_target_info("#{TMP_DIR}/tail.txt")

      Timecop.travel(now + 10) do
        sleep 3
        d.instance.instance_eval do
          @tails[path_ino] == nil
          @tails[new_path_ino] != nil
        end
      end

      events = d.events

      assert_equal(5, events.length)

      d.instance_shutdown
    end

    def test_truncate_file_with_follow_inodes
      config = COMMON_FOLLOW_INODE_CONFIG

      d = create_driver(config, false)

      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d.run(expect_emits: 3, shutdown: false) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f| f.puts "test3\n"}
        sleep 2
        File.open("#{TMP_DIR}/tail.txt", "w+b") {|f| f.puts "test4\n"}
      end

      events = d.events
      assert_equal(4, events.length)
      assert_equal({"message" => "test1"}, events[0][2])
      assert_equal({"message" => "test2"}, events[1][2])
      assert_equal({"message" => "test3"}, events[2][2])
      assert_equal({"message" => "test4"}, events[3][2])
      d.instance_shutdown
    end

    # issue #3464
    def test_should_replace_target_info
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1\n"
      }
      target_info = create_target_info("#{TMP_DIR}/tail.txt")
      inodes = []

      config = config_element("ROOT", "", {
                                "path" => "#{TMP_DIR}/tail.txt*",
                                "pos_file" => "#{TMP_DIR}/tail.pos",
                                "tag" => "t1",
                                "refresh_interval" => "60s",
                                "read_from_head" => "true",
                                "format" => "none",
                                "rotate_wait" => "1s",
                                "follow_inodes" => "true"
                              })
      d = create_driver(config, false)
      d.run(timeout: 5) do
        while d.events.size < 1 do
          sleep 0.1
        end
        inodes = d.instance.instance_variable_get(:@tails).keys.collect do |key|
          key.ino
        end
        assert_equal([target_info.ino], inodes)

        cleanup_file("#{TMP_DIR}/tail.txt")
        File.open("#{TMP_DIR}/tail.txt", "wb") {|f| f.puts "test2\n"}

        while d.events.size < 2 do
          sleep 0.1
        end
        inodes = d.instance.instance_variable_get(:@tails).keys.collect do |key|
          key.ino
        end
        new_target_info = create_target_info("#{TMP_DIR}/tail.txt")
        assert_not_equal(target_info.ino, new_target_info.ino)
        assert_equal([new_target_info.ino], inodes)
      end
    end
  end

  sub_test_case "tail_path" do
    def test_tail_path_with_singleline
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test1"
        f.puts "test2"
      }

      d = create_driver(SINGLE_LINE_CONFIG + config_element("", "", { "path_key" => "path" }))

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
      end

      events = d.events
      assert_equal(true, events.length > 0)
      events.each do |emit|
        assert_equal("#{TMP_DIR}/tail.txt", emit[2]["path"])
      end
    end

    def test_tail_path_with_multiline_with_firstline
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      config = config_element("", "", {
                                "path_key" => "path",
                                "format" => "multiline",
                                "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/",
                                "format_firstline" => "/^[s]/"
                              })
      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "f test1"
          f.puts "s test2"
          f.puts "f test3"
          f.puts "f test4"
          f.puts "s test5"
          f.puts "s test6"
          f.puts "f test7"
          f.puts "s test8"
        }
      end

      events = d.events
      assert_equal(4, events.length)
      events.each do |emit|
        assert_equal("#{TMP_DIR}/tail.txt", emit[2]["path"])
      end
    end

    def test_tail_path_with_multiline_without_firstline
      File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

      config = config_element("", "", {
                                "path_key" => "path",
                                "format" => "multiline",
                                "format1" => "/(?<var1>foo \\d)\\n/",
                                "format2" => "/(?<var2>bar \\d)\\n/",
                                "format3" => "/(?<var3>baz \\d)/",
                              })
      d = create_driver(config)
      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "foo 1"
          f.puts "bar 1"
          f.puts "baz 1"
        }
      end

      events = d.events
      assert(events.length > 0)
      events.each do |emit|
        assert_equal("#{TMP_DIR}/tail.txt", emit[2]["path"])
      end
    end

    def test_tail_path_with_multiline_with_multiple_paths
      if ENV["APPVEYOR"] && Fluent.windows?
        omit "This testcase is unstable on AppVeyor."
      end
      files = ["#{TMP_DIR}/tail1.txt", "#{TMP_DIR}/tail2.txt"]
      files.each { |file| File.open(file, "wb") { |f| } }

      config = config_element("", "", {
                                "path" => "#{files[0]},#{files[1]}",
                                "path_key" => "path",
                                "tag" => "t1",
                                "format" => "multiline",
                                "format1" => "/^[s|f] (?<message>.*)/",
                                "format_firstline" => "/^[s]/"
                              })
      d = create_driver(config, false)
      d.run(expect_emits: 2) do
        files.each do |file|
          File.open(file, 'ab') { |f|
            f.puts "f #{file} line should be ignored"
            f.puts "s test1"
            f.puts "f test2"
            f.puts "f test3"
            f.puts "s test4"
          }
        end
      end

      events = d.events
      assert_equal(4, events.length)
      assert_equal(files, [events[0][2]["path"], events[1][2]["path"]].sort)
      # "test4" events are here because these events are flushed at shutdown phase
      assert_equal(files, [events[2][2]["path"], events[3][2]["path"]].sort)
    end
  end

  def test_limit_recently_modified
    now = Time.new(2010, 1, 2, 3, 4, 5)
    FileUtils.touch("#{TMP_DIR}/tail_unwatch.txt", mtime: (now - 3601))
    FileUtils.touch("#{TMP_DIR}/tail_watch1.txt", mtime: (now - 3600))
    FileUtils.touch("#{TMP_DIR}/tail_watch2.txt", mtime: now)

    config = config_element('', '', {
      'tag' => 'tail',
      'path' => "#{TMP_DIR}/*.txt",
      'format' => 'none',
      'limit_recently_modified' => '3600s'
    })

    expected_files = [
      create_target_info("#{TMP_DIR}/tail_watch1.txt"),
      create_target_info("#{TMP_DIR}/tail_watch2.txt")
    ]

    Timecop.freeze(now) do
      plugin = create_driver(config, false).instance
      assert_equal expected_files, plugin.expand_paths.values.sort_by { |path_ino| path_ino.path }
    end
  end

  def test_skip_refresh_on_startup
    FileUtils.touch("#{TMP_DIR}/tail.txt")
    config = config_element('', '', {
                              'format' => 'none',
                              'refresh_interval' => 1,
                              'skip_refresh_on_startup' => true
                            })
    d = create_driver(config)
    d.run(shutdown: false) {}
    assert_equal 0, d.instance.instance_variable_get(:@tails).keys.size
    # detect a file at first execution of in_tail_refresh_watchers timer
    waiting(5) { sleep 0.1 until d.instance.instance_variable_get(:@tails).keys.size == 1 }
    d.instance_shutdown
  end

  def test_ENOENT_error_after_setup_watcher
    path = "#{TMP_DIR}/tail.txt"
    FileUtils.touch(path)
    config = config_element('', '', {
                              'format' => 'none',
                            })
    d = create_driver(config)
    mock.proxy(d.instance).setup_watcher(anything, anything) do |tw|
      cleanup_file(path)
      tw
    end
    assert_nothing_raised do
      d.run(shutdown: false) {}
    end
    assert($log.out.logs.any?{|log| log.include?("stat() for #{path} failed with Errno::ENOENT. Drop tail watcher for now.\n") })
  ensure
    d.instance_shutdown if d && d.instance
  end

  def test_EACCES_error_after_setup_watcher
    omit "Cannot test with root user" if Process::UID.eid == 0
    path = "#{TMP_DIR}/noaccess/tail.txt"
    begin
      FileUtils.mkdir_p("#{TMP_DIR}/noaccess")
      FileUtils.chmod(0755, "#{TMP_DIR}/noaccess")
      FileUtils.touch(path)
      config = config_element('', '', {
                                'tag' => "tail",
                                'path' => path,
                                'format' => 'none',
                              })
      d = create_driver(config, false)
      mock.proxy(d.instance).setup_watcher(anything, anything) do |tw|
        FileUtils.chmod(0000, "#{TMP_DIR}/noaccess")
        tw
      end
      assert_nothing_raised do
        d.run(shutdown: false) {}
      end
      assert($log.out.logs.any?{|log| log.include?("stat() for #{path} failed with Errno::EACCES. Drop tail watcher for now.\n") })
    end
  ensure
    d.instance_shutdown if d && d.instance
    if File.exist?("#{TMP_DIR}/noaccess")
      FileUtils.chmod(0755, "#{TMP_DIR}/noaccess")
      FileUtils.rm_rf("#{TMP_DIR}/noaccess")
    end
  end unless Fluent.windows?

  def test_EACCES
    path = "#{TMP_DIR}/tail.txt"
    FileUtils.touch(path)
    config = config_element('', '', {
                              'format' => 'none',
                            })
    d = create_driver(config)
    mock.proxy(Fluent::FileWrapper).stat(path) do |stat|
      raise Errno::EACCES
    end.at_least(1)
    assert_nothing_raised do
      d.run(shutdown: false) {}
    end
    assert($log.out.logs.any?{|log| log.include?("expand_paths: stat() for #{path} failed with Errno::EACCES. Skip file.\n") })
  ensure
    d.instance_shutdown if d && d.instance
  end

  def test_shutdown_timeout
    path = "#{TMP_DIR}/tail.txt"
    File.open("#{TMP_DIR}/tail.txt", "wb") do |f|
      (1024 * 1024 * 5).times do
        f.puts "{\"test\":\"fizzbuzz\"}"
      end
    end

    config =
      CONFIG_READ_FROM_HEAD +
      config_element('', '', {
                              'format' => 'json',
                              'skip_refresh_on_startup' => true,
                            })
    d = create_driver(config)
    mock.proxy(d.instance).io_handler(anything, anything) do |io_handler|
      io_handler.shutdown_timeout = 0.5
      io_handler
    end

    start_time = Fluent::Clock.now
    assert_nothing_raised do
      d.run(expect_emits: 1)
    end

    elapsed = Fluent::Clock.now - start_time
    assert_true(elapsed > 0.5 && elapsed < 2.5)
  end
end
