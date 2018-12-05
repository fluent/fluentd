require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_tail'
require 'fluent/plugin/buffer'
require 'fluent/system_config'
require 'net/http'
require 'flexmock/test_unit'
require 'timecop'

class TailInputTest < Test::Unit::TestCase
  include FlexMock::TestCase

  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR, secure: true)
    if File.exist?(TMP_DIR)
      # ensure files are closed for Windows, on which deleted files
      # are still visible from filesystem
      GC.start(full_mark: true, immediate_mark: true, immediate_sweep: true)
      FileUtils.remove_entry_secure(TMP_DIR, true)
    end
    FileUtils.mkdir_p(TMP_DIR)
  end

  def teardown
    super
    Fluent::Engine.stop
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail#{ENV['TEST_ENV_NUMBER']}"

  CONFIG = config_element("ROOT", "", {
                            "path" => "#{TMP_DIR}/tail.txt",
                            "tag" => "t1",
                            "rotate_wait" => "2s"
                          })
  COMMON_CONFIG = CONFIG + config_element("", "", { "pos_file" => "#{TMP_DIR}/tail.pos" })
  CONFIG_READ_FROM_HEAD = config_element("", "", { "read_from_head" => true })
  CONFIG_ENABLE_WATCH_TIMER = config_element("", "", { "enable_watch_timer" => false })
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
    end

    data("empty" => config_element,
         "w/o @type" => config_element("", "", {}, [config_element("parse", "", {})]))
    test "w/o parse section" do |conf|
      assert_raise(Fluent::ConfigError) do
        create_driver(conf)
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
      msg = 'test' * 500 # in_tail reads 2048 bytes at once.

      d.run(expect_emits: 1) do
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts msg
          f.puts msg
        }
      end

      events = d.events
      assert_equal(true, events.length > 0)
      assert_equal({"message" => msg}, events[0][2])
      assert_equal({"message" => msg}, events[1][2])
      assert_equal(num_events, d.emit_count)
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
    EX_RORATE_WAIT = 0

    EX_CONFIG = config_element("", "", {
                                 "tag" => "tail",
                                 "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                                 "format" => "none",
                                 "pos_file" => "#{TMP_DIR}/tail.pos",
                                 "read_from_head" => true,
                                 "refresh_interval" => 30,
                                 "rotate_wait" => "#{EX_RORATE_WAIT}s",
                               })
    EX_PATHS = [
      'test/plugin/data/2010/01/20100102-030405.log',
      'test/plugin/data/log/foo/bar.log',
      'test/plugin/data/log/test.log'
    ]

    def test_expand_paths
      plugin = create_driver(EX_CONFIG, false).instance
      flexstub(Time) do |timeclass|
        timeclass.should_receive(:now).with_no_args.and_return(Time.new(2010, 1, 2, 3, 4, 5))
        assert_equal EX_PATHS, plugin.expand_paths.sort
      end

      # Test exclusion
      exclude_config = EX_CONFIG + config_element("", "", { "exclude_path" => %Q(["#{EX_PATHS.last}"]) })
      plugin = create_driver(exclude_config, false).instance
      assert_equal EX_PATHS - [EX_PATHS.last], plugin.expand_paths.sort
    end
  end

  def test_z_refresh_watchers
    plugin = create_driver(EX_CONFIG, false).instance
    sio = StringIO.new
    plugin.instance_eval do
      @pf = Fluent::Plugin::TailInput::PositionFile.parse(sio)
      @loop = Coolio::Loop.new
    end

    Timecop.freeze(2010, 1, 2, 3, 4, 5) do
      flexstub(Fluent::Plugin::TailInput::TailWatcher) do |watcherclass|
        EX_PATHS.each do |path|
          watcherclass.should_receive(:new).with(path, EX_RORATE_WAIT, Fluent::Plugin::TailInput::FilePositionEntry, any, true, true, 1000, any, any, any).once.and_return do
            flexmock('TailWatcher') { |watcher|
              watcher.should_receive(:attach).once
              watcher.should_receive(:unwatched=).zero_or_more_times
              watcher.should_receive(:line_buffer).zero_or_more_times
            }
          end
        end
        plugin.refresh_watchers
      end
    end

    plugin.instance_eval do
      @tails['test/plugin/data/2010/01/20100102-030405.log'].should_receive(:close).zero_or_more_times
    end

    Timecop.freeze(2010, 1, 2, 3, 4, 6) do
      flexstub(Fluent::Plugin::TailInput::TailWatcher) do |watcherclass|
        watcherclass.should_receive(:new).with('test/plugin/data/2010/01/20100102-030406.log', EX_RORATE_WAIT, Fluent::Plugin::TailInput::FilePositionEntry, any, true, true, 1000, any, any, any).once.and_return do
          flexmock('TailWatcher') do |watcher|
            watcher.should_receive(:attach).once
            watcher.should_receive(:unwatched=).zero_or_more_times
            watcher.should_receive(:line_buffer).zero_or_more_times
          end
        end
        plugin.refresh_watchers
      end

      flexstub(Fluent::Plugin::TailInput::TailWatcher) do |watcherclass|
        watcherclass.should_receive(:new).never
        plugin.refresh_watchers
      end
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
      events = d.events
      assert_equal(2, events.length)
      assert_equal({"message" => "test3"}, events[0][2])
      assert_equal({"message" => "test4"}, events[1][2])
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
end
