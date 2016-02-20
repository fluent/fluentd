require_relative '../helper'
require 'fluent/test'
require 'net/http'

class TailInputTest < Test::Unit::TestCase

  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR, secure: true)
    if File.exist?(TMP_DIR)
      # ensure files are closed for Windows, on which deleted files
      # are still visible from filesystem
      GC.start(full_mark: true, immediate_mark: true, immediate_sweep: true)
      FileUtils.remove_entry_secure(TMP_DIR)
    end
    FileUtils.mkdir_p(TMP_DIR)
  end

  def teardown
    super
    Fluent::Engine.stop
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail#{ENV['TEST_ENV_NUMBER']}"

  CONFIG = %[
    path #{TMP_DIR}/tail.txt
    tag t1
    rotate_wait 2s
  ]
  COMMON_CONFIG = CONFIG + %[
    pos_file #{TMP_DIR}/tail.pos
  ]
  CONFIG_READ_FROM_HEAD = %[
    read_from_head true
  ]
  SINGLE_LINE_CONFIG = %[
    format /(?<message>.*)/
  ]

  def create_driver(conf = SINGLE_LINE_CONFIG, use_common_conf = true)
    config = use_common_conf ? COMMON_CONFIG + conf : conf
    Fluent::Test::InputTestDriver.new(Fluent::NewTailInput).configure(config)
  end

  def test_configure
    d = create_driver
    assert_equal ["#{TMP_DIR}/tail.txt"], d.instance.paths
    assert_equal "t1", d.instance.tag
    assert_equal 2, d.instance.rotate_wait
    assert_equal "#{TMP_DIR}/tail.pos", d.instance.pos_file
    assert_equal 1000, d.instance.read_lines_limit
  end

  # TODO: Should using more better approach instead of sleep wait

  def test_emit
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "test3"
        f.puts "test4"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message" => "test3"}, emits[0][2])
    assert_equal({"message" => "test4"}, emits[1][2])
    assert(emits[0][1].is_a?(Fluent::EventTime))
    assert(emits[1][1].is_a?(Fluent::EventTime))
    assert_equal(1, d.emit_streams.size)
  end

  data('1' => [1, 2], '10' => [10, 1])
  def test_emit_with_read_lines_limit(data)
    limit, num_emits = data
    d = create_driver(CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG + "read_lines_limit #{limit}")
    msg = 'test' * 500 # in_tail reads 2048 bytes at once.

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts msg
        f.puts msg
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message" => msg}, emits[0][2])
    assert_equal({"message" => msg}, emits[1][2])
    assert_equal(num_emits, d.emit_streams.size)
  end

  def test_emit_with_read_from_head
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver(CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG)

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "test3"
        f.puts "test4"
      }
      sleep 1
    end

    emits = d.emits
    assert(emits.length > 0)
    assert_equal({"message" => "test1"}, emits[0][2])
    assert_equal({"message" => "test2"}, emits[1][2])
    assert_equal({"message" => "test3"}, emits[2][2])
    assert_equal({"message" => "test4"}, emits[3][2])
  end

  def test_rotate_file
    emits = sub_test_rotate_file(SINGLE_LINE_CONFIG)
    assert_equal(4, emits.length)
    assert_equal({"message" => "test3"}, emits[0][2])
    assert_equal({"message" => "test4"}, emits[1][2])
    assert_equal({"message" => "test5"}, emits[2][2])
    assert_equal({"message" => "test6"}, emits[3][2])
  end

  def test_rotate_file_with_read_from_head
    emits = sub_test_rotate_file(CONFIG_READ_FROM_HEAD + SINGLE_LINE_CONFIG)
    assert_equal(6, emits.length)
    assert_equal({"message" => "test1"}, emits[0][2])
    assert_equal({"message" => "test2"}, emits[1][2])
    assert_equal({"message" => "test3"}, emits[2][2])
    assert_equal({"message" => "test4"}, emits[3][2])
    assert_equal({"message" => "test5"}, emits[4][2])
    assert_equal({"message" => "test6"}, emits[5][2])
  end

  def test_rotate_file_with_write_old
    emits = sub_test_rotate_file(SINGLE_LINE_CONFIG) { |rotated_file|
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
    assert_equal(6, emits.length)
    assert_equal({"message" => "test3"}, emits[0][2])
    assert_equal({"message" => "test4"}, emits[1][2])
    assert_equal({"message" => "test7"}, emits[2][2])
    assert_equal({"message" => "test8"}, emits[3][2])
    assert_equal({"message" => "test5"}, emits[4][2])
    assert_equal({"message" => "test6"}, emits[5][2])
  end

  def test_rotate_file_with_write_old_and_no_new_file
    emits = sub_test_rotate_file(SINGLE_LINE_CONFIG) { |rotated_file|
      rotated_file.puts "test7"
      rotated_file.puts "test8"
      rotated_file.flush
    }
    assert_equal(4, emits.length)
    assert_equal({"message" => "test3"}, emits[0][2])
    assert_equal({"message" => "test4"}, emits[1][2])
    assert_equal({"message" => "test7"}, emits[2][2])
    assert_equal({"message" => "test8"}, emits[3][2])
  end

  def sub_test_rotate_file(config = nil)
    file = Fluent::FileWrapper.open("#{TMP_DIR}/tail.txt", "wb")
    file.puts "test1"
    file.puts "test2"
    file.flush

    d = create_driver(config)
    d.run do
      sleep 1

      file.puts "test3"
      file.puts "test4"
      file.flush
      sleep 1

      FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt")
      if block_given?
        yield file
        sleep 1
      else
        sleep 1
        File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }
        sleep 1

        File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
          f.puts "test5"
          f.puts "test6"
        }
        sleep 1
      end
    end

    d.run do
      sleep 1
    end

    d.emits
  ensure
    file.close if file
  end

  def test_lf
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f| }

    d = create_driver

    d.run do
      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.print "test3"
      }
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "test4"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message" => "test3test4"}, emits[0][2])
  end

  def test_whitespace
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f| }

    d = create_driver

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
        f.puts "    "		# 4 spaces
        f.puts "    4 spaces"
        f.puts "4 spaces    "
        f.puts "	"	# tab
        f.puts "	tab"
        f.puts "tab	"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message" => "    "}, emits[0][2])
    assert_equal({"message" => "    4 spaces"}, emits[1][2])
    assert_equal({"message" => "4 spaces    "}, emits[2][2])
    assert_equal({"message" => "	"}, emits[3][2])
    assert_equal({"message" => "	tab"}, emits[4][2])
    assert_equal({"message" => "tab	"}, emits[5][2])
  end

  # multiline mode test

  def test_multiline
    File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

    d = create_driver %[
      format multiline
      format1 /^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/
      format_firstline /^[s]/
    ]
    d.run do
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
      sleep 1

      emits = d.emits
      assert(emits.length == 3)
      assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, emits[0][2])
      assert_equal({"message1" => "test5"}, emits[1][2])
      assert_equal({"message1" => "test6", "message2" => "test7"}, emits[2][2])

      sleep 3
      emits = d.emits
      assert(emits.length == 3)
    end

    emits = d.emits
    assert(emits.length == 4)
    assert_equal({"message1" => "test8"}, emits[3][2])
  end

  def test_multiline_with_flush_interval
    File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

    d = create_driver %[
      format multiline
      format1 /^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/
      format_firstline /^[s]/
      multiline_flush_interval 2s
    ]

    assert_equal 2, d.instance.multiline_flush_interval

    d.run do
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
      sleep 1

      emits = d.emits
      assert(emits.length == 3)
      assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, emits[0][2])
      assert_equal({"message1" => "test5"}, emits[1][2])
      assert_equal({"message1" => "test6", "message2" => "test7"}, emits[2][2])

      sleep 3
      emits = d.emits
      assert(emits.length == 4)
      assert_equal({"message1" => "test8"}, emits[3][2])
    end
  end

  def test_multiline_with_multiple_formats
    File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

    d = create_driver %[
      format multiline
      format1 /^s (?<message1>[^\\n]+)\\n?/
      format2 /(f (?<message2>[^\\n]+)\\n?)?/
      format3 /(f (?<message3>.*))?/
      format_firstline /^[s]/
    ]
    d.run do
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
      sleep 1
    end

    emits = d.emits
    assert(emits.length > 0)
    assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, emits[0][2])
    assert_equal({"message1" => "test5"}, emits[1][2])
    assert_equal({"message1" => "test6", "message2" => "test7"}, emits[2][2])
    assert_equal({"message1" => "test8"}, emits[3][2])
  end

  def test_multilinelog_with_multiple_paths
    files = ["#{TMP_DIR}/tail1.txt", "#{TMP_DIR}/tail2.txt"]
    files.each { |file| File.open(file, "wb") { |f| } }

    d = create_driver(%[
      path #{files[0]},#{files[1]}
      tag t1
      format multiline
      format1 /^[s|f] (?<message>.*)/
      format_firstline /^[s]/
    ], false)
    d.run do
      files.each do |file|
        File.open(file, 'ab') { |f|
          f.puts "f #{file} line should be ignored"
          f.puts "s test1"
          f.puts "f test2"
          f.puts "f test3"
          f.puts "s test4"
        }
      end
      sleep 1
    end

    emits = d.emits
    assert_equal({"message" => "test1\nf test2\nf test3"}, emits[0][2])
    assert_equal({"message" => "test1\nf test2\nf test3"}, emits[1][2])
    # "test4" events are here because these events are flushed at shutdown phase
    assert_equal({"message" => "test4"}, emits[2][2])
    assert_equal({"message" => "test4"}, emits[3][2])
  end

  def test_multiline_without_firstline
    File.open("#{TMP_DIR}/tail.txt", "wb") { |f| }

    d = create_driver %[
      format multiline
      format1 /(?<var1>foo \\d)\\n/
      format2 /(?<var2>bar \\d)\\n/
      format3 /(?<var3>baz \\d)/
    ]
    d.run do
      File.open("#{TMP_DIR}/tail.txt", "ab") { |f|
        f.puts "foo 1"
        f.puts "bar 1"
        f.puts "baz 1"
        f.puts "foo 2"
        f.puts "bar 2"
        f.puts "baz 2"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(2, emits.length)
    assert_equal({"var1" => "foo 1", "var2" => "bar 1", "var3" => "baz 1"}, emits[0][2])
    assert_equal({"var1" => "foo 2", "var2" => "bar 2", "var3" => "baz 2"}, emits[1][2])
  end

  # * path test
  # TODO: Clean up tests
  EX_RORATE_WAIT = 0

  EX_CONFIG = %[
    tag tail
    path test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log
    format none
    pos_file #{TMP_DIR}/tail.pos
    read_from_head true
    refresh_interval 30
    rotate_wait #{EX_RORATE_WAIT}s
  ]
  EX_PATHS = [
    'test/plugin/data/2010/01/20100102-030405.log',
    'test/plugin/data/log/foo/bar.log',
    'test/plugin/data/log/test.log'
  ]

  def test_expand_paths
    plugin = create_driver(EX_CONFIG, false).instance
    stub(Time) do |timeclass|
      timeclass.now.returns(Time.new(2010, 1, 2, 3, 4, 5))
      assert_equal EX_PATHS, plugin.expand_paths.sort
    end

    # Test exclusion
    exclude_config = EX_CONFIG + "  exclude_path [\"#{EX_PATHS.last}\"]"
    plugin = create_driver(exclude_config, false).instance
    assert_equal EX_PATHS - [EX_PATHS.last], plugin.expand_paths.sort
  end

  def test_z_refresh_watchers
    plugin = create_driver(EX_CONFIG, false).instance
    sio = StringIO.new
    plugin.instance_eval do
      @pf = Fluent::NewTailInput::PositionFile.parse(sio)
      @loop = Coolio::Loop.new
    end

    stub(Time) do |timeclass|
      nows = [
        Time.new(2010, 1, 2, 3, 4, 5),
        Time.new(2010, 1, 2, 3, 4, 6),
        Time.new(2010, 1, 2, 3, 4, 7),
      ]
      timeclass.now do
        nows.shift
      end

      stub(Fluent::NewTailInput::TailWatcher) do |watcherclass|
        EX_PATHS.each do |path|
          new = watcherclass.new(path,
                                 EX_RORATE_WAIT,
                                 is_a(Fluent::TailInput::FilePositionEntry),
                                 anything,
                                 true,
                                 1000,
                                 anything,
                                 anything) do
            mock('TailWatcher') do |watcher|
              watcher.attach(anything).once
              watcher.__send__(:unwatched=, true).at_least(0)
              watcher.line_buffer.at_least(0)
              watcher.close.at_least(0)
            end
          end
          new.once
        end
        plugin.refresh_watchers
      end

      plugin.instance_eval do
        @tails['test/plugin/data/2010/01/20100102-030405.log'].close
      end

      stub(Fluent::NewTailInput::TailWatcher) do |watcherclass|
        new = watcherclass.new('test/plugin/data/2010/01/20100102-030406.log',
                               EX_RORATE_WAIT,
                               is_a(Fluent::TailInput::FilePositionEntry),
                               anything,
                               true,
                               1000,
                               anything,
                               anything) do
          mock('TailWatcher') do |watcher|
            watcher.attach(anything).once
            watcher.__send__(:unwatched=, true).at_least(0)
            watcher.line_buffer.at_least(0)
          end
        end
        new.once
        plugin.refresh_watchers
      end

      stub(Fluent::NewTailInput::TailWatcher) do |watcherclass|
        watcherclass.new.never
        plugin.refresh_watchers
      end
    end
  end

  DummyWatcher = Struct.new("DummyWatcher", :tag)

  def test_receive_lines
    plugin = create_driver(EX_CONFIG, false).instance
    stub(plugin.router) do |engineclass|
      engineclass.emit_stream('tail', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    config = %[
      tag pre.*
      path test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log
      format none
      read_from_head true
    ]
    plugin = create_driver(config, false).instance
    stub(plugin.router) do |engineclass|
      engineclass.emit_stream('pre.foo.bar.log', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    config = %[
      tag *.post
      path test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log
      format none
      read_from_head true
    ]
    plugin = create_driver(config, false).instance
    stub(plugin.router) do |engineclass|
      engineclass.emit_stream('foo.bar.log.post', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    config = %[
      tag pre.*.post
      path test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log
      format none
      read_from_head true
    ]
    plugin = create_driver(config, false).instance
    stub(plugin.router) do |engineclass|
      engineclass.emit_stream('pre.foo.bar.log.post', anything).once
      plugin.receive_lines(['foo', 'bar'], DummyWatcher.new('foo.bar.log'))
    end

    config = %[
      tag pre.*.post*ignore
      path test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log
      format none
      read_from_head true
    ]
    plugin = create_driver(config, false).instance
    stub(plugin.router) do |engineclass|
      engineclass.emit_stream('pre.foo.bar.log.post', anything).once
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
    config1 = %[
      tag t1
      path #{TMP_DIR}/non_existent_file.txt,#{TMP_DIR}/tail.txt
      format none
      rotate_wait 2s
      pos_file #{TMP_DIR}/tail.pos
    ]
    config2 = config1 + '  read_from_head true'
    [config1, config2].each do |config|
      d = create_driver(config, false)
      d.run do
        sleep 1
        File.open("#{TMP_DIR}/tail.txt", "ab") {|f|
          f.puts "test3"
          f.puts "test4"
        }
        sleep 1
      end
      emits = d.emits
      assert_equal(2, emits.length)
      assert_equal({"message" => "test3"}, emits[0][2])
      assert_equal({"message" => "test4"}, emits[1][2])
    end
  end
end
