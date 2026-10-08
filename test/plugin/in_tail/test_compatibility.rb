require_relative '../../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_tail'
require 'fluent/file_wrapper'
require 'flexmock/test_unit'
require 'securerandom'

# TailInput subclasses used to verify compatibility with overridden line
# processing methods.
module TailInputSubclasses
  class RecordingInput < Fluent::Plugin::TailInput
    attr_reader :calls

    def initialize
      super
      @calls = []
    end
  end

  class ReceiveLinesOverride < RecordingInput
    def receive_lines(lines, tail_watcher)
      @calls << [:receive_lines, lines]
      super
    end
  end

  class ReceiveLinesWithoutSuper < RecordingInput
    def receive_lines(lines, tail_watcher)
      @calls << [:receive_lines, lines]
      router.emit(tail_watcher.tag, Fluent::EventTime.now, { 'custom' => lines.join('|') })
      true
    end
  end

  # The first call simulates a full buffer, the following ones work normally.
  class ReceiveLinesFailsOnce < RecordingInput
    def receive_lines(lines, tail_watcher)
      @calls << [:receive_lines, lines]
      return false if @calls.size == 1
      super
    end
  end

  class FlushBufferOverride < RecordingInput
    def flush_buffer(tw, buf)
      # the native implementation chomps the buffer, keep a copy of it
      @calls << [:flush_buffer, buf.dup]
      super
    end
  end

  class ConvertLineToEventOverride < RecordingInput
    def convert_line_to_event(line, es, tail_watcher)
      @calls << [:convert_line_to_event, line]
      super
    end
  end

  class ParseSinglelineOverride < RecordingInput
    def parse_singleline(lines, tail_watcher)
      @calls << [:parse_singleline, lines]
      es = Fluent::MultiEventStream.new
      lines.each { |line|
        es.add(Fluent::EventTime.now, { 'custom' => line.chomp })
      }
      es
    end
  end

  class ParseSinglelineOverrideWithSuper < RecordingInput
    def parse_singleline(lines, tail_watcher)
      # the native implementation chomps each line, keep copies of them
      @calls << [:parse_singleline, lines.map(&:dup)]
      super
    end
  end

  # An override of #parse_multilines which calls the native implementation of
  # TailInput, which starts the timer to flush the line buffered by the multiline
  # mode.
  module ParseMultilinesOverrideWithSuper
    def parse_multilines(lines, tail_watcher)
      # the native implementation appends the buffered line to the first line,
      # so keep copies to check the arguments it got
      @calls << [:parse_multilines, lines.map(&:dup), tail_watcher.line_buffer_timer_flusher.line_buffer]
      super
    end
  end

  class ParseMultilinesOverride < RecordingInput
    include ParseMultilinesOverrideWithSuper
  end

  class PrivateParseSinglelineOverride < RecordingInput
    private

    def parse_singleline(lines, tail_watcher)
      @calls << [:parse_singleline, lines]
      super
    end
  end

  module PrependReceiveLines
    def receive_lines(lines, tail_watcher)
      @calls << [:receive_lines, lines]
      super
    end
  end

  class PrependedInput < RecordingInput
    prepend PrependReceiveLines
  end

  module IncludedConvertLineToEvent
    def convert_line_to_event(line, es, tail_watcher)
      @calls << [:convert_line_to_event, line]
      super
    end
  end

  class IncludedInput < RecordingInput
    include IncludedConvertLineToEvent
  end

  class SetupWatcherOverride < RecordingInput
    def setup_watcher(target_info, pe)
      @calls << [:setup_watcher, target_info.path]
      super
    end
  end

  # A plugin which builds the TailWatcher of a file by itself with the
  # TailWatcher::LineBufferTimerFlusher which TailInput built before
  # LineFeeder::FileFeed replaced it.
  class LegacyFlusherSetupWatcherOverride < RecordingInput
    def setup_watcher(target_info, pe)
      @calls << [:setup_watcher, target_info.path]
      line_buffer_timer_flusher = Fluent::Plugin::TailInput::TailWatcher::LineBufferTimerFlusher.new(
        log, @multiline_flush_interval, &method(:flush_buffer)
      )
      Fluent::Plugin::TailInput::TailWatcher.new(
        target_info, pe, log, true, @follow_inodes, method(:update_watcher),
        line_buffer_timer_flusher, method(:io_handler), @metrics
      )
    end
  end

  class LegacyFlusherFlushBufferOverride < LegacyFlusherSetupWatcherOverride
    def flush_buffer(tw, buf)
      # the native implementation chomps the buffer, keep a copy of it
      @calls << [:flush_buffer, buf.dup]
      super
    end
  end

  class LegacyFlusherParseMultilinesOverride < LegacyFlusherSetupWatcherOverride
    include ParseMultilinesOverrideWithSuper
  end

  # An override of #parse_multilines which doesn't call the native implementation
  # of TailInput, like the plugins which parse the multiline lines by themselves.
  # It keeps the line buffer of the file, but doesn't start the timer to flush it,
  # as the plugins which didn't call #reset_timer of the object keeping the line
  # buffer didn't.
  module ParseMultilinesOverrideWithoutSuper
    def parse_multilines(lines, tail_watcher)
      @calls << [:parse_multilines, lines.map(&:dup)]
      tail_watcher.line_buffer_timer_flusher.line_buffer = lines.join
      Fluent::MultiEventStream.new
    end
  end

  # The plugins which override both of #flush_buffer and #parse_multilines, to
  # check whether the timer of the file flushes the buffered line with #flush_buffer.
  class FlushBufferAndParseMultilinesOverride < FlushBufferOverride
    include ParseMultilinesOverrideWithSuper
  end

  class LegacyFlusherFlushBufferAndParseMultilinesOverride < LegacyFlusherFlushBufferOverride
    include ParseMultilinesOverrideWithSuper
  end

  class FlushBufferAndParseMultilinesOverrideWithoutSuper < FlushBufferOverride
    include ParseMultilinesOverrideWithoutSuper
  end

  class LegacyFlusherFlushBufferAndParseMultilinesOverrideWithoutSuper < LegacyFlusherFlushBufferOverride
    include ParseMultilinesOverrideWithoutSuper
  end
end


class TailInputCompatibilityTest < Test::Unit::TestCase
  include FlexMock::TestCase

  EX_ROTATE_WAIT = 0
  EX_FOLLOW_INODES = false

  SINGLE_LINE_CONFIG = config_element("", "", { "format" => "/(?<message>.*)/" })
  PARSE_MULTILINE_CONFIG = config_element(
    "", "", {},
    [config_element("parse", "", {
                      "@type" => "multiline",
                      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/",
                      "format_firstline" => "/^[s]/"
                    })
    ])

  def tmp_dir
    File.join(File.dirname(__FILE__), "..", "tmp", "tail#{ENV['TEST_ENV_NUMBER']}", SecureRandom.hex(10))
  end

  def setup
    Fluent::Test.setup
    @tmp_dir = tmp_dir
    cleanup_directory(@tmp_dir)
  end

  def teardown
    super
    cleanup_directory(@tmp_dir)
    Fluent::Engine.stop
  end

  def cleanup_directory(path)
    unless Dir.exist?(path)
      FileUtils.mkdir_p(path)
      return
    end

    FileUtils.remove_entry_secure(path, true)
  end

  def cleanup_file(path)
    FileUtils.remove_entry_secure(path, true)
  end

  def ex_config
    config_element("", "", {
                     "tag" => "tail",
                     "path" => "test/plugin/*/%Y/%m/%Y%m%d-%H%M%S.log,test/plugin/data/log/**/*.log",
                     "format" => "none",
                     "pos_file" => "#{@tmp_dir}/tail.pos",
                     "read_from_head" => true,
                     "refresh_interval" => 30,
                     "rotate_wait" => "#{EX_ROTATE_WAIT}s",
                     "follow_inodes" => "#{EX_FOLLOW_INODES}",
                   })
  end

  def base_config
    config_element("ROOT", "", {
                     "tag" => "t1",
                     "rotate_wait" => "2s",
                     "refresh_interval" => "1s"
                   }) + config_element("", "", { "path" => "#{@tmp_dir}/tail.txt" })
  end

  def common_config
    base_config + config_element("", "", { "pos_file" => "#{@tmp_dir}/tail.pos" })
  end

  def create_target_info(path)
    Fluent::Plugin::TailInput::TargetInfo.new(path, Fluent::FileWrapper.stat(path).ino)
  end

  def create_driver(conf = SINGLE_LINE_CONFIG, use_common_conf = true, klass: Fluent::Plugin::TailInput)
    config = use_common_conf ? common_config + conf : conf
    Fluent::Test::Driver::Input.new(klass).configure(config)
  end

  sub_test_case "receive_lines compatibility" do
    # Watcher used with a FileFeed.
    DummyWatcher = Struct.new("DummyWatcher", :tag, :file_feed)

    def create_dummy_watcher(plugin, tag = 'foo.bar.log')
      file_feed = plugin.instance_variable_get(:@line_feeder).new_file_feed
      DummyWatcher.new(tag, file_feed)
    end

    def test_tag
      d = create_driver(ex_config, false)
      d.run {}
      plugin = d.instance
      mock(plugin.router).emit_stream('tail', anything).once
      plugin.receive_lines(['foo', 'bar'], create_dummy_watcher(plugin))
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
      plugin.receive_lines(['foo', 'bar'], create_dummy_watcher(plugin))
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
      plugin.receive_lines(['foo', 'bar'], create_dummy_watcher(plugin))
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
      plugin.receive_lines(['foo', 'bar'], create_dummy_watcher(plugin))
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
      plugin.receive_lines(['foo', 'bar'], create_dummy_watcher(plugin))
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
      plugin.receive_lines(['foo', 'bar'], create_dummy_watcher(plugin))
    end
  end

  # Verify that overridden line processing methods remain on the call path.
  sub_test_case "line processing methods overridden by a subclass" do
    LineProcessingWatcher = Struct.new("LineProcessingWatcher", :tag, :path, :file_feed) do
      # Keep the legacy reader for compatibility.
      def line_buffer_timer_flusher
        file_feed
      end
    end

    def create_subclass_driver(conf, name)
      create_driver(conf + config_element("", "", { "read_from_head" => "true" }), true,
                    klass: TailInputSubclasses.const_get(name))
    end

    # Builds a watcher with a FileFeed.
    def create_line_processing_watcher(plugin, tag: 'foo.bar.log', path: nil, flush_interval: 4)
      file_feed = plugin.instance_variable_get(:@line_feeder).new_file_feed(flush_interval: flush_interval)
      LineProcessingWatcher.new(tag, path, file_feed)
    end

    def first_watcher(plugin)
      plugin.instance_variable_get(:@tails).values.first
    end

    # Returns only #flush_buffer calls.
    def flushed_calls(plugin)
      plugin.calls.select { |call| call.first == :flush_buffer }
    end

    def test_receive_lines_with_super
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :ReceiveLinesOverride)
      plugin = d.instance
      d.run do
        assert_true plugin.receive_lines(['foo', 'bar'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:receive_lines, ['foo', 'bar']]], plugin.calls)
      assert_equal(['t1', 't1'], d.events.map { |e| e[0] })
      assert_equal([{ 'message' => 'foo' }, { 'message' => 'bar' }], d.events.map { |e| e[2] })
    end

    def test_receive_lines_without_super
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :ReceiveLinesWithoutSuper)
      plugin = d.instance
      d.run do
        assert_true plugin.receive_lines(['foo', 'bar'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:receive_lines, ['foo', 'bar']]], plugin.calls)
      assert_equal([['foo.bar.log', { 'custom' => 'foo|bar' }]], d.events.map { |e| [e[0], e[2]] })
    end

    # IOHandler keeps the read position when receive_lines returns false, so
    # the lines must be emitted once the buffer is not full anymore.
    def test_receive_lines_returning_false_does_not_advance_the_position
      File.binwrite("#{@tmp_dir}/tail.txt", "")
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :ReceiveLinesFailsOnce)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        File.binwrite("#{@tmp_dir}/tail.txt", "test1\ntest2\n")

        tw.on_notify
        assert_equal 0, tw.pe.read_pos
        assert_true d.events.empty?

        tw.on_notify
        assert_equal 12, tw.pe.read_pos
      end
      assert_equal 2, plugin.calls.size
      assert_equal([{ 'message' => 'test1' }, { 'message' => 'test2' }], d.events.map { |e| e[2] })
    end

    # FileFeed flushes through the overridable TailInput#flush_buffer.
    def test_flush_buffer_override_is_called_by_the_file_feed
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :FlushBufferOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        refute_nil tw.file_feed
        assert_equal tw.file_feed, tw.line_buffer_timer_flusher
        tw.file_feed.line_buffer = "s incomplete\n"
        tw.file_feed.close(tw)
      end
      assert_equal([[:flush_buffer, "s incomplete\n"]], plugin.calls)
      assert_equal([{ 'message1' => 'incomplete' }], d.events.map { |e| e[2] })
    end

    def test_convert_line_to_event_override
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :ConvertLineToEventOverride)
      plugin = d.instance
      d.run do
        plugin.receive_lines(['foo', 'bar'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:convert_line_to_event, 'foo'], [:convert_line_to_event, 'bar']], plugin.calls)
      assert_equal([{ 'message' => 'foo' }, { 'message' => 'bar' }], d.events.map { |e| e[2] })
    end

    def test_parse_singleline_override
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :ParseSinglelineOverride)
      plugin = d.instance
      d.run do
        plugin.receive_lines(['foo', 'bar'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:parse_singleline, ['foo', 'bar']]], plugin.calls)
      assert_equal([{ 'custom' => 'foo' }, { 'custom' => 'bar' }], d.events.map { |e| e[2] })
    end

    def test_parse_multilines_override_with_super
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :ParseMultilinesOverride)
      plugin = d.instance
      d.run do
        watcher = create_line_processing_watcher(plugin, path: "#{@tmp_dir}/tail.txt")
        plugin.receive_lines(["s test1\n", "f test2\n"], watcher)
        assert_equal([[:parse_multilines, ["s test1\n", "f test2\n"], nil]], plugin.calls)
        assert_equal("s test1\nf test2\n", watcher.line_buffer_timer_flusher.line_buffer)
        assert_true d.events.empty?
      end
    end

    # #super comes back to the delegation to LineFeeder, so the native
    # implementation parses the lines and the events are built as usual.
    def test_parse_singleline_override_with_super
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :ParseSinglelineOverrideWithSuper)
      plugin = d.instance
      d.run do
        assert_true plugin.receive_lines(['foo', 'bar'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:parse_singleline, ['foo', 'bar']]], plugin.calls)
      assert_equal(['t1', 't1'], d.events.map { |e| e[0] })
      assert_equal([{ 'message' => 'foo' }, { 'message' => 'bar' }], d.events.map { |e| e[2] })
    end

    # LineFeeder converts a completed multiline block with the injected
    # convert_handler, so an override of #convert_line_to_event is called in the
    # multiline mode too. The native implementation chomps the buffered lines in
    # place, thus the recorded buffer has no trailing newline.
    def test_convert_line_to_event_override_with_super_in_multiline_mode
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :ConvertLineToEventOverride)
      plugin = d.instance
      d.run do
        watcher = create_line_processing_watcher(plugin, path: "#{@tmp_dir}/tail.txt")
        plugin.receive_lines(["s test1\n", "f test2\n", "s test3\n"], watcher)
        assert_equal([[:convert_line_to_event, "s test1\nf test2"]], plugin.calls)
        assert_equal("s test3\n", watcher.line_buffer_timer_flusher.line_buffer)
      end
      assert_equal([{ 'message1' => 'test1', 'message2' => 'test2' }], d.events.map { |e| e[2] })
    end

    # #super comes back to the delegation to LineFeeder, so the block completed
    # by the next firstline is emitted and the following lines stay buffered.
    def test_parse_multilines_override_with_super_emits_completed_events
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :ParseMultilinesOverride)
      plugin = d.instance
      d.run do
        watcher = create_line_processing_watcher(plugin, path: "#{@tmp_dir}/tail.txt")
        plugin.receive_lines(["s test1\n", "f test2\n", "s test3\n"], watcher)
        assert_equal([[:parse_multilines, ["s test1\n", "f test2\n", "s test3\n"], nil]], plugin.calls)
        assert_equal("s test3\n", watcher.line_buffer_timer_flusher.line_buffer)
      end
      assert_equal([{ 'message1' => 'test1', 'message2' => 'test2' }], d.events.map { |e| e[2] })
    end

    def test_private_parse_singleline_override
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :PrivateParseSinglelineOverride)
      plugin = d.instance
      d.run do
        plugin.receive_lines(['foo'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:parse_singleline, ['foo']]], plugin.calls)
      assert_equal([{ 'message' => 'foo' }], d.events.map { |e| e[2] })
    end

    def test_prepended_receive_lines_override
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :PrependedInput)
      plugin = d.instance
      d.run do
        plugin.receive_lines(['foo'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:receive_lines, ['foo']]], plugin.calls)
      assert_equal([{ 'message' => 'foo' }], d.events.map { |e| e[2] })
    end

    def test_included_convert_line_to_event_override
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :IncludedInput)
      plugin = d.instance
      d.run do
        plugin.receive_lines(['foo'], create_line_processing_watcher(plugin))
      end
      assert_equal([[:convert_line_to_event, 'foo']], plugin.calls)
      assert_equal([{ 'message' => 'foo' }], d.events.map { |e| e[2] })
    end

    def test_setup_watcher_override
      File.open("#{@tmp_dir}/tail.txt", "w") { |f| f.write("test1\n") }
      d = create_subclass_driver(SINGLE_LINE_CONFIG, :SetupWatcherOverride)
      d.run do
        assert_equal([[:setup_watcher, "#{@tmp_dir}/tail.txt"]], d.instance.calls)
        assert_equal(1, d.instance.instance_variable_get(:@tails).size)
      end
    end

    # A custom watcher using the deprecated LineBufferTimerFlusher still works.
    def test_setup_watcher_override_with_the_deprecated_line_buffer_timer_flusher
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\nf test2\ns test3\n")
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :LegacyFlusherFlushBufferOverride)
      plugin = d.instance
      d.run do
        assert_equal([[:setup_watcher, "#{@tmp_dir}/tail.txt"]], plugin.calls)

        tw = first_watcher(plugin)
        flusher = tw.line_buffer_timer_flusher
        assert_kind_of Fluent::Plugin::TailInput::TailWatcher::LineBufferTimerFlusher, flusher
        assert_equal flusher, tw.file_feed

        tw.on_notify

        assert_equal([{ 'message1' => 'test1', 'message2' => 'test2' }], d.events.map { |e| e[2] })
        assert_equal("s test3\n", flusher.line_buffer)
        assert_equal([], flushed_calls(plugin))
      end
    end

    # The deprecated flusher uses the overridable TailInput#flush_buffer.
    def test_deprecated_line_buffer_timer_flusher_flushes_the_pending_line_buffer
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      conf = PARSE_MULTILINE_CONFIG + config_element("", "", { "multiline_flush_interval" => "0" })
      d = create_subclass_driver(conf, :LegacyFlusherFlushBufferOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)
        assert_equal([], flushed_calls(plugin))

        tw.on_notify

        assert_equal([[:flush_buffer, "s test1\n"]], flushed_calls(plugin))
        assert_nil tw.line_buffer_timer_flusher.line_buffer
      end
      assert_equal([{ 'message1' => 'test1' }], d.events.map { |e| e[2] })
    end

    # The deprecated flusher has the same detach/close behavior as FileFeed.
    def test_detach_flushes_the_pending_line_buffer_of_the_deprecated_flusher_and_close_does_not
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :LegacyFlusherFlushBufferOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)

        tw.close
        assert_equal([], flushed_calls(plugin))
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)

        tw.detach
        assert_equal([[:flush_buffer, "s test1\n"]], flushed_calls(plugin))
        assert_nil tw.line_buffer_timer_flusher.line_buffer
      end
    end

    # A legacy parse_multilines override still reads the buffer from TailWatcher.
    def test_parse_multilines_override_with_the_deprecated_line_buffer_timer_flusher
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\nf test2\n")
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :LegacyFlusherParseMultilinesOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal([[:setup_watcher, "#{@tmp_dir}/tail.txt"],
                      [:parse_multilines, ["s test1\n", "f test2\n"], nil]], plugin.calls)
        assert_equal("s test1\nf test2\n", tw.line_buffer_timer_flusher.line_buffer)
        assert_true d.events.empty?
      end
    end

    # Calling super starts the FileFeed flush timer.
    def test_parse_multilines_override_with_super_starts_the_flush_timer_of_the_file_feed
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      conf = PARSE_MULTILINE_CONFIG + config_element("", "", { "multiline_flush_interval" => "0" })
      d = create_subclass_driver(conf, :FlushBufferAndParseMultilinesOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal([[:parse_multilines, ["s test1\n"], nil]], plugin.calls)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)
        assert_equal([], flushed_calls(plugin))

        tw.on_notify

        assert_equal([[:flush_buffer, "s test1\n"]], flushed_calls(plugin))
        assert_nil tw.line_buffer_timer_flusher.line_buffer
      end
    end

    # Skipping super does not start the FileFeed flush timer.
    def test_parse_multilines_override_without_super_does_not_start_the_flush_timer_of_the_file_feed
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      conf = PARSE_MULTILINE_CONFIG + config_element("", "", { "multiline_flush_interval" => "0" })
      d = create_subclass_driver(conf, :FlushBufferAndParseMultilinesOverrideWithoutSuper)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal([[:parse_multilines, ["s test1\n"]]], plugin.calls)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)

        tw.on_notify

        assert_equal([], flushed_calls(plugin))
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)
      end
    end

    # The legacy flusher timer is started by native #parse_multilines.
    def test_parse_multilines_override_with_super_starts_the_timer_of_the_deprecated_flusher
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      conf = PARSE_MULTILINE_CONFIG + config_element("", "", { "multiline_flush_interval" => "0" })
      d = create_subclass_driver(conf, :LegacyFlusherFlushBufferAndParseMultilinesOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)
        assert_equal([], flushed_calls(plugin))

        tw.on_notify

        assert_equal([[:flush_buffer, "s test1\n"]], flushed_calls(plugin))
        assert_nil tw.line_buffer_timer_flusher.line_buffer
      end
    end

    def test_parse_multilines_override_without_super_does_not_start_the_timer_of_the_deprecated_flusher
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      conf = PARSE_MULTILINE_CONFIG + config_element("", "", { "multiline_flush_interval" => "0" })
      d = create_subclass_driver(conf, :LegacyFlusherFlushBufferAndParseMultilinesOverrideWithoutSuper)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)

        tw.on_notify

        assert_equal([], flushed_calls(plugin))
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)
      end
    end

    # A plugin can build a TailWatcher with a FileFeed itself.
    def test_tailwatcher_and_file_feed_signatures
      path = "#{@tmp_dir}/tail.txt"
      File.open(path, "w") { |f| f.write("") }
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :FlushBufferOverride)
      plugin = d.instance
      d.run do
        file_feed = plugin.instance_variable_get(:@line_feeder).new_file_feed(flush_interval: 4)
        tw = Fluent::Plugin::TailInput::TailWatcher.new(
          create_target_info(path), nil, $log, true, false, nil, file_feed, nil, nil
        )
        assert_match(/tail\.txt\z/, tw.tag)
        assert_equal file_feed, tw.file_feed
        # The legacy reader still returns the FileFeed.
        assert_equal file_feed, tw.line_buffer_timer_flusher

        file_feed.line_buffer = "s incomplete\n"
        file_feed.close(tw)
      end
      assert_equal([[:flush_buffer, "s incomplete\n"]], plugin.calls)
    end

    # detach flushes the pending line buffer and close only closes the io.
    def test_detach_flushes_the_pending_line_buffer_and_close_does_not
      File.binwrite("#{@tmp_dir}/tail.txt", "s test1\n")
      d = create_subclass_driver(PARSE_MULTILINE_CONFIG, :FlushBufferOverride)
      plugin = d.instance
      d.run do
        tw = first_watcher(plugin)
        tw.on_notify
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)

        tw.close
        assert_equal([], plugin.calls)
        assert_equal("s test1\n", tw.line_buffer_timer_flusher.line_buffer)

        tw.detach
        assert_equal([[:flush_buffer, "s test1\n"]], plugin.calls)
        assert_nil tw.line_buffer_timer_flusher.line_buffer
      end
    end
  end
end
