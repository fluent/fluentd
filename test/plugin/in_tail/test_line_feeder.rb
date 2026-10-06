require_relative '../../helper'

require 'fluent/plugin/in_tail'
require 'fluent/plugin/parser_none'

class IntailLineFeederTest < Test::Unit::TestCase
  # A parser which never matches, like parser plugins which don't implement
  # the `unmatched_lines` feature. Such parsers yield (nil, nil) to the caller.
  class NeverMatchingParser
    def parse(text, &block)
      block.call(nil, nil)
      nil
    end
  end

  # A parser which behaves like parser_multiline configured with format_firstline.
  class FirstlineParser
    def has_firstline?
      true
    end

    def firstline?(text)
      text.start_with?('s ')
    end

    def parse(text, &block)
      block.call(Fluent::EventTime.now, { 'message' => text })
    end
  end

  # A parser which behaves like parser_multiline without format_firstline. It
  # completes a record only when the accumulated lines contain 'end'.
  class IncompleteUntilEndParser
    def has_firstline?
      false
    end

    def parse(text, &block)
      if text.include?('end')
        block.call(Fluent::EventTime.now, { 'message' => text })
      else
        block.call(nil, nil)
      end
    end
  end

  class RecordingRouter
    attr_reader :emits
    attr_accessor :error_to_raise

    def initialize
      @emits = []
      @error_to_raise = nil
    end

    def emit(tag, time, record)
      @emits << [tag, time, record]
    end

    def emit_stream(tag, es)
      raise @error_to_raise if @error_to_raise

      es.each { |time, record| @emits << [tag, time, record] }
    end
  end

  # The watcher of the file which the lines are fed with. LineFeeder reads the
  # tag and the path of the file from it, and the line buffer of the multiline
  # mode from the FileFeed of it.
  Watcher = Struct.new(:tag, :path, :file_feed)

  # The watcher of a file which keeps its state in the deprecated
  # TailWatcher::LineBufferTimerFlusher, which a plugin overriding
  # TailInput#setup_watcher may build and pass to TailWatcher by itself.
  LegacyWatcher = Struct.new(:tag, :path, :line_buffer_timer_flusher) do
    # TailWatcher answers both of them with the object keeping the line buffer.
    def file_feed
      line_buffer_timer_flusher
    end
  end

  FILE_PATH = '/tmp/foo.bar.log'

  def setup
    Fluent::Test.setup
    @router = RecordingRouter.new
  end

  def create_parser(type = 'none')
    Fluent::Plugin::PARSER_REGISTRY.lookup(type).new.tap { |parser|
      parser.configure(config_element('parse', '', {}))
    }
  end

  def create_feeder(tag:, tag_prefix: nil, tag_suffix: nil, path_key: nil,
                    emit_unmatched_lines: false, multiline_mode: false, parser: nil,
                    parse_handler: nil, convert_handler: nil, flush_handler: nil)
    Fluent::Plugin::TailInput::LineFeeder.new(
      parser: parser || create_parser,
      router_provider: -> { @router },
      log: $log,
      tag: tag,
      tag_prefix: tag_prefix,
      tag_suffix: tag_suffix,
      path_key: path_key,
      emit_unmatched_lines: emit_unmatched_lines,
      multiline_mode: multiline_mode,
      parse_handler: parse_handler,
      convert_handler: convert_handler,
      flush_handler: flush_handler,
    )
  end

  # Builds a FileFeed of a file and the watcher of it, which are used together as
  # TailInput does.
  def create_file_feed(line_feeder, flush_interval: nil, path: FILE_PATH)
    file_feed = line_feeder.new_file_feed(path: path, flush_interval: flush_interval)
    [file_feed, Watcher.new('foo.bar.log', path, file_feed)]
  end

  def emitted_tags
    @router.emits.map { |tag, _, _| tag }
  end

  def emitted_records
    @router.emits.map { |_, _, record| record }
  end

  sub_test_case 'FileFeed#feed_lines' do
    data('without *': { tag: 'tail', tag_prefix: nil, tag_suffix: nil, expected_tag: 'tail' },
         'only *': { tag: '*', tag_prefix: '', tag_suffix: '', expected_tag: 'foo.bar.log' },
         'prefix': { tag: 'pre.*', tag_prefix: 'pre.', tag_suffix: '', expected_tag: 'pre.foo.bar.log' },
         'suffix': { tag: '*.post', tag_prefix: '', tag_suffix: '.post', expected_tag: 'foo.bar.log.post' },
         'prefix and suffix': { tag: 'pre.*.post', tag_prefix: 'pre.', tag_suffix: '.post', expected_tag: 'pre.foo.bar.log.post' },
         'extra * is ignored': { tag: 'pre.*.post*ignore', tag_prefix: 'pre.', tag_suffix: '.post', expected_tag: 'pre.foo.bar.log.post' })
    test 'emits events with the tag built for each watcher' do |data|
      file_feed, tw = create_file_feed(create_feeder(tag: data[:tag], tag_prefix: data[:tag_prefix], tag_suffix: data[:tag_suffix]))

      assert_true file_feed.feed_lines(['foo', 'bar'], tw)

      assert_equal([data[:expected_tag], data[:expected_tag]], emitted_tags)
      assert_equal([{ 'message' => 'foo' }, { 'message' => 'bar' }], emitted_records)
    end

    test 'adds the path_key field to records' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', path_key: 'path'))

      file_feed.feed_lines(['foo'], tw)

      assert_equal({ 'message' => 'foo', 'path' => FILE_PATH }, emitted_records[0])
    end

    test 'emits unmatched lines when the parser does not match' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', emit_unmatched_lines: true, parser: NeverMatchingParser.new))

      file_feed.feed_lines(['foo'], tw)

      assert_equal([{ 'unmatched_line' => 'foo' }], emitted_records)
    end

    test 'does not emit anything when the parser does not match' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', parser: NeverMatchingParser.new))

      assert_true file_feed.feed_lines(['foo'], tw)

      assert_equal([], @router.emits)
    end

    test 'returns false when the router raises BufferOverflowError' do
      @router.error_to_raise = Fluent::Plugin::Buffer::BufferOverflowError.new('buffer is full')
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail'))

      assert_false file_feed.feed_lines(['foo'], tw)
    end

    test 'returns true when the router raises an error other than BufferOverflowError' do
      @router.error_to_raise = RuntimeError.new('unexpected error')
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail'))

      assert_true file_feed.feed_lines(['foo'], tw)
    end

    test 'buffers multiline lines until the next firstline' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new))

      file_feed.feed_lines(["s test1\n", "f test2\n", "s test3\n"], tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => "s test1\nf test2" }, emitted_records[0])
      assert_equal("s test3\n", file_feed.line_buffer)
    end

    test 'keeps the buffered line between the feeds of the file' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new))

      file_feed.feed_lines(["s test1\n", "f test2\n"], tw)
      file_feed.feed_lines(["f test3\n", "s test4\n"], tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => "s test1\nf test2\nf test3" }, emitted_records[0])
      assert_equal("s test4\n", file_feed.line_buffer)
    end

    test 'passes the watcher of the file to the parser' do
      watchers = []
      parse_handler = ->(lines, tw) {
        watchers << tw
        Fluent::MultiEventStream.new
      }
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', parse_handler: parse_handler))

      file_feed.feed_lines(['foo'], tw)

      assert_equal([tw], watchers)
    end

    # The deadline to flush the buffered line starts at the beginning of the
    # parsing, like TailWatcher::LineBufferTimerFlusher did, so a slow parser does
    # not postpone the flush of the buffered line.
    test 'starts the flush timer before the lines are parsed' do
      parse_handler = ->(lines, tw) {
        sleep 0.1
        tw.file_feed.line_buffer = lines.join
        Fluent::MultiEventStream.new
      }
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new,
                                                     parse_handler: parse_handler),
                                       flush_interval: 0.01)

      file_feed.feed_lines(["s test1\n"], tw)
      file_feed.on_notify(tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => 's test1' }, emitted_records[0])
      assert_nil file_feed.line_buffer
    end
  end

  sub_test_case 'FileFeed#on_notify' do
    test 'flushes the buffered line when the flush interval has passed' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new), flush_interval: 0)
      file_feed.feed_lines(["s test1\n", "f test2\n"], tw)

      assert_equal([], @router.emits)

      file_feed.on_notify(tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => "s test1\nf test2" }, emitted_records[0])
      assert_nil file_feed.line_buffer
    end

    test 'does not flush the buffered line before the flush interval' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new), flush_interval: 30)
      file_feed.feed_lines(["s test1\n", "f test2\n"], tw)

      file_feed.on_notify(tw)

      assert_equal([], @router.emits)
      assert_equal("s test1\nf test2\n", file_feed.line_buffer)
    end

    test 'does nothing when no line was fed' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new), flush_interval: 0)

      file_feed.on_notify(tw)

      assert_equal([], @router.emits)
    end

    test 'does not flush the buffered line when the flush interval is not set' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new), flush_interval: nil)
      file_feed.feed_lines(["s test1\n", "f test2\n"], tw)

      file_feed.on_notify(tw)

      assert_equal([], @router.emits)
      assert_equal("s test1\nf test2\n", file_feed.line_buffer)
    end

    test 'does not set the flush interval of a multiline parser without a firstline' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: IncompleteUntilEndParser.new), flush_interval: 0)

      file_feed.feed_lines(["test1\n", "test2\n"], tw)
      file_feed.on_notify(tw)

      assert_equal([], @router.emits)
      assert_equal("test1\ntest2\n", file_feed.line_buffer)
    end
  end

  sub_test_case 'FileFeed#close' do
    test 'flushes the buffered line' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new))

      file_feed.feed_lines(["s test1\n", "f test2\n"], tw)
      file_feed.close(tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => "s test1\nf test2" }, emitted_records[0])
      assert_nil file_feed.line_buffer
    end

    test 'does nothing when no line was buffered' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new))

      file_feed.close(tw)

      assert_equal([], @router.emits)
    end
  end

  sub_test_case 'FileFeed#flush_buffer' do
    test 'emits the buffered line' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', path_key: 'path'))

      file_feed.flush_buffer("foo\n", tw)

      assert_equal('tail', emitted_tags[0])
      assert_equal({ 'message' => 'foo', 'path' => FILE_PATH }, emitted_records[0])
    end

    test 'emits the buffered line as unmatched_line when the parser does not match' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', path_key: 'path', emit_unmatched_lines: true, parser: NeverMatchingParser.new))

      file_feed.flush_buffer("incomplete line\n", tw)

      assert_equal('tail', emitted_tags[0])
      assert_equal({ 'unmatched_line' => 'incomplete line', 'path' => FILE_PATH }, emitted_records[0])
    end

    test 'does not emit anything when emit_unmatched_lines is disabled' do
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', parser: NeverMatchingParser.new))

      file_feed.flush_buffer("incomplete line\n", tw)

      assert_equal([], @router.emits)
    end
  end

  # A plugin which overrides TailInput#setup_watcher may build the deprecated
  # TailWatcher::LineBufferTimerFlusher and pass it to TailWatcher by itself.
  # LineFeeder#feed_lines feeds the lines of such a watcher, keeping the line
  # buffer and the timer of that flusher.
  sub_test_case 'LineFeeder#feed_lines with the deprecated LineBufferTimerFlusher' do
    def create_legacy_flusher_watcher(flush_interval:, path: FILE_PATH, &flush_method)
      flusher = Fluent::Plugin::TailInput::TailWatcher::LineBufferTimerFlusher.new($log, flush_interval, &flush_method)
      [flusher, LegacyWatcher.new('foo.bar.log', path, flusher)]
    end

    test 'parses and emits the lines and keeps the buffered line in the flusher' do
      line_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new)
      flusher, tw = create_legacy_flusher_watcher(flush_interval: 4) { |_tw, _buf| nil }

      assert_true line_feeder.feed_lines(["s test1\n", "f test2\n", "s test3\n"], tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => "s test1\nf test2" }, emitted_records[0])
      assert_equal("s test3\n", flusher.line_buffer)
    end

    test 'flushes the buffered line with the flush method of the flusher when the flush interval has passed' do
      flushed = []
      line_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new)
      flusher, tw = create_legacy_flusher_watcher(flush_interval: 0) { |tw, buf| flushed << [tw.path, buf] }

      line_feeder.feed_lines(["s test1\n", "f test2\n"], tw)
      flusher.on_notify(tw)

      assert_equal([[FILE_PATH, "s test1\nf test2\n"]], flushed)
      assert_nil flusher.line_buffer
    end

    test 'does not start the timer of a multiline parser without a firstline' do
      flushed = []
      line_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: IncompleteUntilEndParser.new)
      flusher, tw = create_legacy_flusher_watcher(flush_interval: 0) { |_tw, buf| flushed << buf }

      line_feeder.feed_lines(["test1\n", "test2\n"], tw)
      flusher.on_notify(tw)

      assert_equal([], flushed)
      assert_equal("test1\ntest2\n", flusher.line_buffer)
    end

    test 'starts the flush timer before the lines are parsed' do
      flushed = []
      parse_handler = ->(lines, tw) {
        sleep 0.1
        tw.line_buffer_timer_flusher.line_buffer = lines.join
        Fluent::MultiEventStream.new
      }
      line_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new,
                                  parse_handler: parse_handler)
      flusher, tw = create_legacy_flusher_watcher(flush_interval: 0.01) { |_tw, buf| flushed << buf }

      line_feeder.feed_lines(["s test1\n"], tw)
      flusher.on_notify(tw)

      assert_equal(["s test1\n"], flushed)
    end

    test 'emits the events when the watcher has no flusher' do
      line_feeder = create_feeder(tag: 'tail')
      tw = LegacyWatcher.new('foo.bar.log', FILE_PATH, nil)

      assert_true line_feeder.feed_lines(['foo'], tw)

      assert_equal(['tail'], emitted_tags)
      assert_equal([{ 'message' => 'foo' }], emitted_records)
    end
  end

  # TailInput binds its own line processing methods and passes them as handlers,
  # so that the methods overridden by a plugin which inherits TailInput are used
  # instead of the ones of LineFeeder.
  sub_test_case 'handlers injected by TailInput' do
    test 'uses the injected parse_handler' do
      parse_handler = ->(lines, _tw) {
        es = Fluent::MultiEventStream.new
        es.add(Fluent::EventTime.now, { 'custom' => lines.join('|') })
        es
      }
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', parse_handler: parse_handler))

      assert_true file_feed.feed_lines(['foo', 'bar'], tw)

      assert_equal([{ 'custom' => 'foo|bar' }], emitted_records)
    end

    test 'uses the injected convert_handler' do
      converted = []
      convert_handler = ->(line, es, _tw) {
        converted << line
        es.add(Fluent::EventTime.now, { 'custom' => line })
      }
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', convert_handler: convert_handler))

      assert_true file_feed.feed_lines(['foo', 'bar'], tw)

      assert_equal(['foo', 'bar'], converted)
      assert_equal([{ 'custom' => 'foo' }, { 'custom' => 'bar' }], emitted_records)
    end

    test 'uses the injected parse_handler in the multiline mode too' do
      parse_handler = ->(lines, tw) {
        tw.file_feed.line_buffer = lines.join
        Fluent::MultiEventStream.new
      }
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true,
                                                     parser: FirstlineParser.new, parse_handler: parse_handler))

      assert_true file_feed.feed_lines(["s test1\n"], tw)

      assert_equal([], @router.emits)
      assert_equal("s test1\n", file_feed.line_buffer)
    end

    test 'uses the injected flush_handler when the buffered line is flushed' do
      flushed = []
      flush_handler = ->(tw, buf) { flushed << [tw.path, buf] }
      file_feed, tw = create_file_feed(create_feeder(tag: 'tail', multiline_mode: true,
                                                     parser: FirstlineParser.new, flush_handler: flush_handler),
                                       flush_interval: 0)

      file_feed.feed_lines(["s test1\n", "f test2\n"], tw)
      file_feed.close(tw)

      assert_equal([[FILE_PATH, "s test1\nf test2\n"]], flushed)
    end

    test 'falls back to its own methods when no handler is injected' do
      line_feeder = create_feeder(tag: 'tail')

      assert_equal(line_feeder.method(:parse_singleline), line_feeder.instance_variable_get(:@parse_handler))
      assert_equal(line_feeder.method(:convert_line_to_event), line_feeder.instance_variable_get(:@convert_handler))
      assert_equal(line_feeder.method(:flush_buffer), line_feeder.instance_variable_get(:@flush_handler))

      multiline_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new)

      assert_equal(multiline_feeder.method(:parse_multilines), multiline_feeder.instance_variable_get(:@parse_handler))
    end
  end
end
