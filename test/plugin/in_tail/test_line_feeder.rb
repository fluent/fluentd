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

  Watcher = Struct.new(:tag, :path, :line_buffer_timer_flusher)

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
                    parse_handler: nil, convert_handler: nil)
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
    )
  end

  def create_watcher(tag: 'foo.bar.log', path: '/tmp/foo.bar.log', line_buffer_timer_flusher: nil)
    Watcher.new(tag, path, line_buffer_timer_flusher)
  end

  def create_line_buffer_timer_flusher(line_feeder)
    Fluent::Plugin::TailInput::TailWatcher::LineBufferTimerFlusher.new($log, 30, &line_feeder.method(:flush_buffer))
  end

  def emitted_tags
    @router.emits.map { |tag, _, _| tag }
  end

  def emitted_records
    @router.emits.map { |_, _, record| record }
  end

  sub_test_case '#feed_lines' do
    data('without *': { tag: 'tail', tag_prefix: nil, tag_suffix: nil, expected_tag: 'tail' },
         'only *': { tag: '*', tag_prefix: '', tag_suffix: '', expected_tag: 'foo.bar.log' },
         'prefix': { tag: 'pre.*', tag_prefix: 'pre.', tag_suffix: '', expected_tag: 'pre.foo.bar.log' },
         'suffix': { tag: '*.post', tag_prefix: '', tag_suffix: '.post', expected_tag: 'foo.bar.log.post' },
         'prefix and suffix': { tag: 'pre.*.post', tag_prefix: 'pre.', tag_suffix: '.post', expected_tag: 'pre.foo.bar.log.post' },
         'extra * is ignored': { tag: 'pre.*.post*ignore', tag_prefix: 'pre.', tag_suffix: '.post', expected_tag: 'pre.foo.bar.log.post' })
    test 'emits events with the tag built for each watcher' do |data|
      line_feeder = create_feeder(tag: data[:tag], tag_prefix: data[:tag_prefix], tag_suffix: data[:tag_suffix])

      assert_true line_feeder.feed_lines(['foo', 'bar'], create_watcher)

      assert_equal([data[:expected_tag], data[:expected_tag]], emitted_tags)
      assert_equal([{ 'message' => 'foo' }, { 'message' => 'bar' }], emitted_records)
    end

    test 'adds the path_key field to records' do
      create_feeder(tag: 'tail', path_key: 'path').feed_lines(['foo'], create_watcher(path: '/tmp/foo.bar.log'))

      assert_equal({ 'message' => 'foo', 'path' => '/tmp/foo.bar.log' }, emitted_records[0])
    end

    test 'emits unmatched lines when the parser does not match' do
      create_feeder(tag: 'tail', emit_unmatched_lines: true, parser: NeverMatchingParser.new).feed_lines(['foo'], create_watcher)

      assert_equal([{ 'unmatched_line' => 'foo' }], emitted_records)
    end

    test 'does not emit anything when the parser does not match' do
      assert_true create_feeder(tag: 'tail', parser: NeverMatchingParser.new).feed_lines(['foo'], create_watcher)

      assert_equal([], @router.emits)
    end

    test 'returns false when the router raises BufferOverflowError' do
      @router.error_to_raise = Fluent::Plugin::Buffer::BufferOverflowError.new('buffer is full')

      assert_false create_feeder(tag: 'tail').feed_lines(['foo'], create_watcher)
    end

    test 'returns true when the router raises an error other than BufferOverflowError' do
      @router.error_to_raise = RuntimeError.new('unexpected error')

      assert_true create_feeder(tag: 'tail').feed_lines(['foo'], create_watcher)
    end

    test 'buffers multiline lines until the next firstline' do
      line_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new)
      timer_flusher = create_line_buffer_timer_flusher(line_feeder)

      line_feeder.feed_lines(["s test1\n", "f test2\n", "s test3\n"], create_watcher(line_buffer_timer_flusher: timer_flusher))

      assert_equal(['tail'], emitted_tags)
      assert_equal({ 'message' => "s test1\nf test2" }, emitted_records[0])
      assert_equal("s test3\n", timer_flusher.line_buffer)
    end
  end

  sub_test_case '#flush_buffer' do
    test 'emits the buffered line' do
      create_feeder(tag: 'tail', path_key: 'path').flush_buffer(create_watcher(path: '/tmp/foo.bar.log'), "foo\n")

      assert_equal('tail', emitted_tags[0])
      assert_equal({ 'message' => 'foo', 'path' => '/tmp/foo.bar.log' }, emitted_records[0])
    end

    test 'emits the buffered line as unmatched_line when the parser does not match' do
      create_feeder(tag: 'tail', path_key: 'path', emit_unmatched_lines: true, parser: NeverMatchingParser.new)
        .flush_buffer(create_watcher(path: '/tmp/foo.bar.log'), "incomplete line\n")

      assert_equal('tail', emitted_tags[0])
      assert_equal({ 'unmatched_line' => 'incomplete line', 'path' => '/tmp/foo.bar.log' }, emitted_records[0])
    end

    test 'does not emit anything when emit_unmatched_lines is disabled' do
      create_feeder(tag: 'tail', parser: NeverMatchingParser.new).flush_buffer(create_watcher, "incomplete line\n")

      assert_equal([], @router.emits)
    end
  end

  # TailInput binds its own line processing methods and passes them as
  # handlers, so that the methods overridden by a plugin which inherits
  # TailInput are used instead of the ones of LineFeeder.
  sub_test_case 'handlers injected by TailInput' do
    test 'uses the injected parse_handler' do
      parse_handler = ->(lines, _tw) {
        es = Fluent::MultiEventStream.new
        es.add(Fluent::EventTime.now, { 'custom' => lines.join('|') })
        es
      }
      line_feeder = create_feeder(tag: 'tail', parse_handler: parse_handler)

      assert_true line_feeder.feed_lines(['foo', 'bar'], create_watcher)

      assert_equal([{ 'custom' => 'foo|bar' }], emitted_records)
    end

    test 'uses the injected convert_handler' do
      converted = []
      convert_handler = ->(line, es, _tw) {
        converted << line
        es.add(Fluent::EventTime.now, { 'custom' => line })
      }
      line_feeder = create_feeder(tag: 'tail', convert_handler: convert_handler)

      assert_true line_feeder.feed_lines(['foo', 'bar'], create_watcher)

      assert_equal(['foo', 'bar'], converted)
      assert_equal([{ 'custom' => 'foo' }, { 'custom' => 'bar' }], emitted_records)
    end

    test 'uses the injected parse_handler in the multiline mode too' do
      parse_handler = ->(lines, tw) {
        tw.line_buffer_timer_flusher.line_buffer = lines.join
        Fluent::MultiEventStream.new
      }
      line_feeder = create_feeder(tag: 'tail', multiline_mode: true,
                                  parser: FirstlineParser.new, parse_handler: parse_handler)
      timer_flusher = create_line_buffer_timer_flusher(line_feeder)

      assert_true line_feeder.feed_lines(["s test1\n"], create_watcher(line_buffer_timer_flusher: timer_flusher))

      assert_equal([], @router.emits)
      assert_equal("s test1\n", timer_flusher.line_buffer)
    end

    test 'falls back to its own methods when no handler is injected' do
      line_feeder = create_feeder(tag: 'tail')

      assert_equal(line_feeder.method(:parse_singleline), line_feeder.instance_variable_get(:@parse_handler))
      assert_equal(line_feeder.method(:convert_line_to_event), line_feeder.instance_variable_get(:@convert_handler))

      multiline_feeder = create_feeder(tag: 'tail', multiline_mode: true, parser: FirstlineParser.new)

      assert_equal(multiline_feeder.method(:parse_multilines), multiline_feeder.instance_variable_get(:@parse_handler))
    end
  end
end
