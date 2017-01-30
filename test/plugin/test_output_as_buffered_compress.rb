require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/plugin/compressable'
require 'fluent/event'

require 'timeout'

module FluentPluginOutputAsBufferedCompressTest
  class DummyBareOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
  end

  class DummyAsyncOutput < DummyBareOutput
    def initialize
      super
      @format = @write = nil
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end

  class DummyAsyncOutputWithFormat < DummyBareOutput
    def initialize
      super
      @format = nil
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
  end
end

class BufferedOutputCompressTest < Test::Unit::TestCase
  include Fluent::Plugin::Compressable

  def create_output(type=:async)
    case type
    when :async then FluentPluginOutputAsBufferedCompressTest::DummyAsyncOutput.new
    when :async_with_format then FluentPluginOutputAsBufferedCompressTest::DummyAsyncOutputWithFormat.new
    else
      raise ArgumentError, "unknown type: #{type}"
    end
  end

  def waiting(seconds)
    begin
      Timeout.timeout(seconds) do
        yield
      end
    rescue Timeout::Error
      STDERR.print(*@i.log.out.logs)
      raise
    end
  end

  def dummy_event_stream
    Fluent::ArrayEventStream.new(
      [
        [event_time('2016-04-13 18:33:00'), { 'name' => 'moris', 'age' => 36, 'message' => 'data1' }],
        [event_time('2016-04-13 18:33:13'), { 'name' => 'moris', 'age' => 36, 'message' => 'data2' }],
        [event_time('2016-04-13 18:33:32'), { 'name' => 'moris', 'age' => 36, 'message' => 'data3' }],
      ]
    )
  end

  TMP_DIR = File.expand_path('../../tmp/test_output_as_buffered_compress', __FILE__)

  setup do
    FileUtils.rm_r TMP_DIR rescue nil
    FileUtils.mkdir_p TMP_DIR
  end

  teardown do
    if @i
      @i.stop unless @i.stopped?
      @i.before_shutdown unless @i.before_shutdown?
      @i.shutdown unless @i.shutdown?
      @i.after_shutdown unless @i.after_shutdown?
      @i.close unless @i.closed?
      @i.terminate unless @i.terminated?
    end
  end

  data(
    handle_simple_stream: config_element('buffer', '', { 'flush_interval' => 1, 'compress' => 'gzip' }),
    handle_stream_with_standard_format:  config_element('buffer', 'tag', { 'flush_interval' => 1, 'compress' => 'gzip' }),
    handle_simple_stream_and_file_chunk: config_element('buffer', '', { '@type' => 'file', 'path' => File.join(TMP_DIR,'test.*.log'), 'flush_interval' => 1, 'compress' => 'gzip' }),
    handle_stream_with_standard_format_and_file_chunk:  config_element('buffer', 'tag', { '@type' => 'file', 'path' => File.join(TMP_DIR,'test.*.log'), 'flush_interval' => 1, 'compress' => 'gzip' }),
  )
  test 'call a standard format when output plugin adds data to chunk' do |buffer_config|
    @i = create_output(:async)
    @i.configure(config_element('ROOT','', {}, [buffer_config]))
    @i.start
    @i.after_start

    io = StringIO.new
    es = dummy_event_stream
    expected = es.map { |e| e }
    compressed_data = ''

    assert_equal :gzip, @i.buffer.compress

    @i.register(:write) do |c|
      compressed_data = c.instance_variable_get(:@chunk)
      if compressed_data.is_a?(File)
        compressed_data.seek(0, IO::SEEK_SET)
        compressed_data = compressed_data.read
      end
      c.write_to(io)
    end

    @i.emit_events('tag', es)
    @i.enqueue_thread_wait
    @i.flush_thread_wakeup
    waiting(4) { Thread.pass until io.size > 0 }

    assert_equal expected, Fluent::MessagePackEventStream.new(decompress(compressed_data)).map { |t, r| [t, r] }
    assert_equal expected, Fluent::MessagePackEventStream.new(io.string).map { |t, r| [t, r] }
  end

  data(
    handle_simple_stream: config_element('buffer', '', { 'flush_interval' => 1, 'compress' => 'gzip' }),
    handle_stream_with_custom_format:  config_element('buffer', 'tag', { 'flush_interval' => 1, 'compress' => 'gzip' }),
    handle_simple_stream_and_file_chunk: config_element('buffer', '', { '@type' => 'file', 'path' => File.join(TMP_DIR,'test.*.log'), 'flush_interval' => 1, 'compress' => 'gzip' }),
    handle_stream_with_custom_format_and_file_chunk:  config_element('buffer', 'tag', { '@type' => 'file', 'path' => File.join(TMP_DIR,'test.*.log'), 'flush_interval' => 1, 'compress' => 'gzip' }),
  )
  test 'call a custom format when output plugin adds data to chunk' do |buffer_config|
    @i = create_output(:async_with_format)
    @i.configure(config_element('ROOT','', {}, [buffer_config]))
    @i.start
    @i.after_start

    io = StringIO.new
    es = dummy_event_stream
    expected = es.map { |e| "#{e[1]}\n" }.join # e[1] is record
    compressed_data = ''

    assert_equal :gzip, @i.buffer.compress

    @i.register(:format) { |tag, time, record| "#{record}\n" }
    @i.register(:write) { |c|
      compressed_data = c.instance_variable_get(:@chunk)
      if compressed_data.is_a?(File)
        compressed_data.seek(0, IO::SEEK_SET)
        compressed_data = compressed_data.read
      end
      c.write_to(io)
    }

    @i.emit_events('tag', es)
    @i.enqueue_thread_wait
    @i.flush_thread_wakeup
    waiting(4) { sleep 0.1 until io.size > 0 }

    assert_equal expected, decompress(compressed_data)
    assert_equal expected, io.string
  end
end
