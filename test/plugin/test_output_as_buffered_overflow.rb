require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/event'

require 'json'
require 'time'
require 'timeout'
require 'timecop'

module FluentPluginOutputAsBufferedOverflowTest
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
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end
end

class BufferedOutputOverflowTest < Test::Unit::TestCase
  def create_output
    FluentPluginOutputAsBufferedOverflowTest::DummyAsyncOutput.new
  end
  def create_metadata(timekey: nil, tag: nil, variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end
  def waiting(seconds)
    begin
      Timeout.timeout(seconds) do
        yield
      end
    rescue Timeout::Error
      logs = @i.log.out.logs
      STDERR.print(*logs)
      raise
    end
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
    Timecop.return
  end

  sub_test_case 'buffered output with default configuration (throws exception for buffer overflow)' do
    setup do
      hash = {
        'flush_mode' => 'lazy',
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
        'total_limit_size' => 4096,
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{},[config_element('buffer','tag',hash)]))
      @i.start
      @i.after_start
    end

    test '#emit_events raises error when buffer is full' do
      @i.register(:format){|tag, time, record| "x" * 128 } # 128bytes per record (x4 -> 512bytes)

      es = Fluent::ArrayEventStream.new([
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
      ])

      8.times do |i|
        @i.emit_events("tag#{i}", es)
      end

      assert !@i.buffer.storable?

      assert_raise(Fluent::Plugin::Buffer::BufferOverflowError) do
        @i.emit_events("tag9", es)
      end
      logs = @i.log.out.logs
      assert{ logs.any?{|line| line.include?("failed to write data into buffer by buffer overflow") } }
    end
  end

  sub_test_case 'buffered output configured with "overflow_action block"' do
    setup do
      hash = {
        'flush_mode' => 'lazy',
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
        'total_limit_size' => 4096,
        'overflow_action' => "block",
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{'log_level' => 'debug'},[config_element('buffer','tag',hash)]))
      @i.start
      @i.after_start
    end

    test '#emit_events blocks until any queues are flushed' do
      failing = true
      flushed_chunks = []
      @i.register(:format){|tag, time, record| "x" * 128 } # 128bytes per record (x4 -> 512bytes)
      @i.register(:write) do |chunk|
        if failing
          raise "blocking"
        end
        flushed_chunks << chunk
      end

      es = Fluent::ArrayEventStream.new([
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
      ])

      4.times do |i|
        @i.emit_events("tag#{i}", es)
      end

      assert !@i.buffer.storable?

      Thread.new do
        sleep 3
        failing = false
      end

      assert_nothing_raised do
        @i.emit_events("tag9", es)
      end

      assert !failing
      assert{ flushed_chunks.size > 0 }

      logs = @i.log.out.logs
      assert{ logs.any?{|line| line.include?("failed to write data into buffer by buffer overflow") } }
      assert{ logs.any?{|line| line.include?("buffer.write is now blocking") } }
      assert{ logs.any?{|line| line.include?("retrying buffer.write after blocked operation") } }
    end
  end

  sub_test_case 'buffered output configured with "overflow_action drop_oldest_chunk"' do
    setup do
      hash = {
        'flush_mode' => 'lazy',
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
        'total_limit_size' => 4096,
        'overflow_action' => "drop_oldest_chunk",
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{'log_level' => 'debug'},[config_element('buffer','tag',hash)]))
      @i.start
      @i.after_start
    end

    test '#emit_events will success by dropping oldest chunk' do
      failing = true
      flushed_chunks = []
      @i.register(:format){|tag, time, record| "x" * 128 } # 128bytes per record (x4 -> 512bytes)
      @i.register(:write) do |chunk|
        if failing
          raise "blocking"
        end
        flushed_chunks << chunk
      end

      es = Fluent::ArrayEventStream.new([
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
      ])

      4.times do |i|
        @i.emit_events("tag#{i}", es)
      end

      assert !@i.buffer.storable?

      assert{ @i.buffer.queue[0].metadata.tag == "tag0" }
      assert{ @i.buffer.queue[1].metadata.tag == "tag1" }

      assert_nothing_raised do
        @i.emit_events("tag9", es)
      end

      assert failing
      assert{ flushed_chunks.size == 0 }

      assert{ @i.buffer.queue[0].metadata.tag == "tag1" }

      logs = @i.log.out.logs
      assert{ logs.any?{|line| line.include?("failed to write data into buffer by buffer overflow") } }
      assert{ logs.any?{|line| line.include?("dropping oldest chunk to make space after buffer overflow") } }
    end

    test '#emit_events raises OverflowError if all buffer spaces are used by staged chunks' do
      @i.register(:format){|tag, time, record| "x" * 128 } # 128bytes per record (x4 -> 512bytes)

      es = Fluent::ArrayEventStream.new([
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
        [event_time(), {"message" => "test"}],
      ])

      8.times do |i|
        @i.emit_events("tag#{i}", es)
      end

      assert !@i.buffer.storable?

      assert{ @i.buffer.queue.size == 0 }
      assert{ @i.buffer.stage.size == 8 }

      assert_raise Fluent::Plugin::Buffer::BufferOverflowError do
        @i.emit_events("tag9", es)
      end

      logs = @i.log.out.logs
      assert{ logs.any?{|line| line.include?("failed to write data into buffer by buffer overflow") } }
      assert{ logs.any?{|line| line.include?("no queued chunks to be dropped for drop_oldest_chunk") } }
    end
  end
end
