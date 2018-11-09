require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/event'

require 'json'
require 'time'
require 'timeout'
require 'timecop'

module FluentPluginOutputAsBufferedSecondaryTest
  class DummyBareOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
  end
  class DummySyncOutput < DummyBareOutput
    def initialize
      super
      @process = nil
    end
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
  end
  class DummyFullFeatureOutput < DummyBareOutput
    def initialize
      super
      @prefer_buffered_processing = nil
      @prefer_delayed_commit = nil
      @process = nil
      @format = nil
      @write = nil
      @try_write = nil
    end
    def prefer_buffered_processing
      @prefer_buffered_processing ? @prefer_buffered_processing.call : false
    end
    def prefer_delayed_commit
      @prefer_delayed_commit ? @prefer_delayed_commit.call : false
    end
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
  class DummyFullFeatureOutput2 < DummyFullFeatureOutput
    def prefer_buffered_processing; true; end
    def prefer_delayed_commit; super; end
    def format(tag, time, record); super; end
    def write(chunk); super; end
    def try_write(chunk); super; end
  end
end

class BufferedOutputSecondaryTest < Test::Unit::TestCase
  def create_output(type=:full)
    case type
    when :bare then FluentPluginOutputAsBufferedSecondaryTest::DummyBareOutput.new
    when :sync then FluentPluginOutputAsBufferedSecondaryTest::DummySyncOutput.new
    when :full then FluentPluginOutputAsBufferedSecondaryTest::DummyFullFeatureOutput.new
    else
      raise ArgumentError, "unknown type: #{type}"
    end
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
      STDERR.print(*@i.log.out.logs)
      raise
    end
  end
  def dummy_event_stream
    Fluent::ArrayEventStream.new([
      [ event_time('2016-04-13 18:33:00'), {"name" => "moris", "age" => 36, "message" => "data1"} ],
      [ event_time('2016-04-13 18:33:13'), {"name" => "moris", "age" => 36, "message" => "data2"} ],
      [ event_time('2016-04-13 18:33:32'), {"name" => "moris", "age" => 36, "message" => "data3"} ],
    ])
  end

  setup do
    @i = create_output
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

  sub_test_case 'secondary plugin feature for buffered output with periodical retry' do
    setup do
      Fluent::Plugin.register_output('output_secondary_test', FluentPluginOutputAsBufferedSecondaryTest::DummyFullFeatureOutput)
      Fluent::Plugin.register_output('output_secondary_test2', FluentPluginOutputAsBufferedSecondaryTest::DummyFullFeatureOutput2)
    end

    test 'raises configuration error if primary does not support buffering' do
      i = create_output(:sync)
      assert_raise Fluent::ConfigError do
        i.configure(config_element('ROOT','',{},[config_element('secondary','',{'@type'=>'output_secondary_test'})]))
      end
    end

    test 'raises configuration error if <buffer>/<secondary> section is specified in <secondary> section' do
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 30, 'retry_randomize' => false})
      secconf1 = config_element('secondary','',{'@type' => 'output_secondary_test'},[config_element('buffer', 'time')])
      secconf2 = config_element('secondary','',{'@type' => 'output_secondary_test'},[config_element('secondary', '')])
      i = create_output()
      assert_raise Fluent::ConfigError do
        i.configure(config_element('ROOT','',{},[priconf,secconf1]))
      end
      assert_raise Fluent::ConfigError do
        i.configure(config_element('ROOT','',{},[priconf,secconf2]))
      end
    end

    test 'uses same plugin type with primary if @type is missing in secondary' do
      bufconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 30, 'retry_randomize' => false})
      secconf = config_element('secondary','',{})
      priconf = config_element('ROOT', '', {'@type' => 'output_secondary_test'}, [bufconf, secconf])
      i = create_output()
      assert_nothing_raised do
        i.configure(priconf)
      end
      logs = i.log.out.logs
      assert{ logs.empty? }
      assert{ i.secondary.is_a? FluentPluginOutputAsBufferedSecondaryTest::DummyFullFeatureOutput }
    end

    test 'warns if secondary plugin is different type from primary one' do
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 30, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      i = create_output()
      i.configure(config_element('ROOT','',{},[priconf,secconf]))
      logs = i.log.out.logs
      assert{ logs.any?{|l| l.include?("secondary type should be same with primary one") } }
    end

    test 'secondary plugin lifecycle is kicked by primary' do
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 30, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      i = create_output()
      i.configure(config_element('ROOT','',{},[priconf,secconf]))
      logs = i.log.out.logs
      assert{ logs.any?{|l| l.include?("secondary type should be same with primary one") } }

      assert i.secondary.configured?

      assert !i.secondary.started?
      i.start
      assert i.secondary.started?

      assert !i.secondary.after_started?
      i.after_start
      assert i.secondary.after_started?

      assert !i.secondary.stopped?
      i.stop
      assert i.secondary.stopped?

      assert !i.secondary.before_shutdown?
      i.before_shutdown
      assert i.secondary.before_shutdown?

      assert !i.secondary.shutdown?
      i.shutdown
      assert i.secondary.shutdown?

      assert !i.secondary.after_shutdown?
      i.after_shutdown
      assert i.secondary.after_shutdown?

      assert !i.secondary.closed?
      i.close
      assert i.secondary.closed?

      assert !i.secondary.terminated?
      i.terminate
      assert i.secondary.terminated?
    end

    test 'primary plugin will emit event streams to secondary after retries for time of retry_timeout * retry_secondary_threshold' do
      written = []
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ false }
      @i.secondary.register(:write){|chunk| chunk.read.split("\n").each{|line| written << JSON.parse(line) } }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8
      now = first_failure + 60 * 0.8 + 1 # to step from primary to secondary
      Timecop.freeze( now )

      unless @i.retry.secondary?
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors

        # next step is on secondary
        now = first_failure + 60 * 0.8 + 10
        Timecop.freeze( now )
      end

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

      current_write_count = @i.write_count
      current_num_errors = @i.num_errors
      assert{ current_write_count > prev_write_count }
      assert{ current_num_errors == prev_num_errors }

      assert_nil @i.retry

      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[0]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[1]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[2]

      logs = @i.log.out.logs
      waiting(4){ sleep 0.1 until logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
      assert{ logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
    end

    test 'secondary can do non-delayed commit even if primary do delayed commit' do
      written = []
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ true }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:try_write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ false }
      @i.secondary.register(:write){|chunk| chunk.read.split("\n").each{|line| written << JSON.parse(line) } }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8
      now = first_failure + 60 * 0.8 + 1 # to step from primary to secondary
      Timecop.freeze( now )

      unless @i.retry.secondary?
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors

        # next step is on secondary
        now = first_failure + 60 * 0.8 + 10
        Timecop.freeze( now )
      end

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

      assert{ @i.write_count > prev_write_count }
      assert{ @i.num_errors == prev_num_errors }

      assert_nil @i.retry

      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[0]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[1]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[2]

      logs = @i.log.out.logs
      waiting(4){ sleep 0.1 until logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
      assert{ logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
    end

    test 'secondary plugin can do delayed commit if primary do it' do
      written = []
      chunks = []
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ true }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:try_write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ true }
      @i.secondary.register(:try_write){|chunk| chunks << chunk; chunk.read.split("\n").each{|line| written << JSON.parse(line) } }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8
      now = first_failure + 60 * 0.8 + 1 # to step from primary to secondary
      Timecop.freeze( now )

      unless @i.retry.secondary?
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors

        # next step is on secondary
        now = first_failure + 60 * 0.8 + 10
        Timecop.freeze( now )
      end

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

      assert{ @i.write_count > prev_write_count }
      assert{ @i.num_errors == prev_num_errors }

      assert @i.retry

      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[0]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[1]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[2]

      assert{ @i.buffer.dequeued.size > 0 }
      assert{ chunks.size > 0 }
      assert{ !chunks.first.empty? }

      @i.secondary.commit_write(chunks[0].unique_id)

      assert{ @i.buffer.dequeued[chunks[0].unique_id].nil? }
      assert{ chunks.first.empty? }

      assert_nil @i.retry

      logs = @i.log.out.logs
      waiting(4){ sleep 0.1 until logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
      assert{ logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
    end

    test 'secondary plugin can do delayed commit even if primary does not do it' do
      written = []
      chunks = []
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ true }
      @i.secondary.register(:try_write){|chunk| chunks << chunk; chunk.read.split("\n").each{|line| written << JSON.parse(line) } }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8
      now = first_failure + 60 * 0.8 + 1 # to step from primary to secondary
      Timecop.freeze( now )

      unless @i.retry.secondary?
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors

        # next step is on secondary
        now = first_failure + 60 * 0.8 + 10
        Timecop.freeze( now )
      end

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

      assert{ @i.write_count > prev_write_count }
      assert{ @i.num_errors == prev_num_errors }

      assert @i.retry

      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[0]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[1]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[2]

      assert{ @i.buffer.dequeued.size > 0 }
      assert{ chunks.size > 0 }
      assert{ !chunks.first.empty? }

      @i.secondary.commit_write(chunks[0].unique_id)

      assert{ @i.buffer.dequeued[chunks[0].unique_id].nil? }
      assert{ chunks.first.empty? }

      assert_nil @i.retry

      logs = @i.log.out.logs
      waiting(4){ sleep 0.1 until logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
      assert{ logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
    end

    test 'secondary plugin can do delayed commit even if primary does not do it, and non-committed chunks will be rollbacked by primary' do
      written = []
      chunks = []
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 60, 'delayed_commit_timeout' => 2, 'retry_randomize' => false, 'queued_chunks_limit_size' => 10})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ true }
      @i.secondary.register(:try_write){|chunk| chunks << chunk; chunk.read.split("\n").each{|line| written << JSON.parse(line) } }
      @i.secondary.register(:write){|chunk| raise "don't use this" }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())
      @i.emit_events("test.tag.2", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size == 2 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8

      now = first_failure + 60 * 0.8 + 1
      Timecop.freeze( now )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      now = first_failure + 60 * 0.8 + 2
      Timecop.freeze( now )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4){ sleep 0.1 until chunks.size == 2 }

      assert{ @i.write_count > prev_write_count }
      assert{ @i.num_errors == prev_num_errors }

      assert @i.retry

      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[0]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[1]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[2]
      assert_equal [ 'test.tag.2', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[3]
      assert_equal [ 'test.tag.2', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[4]
      assert_equal [ 'test.tag.2', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[5]

      assert{ @i.buffer.dequeued.size == 2 }
      assert{ chunks.size == 2 }
      assert{ !chunks[0].empty? }
      assert{ !chunks[1].empty? }

      30.times do |i| # large enough
        now = first_failure + 60 * 0.8 + 2 + i
        Timecop.freeze( now )
        @i.flush_thread_wakeup

        break if @i.buffer.dequeued.size == 0
      end

      assert @i.retry
      logs = @i.log.out.logs
      waiting(4){ sleep 0.1 until logs.select{|l| l.include?("[warn]: failed to flush the buffer chunk, timeout to commit.") }.size == 2 }
      assert{ logs.select{|l| l.include?("[warn]: failed to flush the buffer chunk, timeout to commit.") }.size == 2 }
    end

    test 'retry_wait for secondary is same with one for primary' do
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :periodic, 'retry_wait' => 3, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ false }
      @i.secondary.register(:write){|chunk| raise "your secondary is also useless." }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8

      now = first_failure + 60 * 0.8 + 1

      Timecop.freeze( now )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

      assert{ @i.write_count > prev_write_count }
      assert{ @i.num_errors > prev_num_errors }

      assert @i.retry

      assert_equal 3, (@i.next_flush_time - Time.now)

      logs = @i.log.out.logs
      waiting(4){ sleep 0.1 until logs.any?{|l| l.include?("[warn]: failed to flush the buffer with secondary output.") } }
      assert{ logs.any?{|l| l.include?("[warn]: failed to flush the buffer with secondary output.") } }
    end
  end

  sub_test_case 'secondary plugin feature for buffered output with exponential backoff' do
    setup do
      Fluent::Plugin.register_output('output_secondary_test', FluentPluginOutputAsBufferedSecondaryTest::DummyFullFeatureOutput)
      Fluent::Plugin.register_output('output_secondary_test2', FluentPluginOutputAsBufferedSecondaryTest::DummyFullFeatureOutput2)
    end

    test 'primary plugin will emit event streams to secondary after retries for time of retry_timeout * retry_secondary_threshold' do
      written = []
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :exponential_backoff, 'retry_wait' => 1, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ false }
      @i.secondary.register(:write){|chunk| chunk.read.split("\n").each{|line| written << JSON.parse(line) } }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      20.times do |i| # large enough
        now = @i.next_flush_time
        Timecop.freeze( now )
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        assert{ @i.write_count > prev_write_count }

        break if @i.buffer.queue.size == 0

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors
      end

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8

      assert{ now >= first_failure + 60 * 0.8 }

      assert_nil @i.retry

      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:00').to_i, {"name" => "moris", "age" => 36, "message" => "data1"} ], written[0]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:13').to_i, {"name" => "moris", "age" => 36, "message" => "data2"} ], written[1]
      assert_equal [ 'test.tag.1', event_time('2016-04-13 18:33:32').to_i, {"name" => "moris", "age" => 36, "message" => "data3"} ], written[2]

      assert{ @i.log.out.logs.any?{|l| l.include?("[warn]: retry succeeded by secondary.") } }
    end

    test 'exponential backoff interval will be initialized when switched to secondary' do
      priconf = config_element('buffer','tag',{'flush_interval' => 1, 'retry_type' => :exponential_backoff, 'retry_wait' => 1, 'retry_timeout' => 60, 'retry_randomize' => false})
      secconf = config_element('secondary','',{'@type' => 'output_secondary_test2'})
      @i.configure(config_element('ROOT','',{},[priconf,secconf]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.secondary.register(:prefer_delayed_commit){ false }
      @i.secondary.register(:write){|chunk| raise "your secondary is also useless." }
      @i.start
      @i.after_start

      @i.interrupt_flushes

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:31 -0700')
      Timecop.freeze( now )

      @i.emit_events("test.tag.2", dummy_event_stream())

      assert_equal 0, @i.write_count
      assert_equal 0, @i.num_errors

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup
      waiting(4){ sleep 0.1 until @i.write_count > 0 && @i.num_errors > 0 }

      assert{ @i.buffer.queue.size > 0 }
      assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }

      assert{ @i.write_count > 0 }
      assert{ @i.num_errors > 0 }

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      first_failure = @i.retry.start

      20.times do |i| # large enough
        now = @i.next_flush_time
        # p({i: i, now: now, diff: (now - Time.now)})
        # {:i=>0, :now=>2016-04-13 18:33:32 -0700, :diff=>1.0}
        # {:i=>1, :now=>2016-04-13 18:33:34 -0700, :diff=>2.0}
        # {:i=>2, :now=>2016-04-13 18:33:38 -0700, :diff=>4.0}
        # {:i=>3, :now=>2016-04-13 18:33:46 -0700, :diff=>8.0}
        # {:i=>4, :now=>2016-04-13 18:34:02 -0700, :diff=>16.0}
        # {:i=>5, :now=>2016-04-13 18:34:19 -0700, :diff=>17.0}
        Timecop.freeze( now )
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        assert{ @i.write_count > prev_write_count }
        assert{ @i.num_errors > prev_num_errors }

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors

        break if @i.retry.secondary?

        assert{ @i.buffer.queue.first.metadata.tag == 'test.tag.1' }
      end

      # retry_timeout == 60(sec), retry_secondary_threshold == 0.8

      assert{ now >= first_failure + 60 * 0.8 }
      assert @i.retry
      logs = @i.log.out.logs
      assert{ logs.any?{|l| l.include?("[warn]: failed to flush the buffer with secondary output.") } }

      assert{ (@i.next_flush_time - Time.now) <= 2 } # <= retry_wait (1s) * base (2) ** 1

      20.times do |i| # large enough again
        now = @i.next_flush_time
        # p({i: i, now: now, diff: (now - Time.now)})
        # {:i=>0, :now=>2016-04-13 18:34:20 -0700, :diff=>1.0}
        # {:i=>1, :now=>2016-04-13 18:34:24 -0700, :diff=>4.0}
        # {:i=>2, :now=>2016-04-13 18:34:31 -0700, :diff=>7.0}

        Timecop.freeze( now )
        @i.enqueue_thread_wait
        @i.flush_thread_wakeup
        waiting(4){ sleep 0.1 until @i.write_count > prev_write_count }

        assert{ @i.write_count > prev_write_count }
        assert{ @i.num_errors > prev_num_errors }

        break if @i.buffer.queue.size == 0
      end

      logs = @i.log.out.logs
      assert{ logs.any?{|l| l.include?("[error]: failed to flush the buffer, and hit limit for retries. dropping all chunks in the buffer queue.") } }

      assert{ now >= first_failure + 60 }
    end
  end
end
