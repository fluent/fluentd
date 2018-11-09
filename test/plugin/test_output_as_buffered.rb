require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/output'
require 'fluent/event'

require 'json'
require 'time'
require 'timeout'
require 'timecop'

module FluentPluginOutputAsBufferedTest
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
  class DummyAsyncOutput < DummyBareOutput
    def initialize
      super
      @format = nil
      @write = nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end
  class DummyDelayedOutput < DummyBareOutput
    def initialize
      super
      @format = nil
      @try_write = nil
      @shutdown_hook = nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
    def shutdown
      if @shutdown_hook
        @shutdown_hook.call
      end
      super
    end
  end
  class DummyStandardBufferedOutput < DummyBareOutput
    def initialize
      super
      @prefer_delayed_commit = nil
      @write = nil
      @try_write = nil
    end
    def prefer_delayed_commit
      @prefer_delayed_commit ? @prefer_delayed_commit.call : false
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
  class DummyCustomFormatBufferedOutput < DummyBareOutput
    def initialize
      super
      @format_type_is_msgpack = nil
      @prefer_delayed_commit = nil
      @write = nil
      @try_write = nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def formatted_to_msgpack_binary?
      @format_type_is_msgpack ? @format_type_is_msgpack.call : false
    end
    def prefer_delayed_commit
      @prefer_delayed_commit ? @prefer_delayed_commit.call : false
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
  # check for formatted_to_msgpack_binary compatibility
  class DummyOldCustomFormatBufferedOutput < DummyBareOutput
    def initialize
      super
      @format_type_is_msgpack = nil
      @prefer_delayed_commit = nil
      @write = nil
      @try_write = nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def formatted_to_msgpack_binary
      @format_type_is_msgpack ? @format_type_is_msgpack.call : false
    end
    def prefer_delayed_commit
      @prefer_delayed_commit ? @prefer_delayed_commit.call : false
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
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
  module OldPluginMethodMixin
    def initialize
      super
      @format = nil
      @write = nil
    end
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end
  class DummyOldBufferedOutput < Fluent::BufferedOutput
    include OldPluginMethodMixin
  end
  class DummyOldObjectBufferedOutput < Fluent::ObjectBufferedOutput
    include OldPluginMethodMixin
  end
end

class BufferedOutputTest < Test::Unit::TestCase
  def create_output(type=:full)
    case type
    when :bare     then FluentPluginOutputAsBufferedTest::DummyBareOutput.new
    when :sync     then FluentPluginOutputAsBufferedTest::DummySyncOutput.new
    when :buffered then FluentPluginOutputAsBufferedTest::DummyAsyncOutput.new
    when :delayed  then FluentPluginOutputAsBufferedTest::DummyDelayedOutput.new
    when :standard then FluentPluginOutputAsBufferedTest::DummyStandardBufferedOutput.new
    when :custom   then FluentPluginOutputAsBufferedTest::DummyCustomFormatBufferedOutput.new
    when :full     then FluentPluginOutputAsBufferedTest::DummyFullFeatureOutput.new
    when :old_buf  then FluentPluginOutputAsBufferedTest::DummyOldBufferedOutput.new
    when :old_obj  then FluentPluginOutputAsBufferedTest::DummyOldObjectBufferedOutput.new
    when :old_custom then FluentPluginOutputAsBufferedTest::DummyOldCustomFormatBufferedOutput.new
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

  test 'queued_chunks_limit_size is same as flush_thread_count by default' do
    hash = {'flush_thread_count' => 4}
    i = create_output
    i.register(:prefer_buffered_processing) { true }
    i.configure(config_element('ROOT', '', {}, [config_element('buffer','tag',hash)]))

    assert_equal 4, i.buffer.queued_chunks_limit_size
  end

  test 'prefer queued_chunks_limit_size parameter than flush_thread_count' do
    hash = {'flush_thread_count' => 4, 'queued_chunks_limit_size' => 2}
    i = create_output
    i.register(:prefer_buffered_processing) { true }
    i.configure(config_element('ROOT', '', {}, [config_element('buffer','tag',hash)]))

    assert_equal 2, i.buffer.queued_chunks_limit_size
  end

  sub_test_case 'chunk feature in #write for output plugins' do
    setup do
      @stored_global_logger = $log
      $log = Fluent::Test::TestLogger.new
      @hash = {
        'flush_mode' => 'immediate',
        'flush_thread_interval' => '0.01',
        'flush_thread_burst_interval' => '0.01',
      }
    end

    teardown do
      $log = @stored_global_logger
    end

    test 'plugin using standard format can iterate chunk for time, record in #write' do
      events_from_chunk = []
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',@hash)]))
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|t,r| e << [t,r]}; events_from_chunk << [:write, e] }
      @i.register(:try_write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|t,r| e << [t,r]}; events_from_chunk << [:try_write, e] }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      waiting(5){ sleep 0.1 until events_from_chunk.size == 2 }

      assert_equal 2, events_from_chunk.size
      2.times.each do |i|
        assert_equal :write, events_from_chunk[i][0]
        assert_equal events, events_from_chunk[i][1]
      end
    end

    test 'plugin using standard format can iterate chunk for time, record in #try_write' do
      events_from_chunk = []
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',@hash)]))
      @i.register(:prefer_delayed_commit){ true }
      @i.register(:write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|t,r| e << [t,r]}; events_from_chunk << [:write, e] }
      @i.register(:try_write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|t,r| e << [t,r]}; events_from_chunk << [:try_write, e] }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      waiting(5){ sleep 0.1 until events_from_chunk.size == 2 }

      assert_equal 2, events_from_chunk.size
      2.times.each do |i|
        assert_equal :try_write, events_from_chunk[i][0]
        assert_equal events, events_from_chunk[i][1]
      end
    end

    test 'plugin using custom format cannot iterate chunk in #write' do
      events_from_chunk = []
      @i = create_output(:custom)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',@hash)]))
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){ |tag, time, record| [tag,time,record].to_json }
      @i.register(:format_type_is_msgpack){ false }
      @i.register(:write){ |chunk| assert !(chunk.respond_to?(:each)) }
      @i.register(:try_write){ |chunk| assert !(chunk.respond_to?(:each)) }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      assert_equal 0, events_from_chunk.size
    end

    test 'plugin using custom format cannot iterate chunk in #try_write' do
      events_from_chunk = []
      @i = create_output(:custom)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',@hash)]))
      @i.register(:prefer_delayed_commit){ true }
      @i.register(:format){ |tag, time, record| [tag,time,record].to_json }
      @i.register(:format_type_is_msgpack){ false }
      @i.register(:write){ |chunk| assert !(chunk.respond_to?(:each)) }
      @i.register(:try_write){ |chunk| assert !(chunk.respond_to?(:each)) }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      assert_equal 0, events_from_chunk.size
    end

    data('formatted_to_msgpack_binary?' => :custom,
         'formatted_to_msgpack_binary' => :old_custom)
    test 'plugin using custom format can iterate chunk in #write if #format returns msgpack' do |out_type|
      events_from_chunk = []
      @i = create_output(out_type)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',@hash)]))
      @i.register(:prefer_delayed_commit){ false }
      @i.register(:format){ |tag, time, record| [tag,time,record].to_msgpack }
      @i.register(:format_type_is_msgpack){ true }
      @i.register(:write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|ta,t,r| e << [ta,t,r]}; events_from_chunk << [:write, e] }
      @i.register(:try_write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|ta,t,r| e << [ta,t,r]}; events_from_chunk << [:try_write, e] }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      waiting(5){ sleep 0.1 until events_from_chunk.size == 2 }

      assert_equal 2, events_from_chunk.size
      2.times.each do |i|
        assert_equal :write, events_from_chunk[i][0]
        each_pushed = events_from_chunk[i][1]
        assert_equal 2, each_pushed.size
        assert_equal 'test.tag', each_pushed[0][0]
        assert_equal 'test.tag', each_pushed[1][0]
        assert_equal events, each_pushed.map{|tag,time,record| [time,record]}
      end
    end

    data(:handle_stream_simple => '',
         :handle_stream_with_custom_format => 'tag,message')
    test 'plugin using custom format can skip record chunk when format return nil' do |chunk_keys|
      events_from_chunk = []
      @i = create_output(:custom)
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', chunk_keys, @hash)]))
      @i.register(:prefer_delayed_commit) { false }
      @i.register(:format) { |tag, time, record|
        if record['message'] == 'test1'
          nil
        else
          [tag,time,record].to_msgpack
        end
      }
      @i.register(:format_type_is_msgpack) { true }
      @i.register(:write){ |chunk| e = []; chunk.each { |ta, t, r| e << [ta, t, r] }; events_from_chunk << [:write, e] }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "test1"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "test2"}],
      ]
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      waiting(5) { sleep 0.1 until events_from_chunk.size == 1 }

      assert_equal 1, events_from_chunk.size
      assert_equal :write, events_from_chunk[0][0]
      each_pushed = events_from_chunk[0][1]
      assert_equal 1, each_pushed.size
      assert_equal 'test.tag', each_pushed[0][0]
      assert_equal "test2", each_pushed[0][2]['message']
    end

    test 'plugin using custom format can iterate chunk in #try_write if #format returns msgpack' do
      events_from_chunk = []
      @i = create_output(:custom)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',@hash)]))
      @i.register(:prefer_delayed_commit){ true }
      @i.register(:format){ |tag, time, record| [tag,time,record].to_msgpack }
      @i.register(:format_type_is_msgpack){ true }
      @i.register(:write){ |chunk| events_from_chunk = []; assert chunk.respond_to?(:each); chunk.each{|ta,t,r| e << [ta,t,r]}; events_from_chunk << [:write, e] }
      @i.register(:try_write){ |chunk| e = []; assert chunk.respond_to?(:each); chunk.each{|ta,t,r| e << [ta,t,r]}; events_from_chunk << [:try_write, e] }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      waiting(5){ sleep 0.1 until events_from_chunk.size == 2 }

      assert_equal 2, events_from_chunk.size
      2.times.each do |i|
        assert_equal :try_write, events_from_chunk[i][0]
        each_pushed = events_from_chunk[i][1]
        assert_equal 2, each_pushed.size
        assert_equal 'test.tag', each_pushed[0][0]
        assert_equal 'test.tag', each_pushed[1][0]
        assert_equal events, each_pushed.map{|tag,time,record| [time,record]}
      end
    end

    data(:BufferedOutput => :old_buf,
         :ObjectBufferedOutput => :old_obj)
    test 'old plugin types can iterate chunk by msgpack_each in #write' do |plugin_type|
      events_from_chunk = []
      # event_emitter helper requires Engine.root_agent for routing
      ra = Fluent::RootAgent.new(log: $log)
      stub(Fluent::Engine).root_agent { ra }
      @i = create_output(plugin_type)
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', @hash)]))
      @i.register(:format) { |tag, time, record| [time, record].to_msgpack }
      @i.register(:write) { |chunk| e = []; chunk.msgpack_each { |t, r| e << [t, r] }; events_from_chunk << [:write, e]; }
      @i.start
      @i.after_start

      events = [
        [event_time('2016-10-05 16:16:16 -0700'), {"message" => "yaaaaaaaaay!"}],
        [event_time('2016-10-05 16:16:17 -0700'), {"message" => "yoooooooooy!"}],
      ]

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new(events))

      waiting(5) { sleep 0.1 until events_from_chunk.size == 2 }

      assert_equal 2, events_from_chunk.size
      2.times.each do |i|
        assert_equal :write, events_from_chunk[i][0]
        assert_equal events, events_from_chunk[i][1]
      end
    end
  end

  sub_test_case 'buffered output configured with many chunk keys' do
    setup do
      @stored_global_logger = $log
      $log = Fluent::Test::TestLogger.new
      @hash = {
        'flush_mode' => 'interval',
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
        'timekey' => 60,
      }
      @i = create_output(:buffered)
    end
    teardown do
      $log = @stored_global_logger
    end
    test 'nothing are warned with less chunk keys' do
      chunk_keys = 'time,key1,key2,key3'
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_keys,@hash)]))
      logs = @i.log.out.logs.dup
      @i.start
      @i.after_start
      assert{ logs.select{|log| log.include?('[warn]') }.size == 0 }
    end

    test 'a warning reported with 4 chunk keys' do
      chunk_keys = 'key1,key2,key3,key4'
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_keys,@hash)]))
      logs = @i.log.out.logs.dup

      @i.start # this calls `log.reset`... capturing logs about configure must be done before this line
      @i.after_start
      assert_equal ['key1', 'key2', 'key3', 'key4'], @i.chunk_keys

      assert{ logs.select{|log| log.include?('[warn]: many chunk keys specified, and it may cause too many chunks on your system.') }.size == 1 }
    end

    test 'a warning reported with 4 chunk keys including "tag"' do
      chunk_keys = 'tag,key1,key2,key3'
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_keys,@hash)]))
      logs = @i.log.out.logs.dup
      @i.start # this calls `log.reset`... capturing logs about configure must be done before this line
      @i.after_start
      assert{ logs.select{|log| log.include?('[warn]: many chunk keys specified, and it may cause too many chunks on your system.') }.size == 1 }
    end

    test 'time key is not included for warned chunk keys' do
      chunk_keys = 'time,key1,key2,key3'
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_keys,@hash)]))
      logs = @i.log.out.logs.dup
      @i.start
      @i.after_start
      assert{ logs.select{|log| log.include?('[warn]') }.size == 0 }
    end
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: lazy' do
    setup do
      hash = {
        'flush_mode' => 'lazy',
        'flush_thread_burst_interval' => 0.01,
        'flush_thread_count' => 2,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
      @i.after_start
    end

    test '#start does not create enqueue thread, but creates flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:flush_thread_1)
      assert !@i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each events' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      4.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 8, ary.size
      4.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called only when chunk bytes limit exceeded, and buffer chunk is purged' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      (1024 * 0.9 / event_size).to_i.times do |i|
        @i.emit_events("test.tag", Fluent::ArrayEventStream.new([ [t, r] ]))
      end
      assert{ @i.buffer.queue.size == 0 && ary.size == 0 }

      staged_chunk = @i.buffer.stage[@i.buffer.stage.keys.first]
      assert{ staged_chunk.size != 0 }

      @i.emit_events("test.tag", Fluent::ArrayEventStream.new([ [t, r] ]))

      assert{ @i.buffer.queue.size > 0 || @i.buffer.dequeued.size > 0 || ary.size > 0 }

      waiting(10) do
        Thread.pass until @i.buffer.queue.size == 0 && @i.buffer.dequeued.size == 0
        Thread.pass until staged_chunk.size == 0
      end

      assert_equal 1, ary.size
      assert_equal [tag,t,r].to_json * (1024 / event_size), ary.first
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      (1024 * 0.9 / event_size).to_i.times do |i|
        @i.emit_events("test.tag", Fluent::ArrayEventStream.new([ [t, r] ]))
      end
      assert{ @i.buffer.queue.size == 0 && ary.size == 0 }

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(10) do
        Thread.pass until ary.size == 1
      end
      assert_equal [tag,t,r].to_json * (1024 * 0.9 / event_size), ary.first
    end
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: interval' do
    setup do
      hash = {
        'flush_mode' => 'interval',
        'flush_interval' => 1,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
      @i.after_start
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      4.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 8, ary.size
      4.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called per flush_interval, and buffer chunk is purged' do
      @i.thread_wait_until_start

      ary = []
      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| ary << data } }

      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end

      3.times do |i|
        rand_records = rand(1..4)
        es = Fluent::ArrayEventStream.new([ [t, r] ] * rand_records)
        assert_equal rand_records, es.size

        @i.interrupt_flushes

        assert{ @i.buffer.queue.size == 0 }

        @i.emit_events("test.tag", es)

        assert{ @i.buffer.queue.size == 0 }
        assert{ @i.buffer.stage.size == 1 }

        staged_chunk = @i.instance_eval{ @buffer.stage[@buffer.stage.keys.first] }
        assert{ staged_chunk.size != 0 }

        @i.enqueue_thread_wait

        waiting(10) do
          Thread.pass until @i.buffer.queue.size == 0 && @i.buffer.dequeued.size == 0
          Thread.pass until staged_chunk.size == 0
        end

        assert_equal rand_records, ary.size
        ary.reject!{|e| true }
      end
    end
  end

  sub_test_case 'with much longer flush_interval' do
    setup do
      hash = {
        'flush_mode' => 'interval',
        'flush_interval' => 3000,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
      @i.after_start
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      (1024 * 0.9 / event_size).to_i.times do |i|
        @i.emit_events("test.tag", Fluent::ArrayEventStream.new([ [t, r] ]))
      end
      queue_size = @i.buffer.queue.size
      assert{ queue_size == 0 && ary.size == 0 }

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(10){ sleep 0.1 until ary.size == 1 }
      assert_equal [tag,t,r].to_json * (1024 * 0.9 / event_size), ary.first
    end
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: immediate' do
    setup do
      hash = {
        'flush_mode' => 'immediate',
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.01,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
      @i.after_start
    end

    test '#start does not create enqueue thread, but creates flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert !@i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      4.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 8, ary.size
      4.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called every time for each emits, and buffer chunk is purged' do
      @i.thread_wait_until_start

      ary = []
      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| ary << data } }

      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end

      3.times do |i|
        rand_records = rand(1..5)
        es = Fluent::ArrayEventStream.new([ [t, r] ] * rand_records)
        assert_equal rand_records, es.size
        @i.emit_events("test.tag", es)

        waiting(10){ sleep 0.1 until @i.buffer.stage.size == 0 } # make sure that the emitted es is enqueued by "flush_mode immediate"
        waiting(10){ sleep 0.1 until @i.buffer.queue.size == 0 && @i.buffer.dequeued.size == 0 }
        waiting(10){ sleep 0.1 until ary.size == rand_records }

        assert_equal rand_records, ary.size
        ary.reject!{|e| true }
      end
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      @i.emit_events("test.tag", Fluent::ArrayEventStream.new([ [t, r] ]))

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(10) do
        Thread.pass until ary.size == 1
      end
      assert_equal [tag,t,r].to_json, ary.first
    end
  end

  sub_test_case 'buffered output feature with timekey and range' do
    setup do
      chunk_key = 'time'
      hash = {
        'timekey' => 30, # per 30seconds
        'timekey_wait' => 5, # 5 second delay for flush
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.01,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
      @i.after_start
    end

    test '#configure raises config error if timekey is not specified' do
      i = create_output(:buffered)
      assert_raise Fluent::ConfigError do
        i.configure(config_element('ROOT','',{},[config_element('buffer','time',)]))
      end
    end

    test 'default flush_mode is set to :lazy' do
      assert_equal :lazy, @i.instance_eval{ @flush_mode }
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      5.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called per time ranges after timekey_wait, and buffer chunk is purged' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:00 +0900') )

      @i.thread_wait_until_start

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.timekey.to_i <= e[1].to_i && e[1].to_i < chunk.metadata.timekey.to_i + 30) } }

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        Fluent::EventTime.parse('2016-04-13 14:03:21 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:23 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:29 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:30 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:33 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:38 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:43 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:49 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:51 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:04:00 +0900'), Fluent::EventTime.parse('2016-04-13 14:04:01 +0900'),
      ]
      events = [
        ["test.tag.1", ts[0], r], # range 14:03:00 - 03:29
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r], # range 14:03:30 - 04:00
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r], # range 14:04:00 - 04:29
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 0 }

      @i.enqueue_thread_wait

      waiting(4){ sleep 0.1 until @i.write_count > 0 }

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      waiting(4){ sleep 0.1 until ary.size == 3 }

      assert_equal 3, ary.size
      assert_equal 2, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 1, ary.select{|e| e[0] == "test.tag.2" }.size

      Timecop.freeze( Time.parse('2016-04-13 14:04:04 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:06 +0900') )

      @i.enqueue_thread_wait
      waiting(4){ sleep 0.1 until @i.write_count > 1 }

      assert{ @i.buffer.stage.size == 1 && @i.write_count == 2 }

      assert_equal 9, ary.size
      assert_equal 7, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 2, ary.select{|e| e[0] == "test.tag.2" }.size

      assert metachecks.all?{|e| e }
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:00 +0900') )

      @i.thread_wait_until_start

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk|
        chunk.read.split("\n").reject{|l| l.empty? }.each{|data|
          e = JSON.parse(data)
          ary << e
          metachecks << (chunk.metadata.timekey.to_i <= e[1].to_i && e[1].to_i < chunk.metadata.timekey.to_i + 30)
        }
      }

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        Fluent::EventTime.parse('2016-04-13 14:03:21 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:23 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:29 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:30 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:33 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:38 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:43 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:49 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:51 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:04:00 +0900'), Fluent::EventTime.parse('2016-04-13 14:04:01 +0900'),
      ]
      events = [
        ["test.tag.1", ts[0], r], # range 14:03:00 - 03:29
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r], # range 14:03:30 - 04:00
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r], # range 14:04:00 - 04:29
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 0 }

      @i.enqueue_thread_wait

      waiting(4){ sleep 0.1 until @i.write_count > 0 }

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:04 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:06 +0900') )

      @i.enqueue_thread_wait
      waiting(4){ sleep 0.1 until @i.write_count > 1 }

      assert{ @i.buffer.stage.size == 1 && @i.write_count == 2 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )

      waiting(4){ sleep 0.1 until ary.size == 9 }
      assert_equal 9, ary.size

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(4){ sleep 0.1 until @i.write_count > 2 && ary.size == 11 }

      assert_equal 11, ary.size
      assert metachecks.all?{|e| e }
    end
  end

  sub_test_case 'buffered output feature with tag key' do
    setup do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 10,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.1,
        'chunk_limit_size' => 1024,
        'queued_chunks_limit_size' => 100
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
      @i.after_start
    end

    test 'default flush_mode is set to :interval' do
      assert_equal :interval, @i.instance_eval{ @flush_mode }
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      5.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called per tags, per flush_interval & chunk sizes, and buffer chunk is purged' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.tag == e[0]) } }

      @i.thread_wait_until_start

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        event_time('2016-04-13 14:03:21 +0900'), event_time('2016-04-13 14:03:23 +0900'), event_time('2016-04-13 14:03:29 +0900'),
        event_time('2016-04-13 14:03:30 +0900'), event_time('2016-04-13 14:03:33 +0900'), event_time('2016-04-13 14:03:38 +0900'),
        event_time('2016-04-13 14:03:43 +0900'), event_time('2016-04-13 14:03:49 +0900'), event_time('2016-04-13 14:03:51 +0900'),
        event_time('2016-04-13 14:04:00 +0900'), event_time('2016-04-13 14:04:01 +0900'),
      ]
      # size of a event is 197
      events = [
        ["test.tag.1", ts[0], r],
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r],
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r],
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 2 } # test.tag.1 x1, test.tag.2 x1

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 }

      # to trigger try_flush with flush_thread_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:15 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 2
      end

      assert{ @i.buffer.stage.size == 0 && @i.write_count == 3 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert metachecks.all?{|e| e }
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.tag == e[0]) } }

      @i.thread_wait_until_start

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        event_time('2016-04-13 14:03:21 +0900'), event_time('2016-04-13 14:03:23 +0900'), event_time('2016-04-13 14:03:29 +0900'),
        event_time('2016-04-13 14:03:30 +0900'), event_time('2016-04-13 14:03:33 +0900'), event_time('2016-04-13 14:03:38 +0900'),
        event_time('2016-04-13 14:03:43 +0900'), event_time('2016-04-13 14:03:49 +0900'), event_time('2016-04-13 14:03:51 +0900'),
        event_time('2016-04-13 14:04:00 +0900'), event_time('2016-04-13 14:04:01 +0900'),
      ]
      # size of a event is 197
      events = [
        ["test.tag.1", ts[0], r],
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r],
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r],
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 2 } # test.tag.1 x1, test.tag.2 x1

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.buffer.queue.size == 0 && @i.write_count == 3 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert metachecks.all?{|e| e }
    end
  end

  sub_test_case 'buffered output feature with variables' do
    setup do
      chunk_key = 'name,service'
      hash = {
        'flush_interval' => 10,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.1,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
      @i.after_start
    end

    test 'default flush_mode is set to :interval' do
      assert_equal :interval, @i.instance_eval{ @flush_mode }
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([
        [t, {"key" => "value1", "name" => "moris", "service" => "a"}],
        [t, {"key" => "value2", "name" => "moris", "service" => "b"}],
      ])

      5.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1", "name" => "moris", "service" => "a"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2", "name" => "moris", "service" => "b"}], ary[i*2+1]
      end
    end

    test '#write is called per value combination of variables, per flush_interval & chunk sizes, and buffer chunk is purged' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (e[2]["name"] == chunk.metadata.variables[:name] && e[2]["service"] == chunk.metadata.variables[:service]) } }

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1) xxx-a (6 events)
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2) yyy-a (3 events)
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3) xxx-b (2 events)
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3)
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 3 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size
      assert ary[0...5].all?{|e| e[2]["name"] == "xxx" && e[2]["service"] == "a" }

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 3 }

      # to trigger try_flush with flush_thread_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:12 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:14 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.write_count == 4 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size
      assert_equal 6, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "a" }.size
      assert_equal 3, ary.select{|e| e[2]["name"] == "yyy" && e[2]["service"] == "a" }.size
      assert_equal 2, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "b" }.size

      assert metachecks.all?{|e| e }
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (e[2]["name"] == chunk.metadata.variables[:name] && e[2]["service"] == chunk.metadata.variables[:service]) } }

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1) xxx-a (6 events)
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2) yyy-a (3 events)
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3) xxx-b (2 events)
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3)
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 3 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.buffer.queue.size == 0 && @i.write_count == 4 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size
      assert_equal 6, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "a" }.size
      assert_equal 3, ary.select{|e| e[2]["name"] == "yyy" && e[2]["service"] == "a" }.size
      assert_equal 2, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "b" }.size

      assert metachecks.all?{|e| e }
    end
  end

  sub_test_case 'buffered output feature with many keys' do
    test 'default flush mode is set to :interval if keys does not include time' do
      chunk_key = 'name,service,tag'
      hash = {
        'flush_interval' => 10,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.1,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
      @i.after_start

      assert_equal :interval, @i.instance_eval{ @flush_mode }
    end

    test 'default flush mode is set to :lazy if keys includes time' do
      chunk_key = 'name,service,tag,time'
      hash = {
        'timekey' => 60,
        'flush_interval' => 10,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.1,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
      @i.after_start

      assert_equal :lazy, @i.instance_eval{ @flush_mode }
    end
  end

  sub_test_case 'buffered output feature with delayed commit' do
    setup do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 10,
        'flush_thread_count' => 1,
        'flush_thread_burst_interval' => 0.1,
        'delayed_commit_timeout' => 30,
        'chunk_limit_size' => 1024,
      }
      @i = create_output(:delayed)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
      @i.after_start
      @i.log = Fluent::Test::TestLogger.new
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = Fluent::ArrayEventStream.new([
        [t, {"key" => "value1", "name" => "moris", "service" => "a"}],
        [t, {"key" => "value2", "name" => "moris", "service" => "b"}],
      ])

      5.times do
        @i.emit_events('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1", "name" => "moris", "service" => "a"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2", "name" => "moris", "service" => "b"}], ary[i*2+1]
      end
    end

    test '#try_write is called per flush, buffer chunk is not purged until #commit_write is called' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []
      chunks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:try_write) do |chunk|
        chunks << chunk
        chunk.read.split("\n").reject{|l| l.empty? }.each do |data|
          e = JSON.parse(data)
          ary << e
          metachecks << (e[0] == chunk.metadata.tag)
        end
      end

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}],
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}],
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 2 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }
      assert{ @i.buffer.dequeued.size == 1 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      assert_equal 1, chunks.size
      assert !chunks.first.empty?

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 }

      # to trigger try_flush with flush_thread_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:12 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:14 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.write_count == 3 }
      assert{ @i.buffer.dequeued.size == 3 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert_equal 3, chunks.size
      assert chunks.all?{|c| !c.empty? }

      assert metachecks.all?{|e| e }

      @i.commit_write(chunks[0].unique_id)
      assert{ @i.buffer.dequeued.size == 2 }
      assert chunks[0].empty?

      @i.commit_write(chunks[1].unique_id)
      assert{ @i.buffer.dequeued.size == 1 }
      assert chunks[1].empty?

      @i.commit_write(chunks[2].unique_id)
      assert{ @i.buffer.dequeued.size == 0 }
      assert chunks[2].empty?

      # no problem to commit chunks already committed
      assert_nothing_raised do
        @i.commit_write(chunks[2].unique_id)
      end
    end

    test '#rollback_write and #try_rollback_write can rollback buffer chunks for delayed commit after timeout, and then be able to write it again' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []
      chunks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:try_write) do |chunk|
        chunks << chunk
        chunk.read.split("\n").reject{|l| l.empty? }.each do |data|
          e = JSON.parse(data)
          ary << e
          metachecks << (e[0] == chunk.metadata.tag)
        end
      end

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}],
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}],
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 2 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }
      assert{ @i.buffer.dequeued.size == 1 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      assert_equal 1, chunks.size
      assert !chunks.first.empty?

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 }

      # to trigger try_flush with flush_thread_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:12 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:14 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 2
      end

      assert{ @i.buffer.stage.size == 0 && @i.write_count == 3 }
      assert{ @i.buffer.dequeued.size == 3 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert_equal 3, chunks.size
      assert chunks.all?{|c| !c.empty? }

      assert metachecks.all?{|e| e }

      @i.interrupt_flushes

      @i.rollback_write(chunks[2].unique_id)

      assert{ @i.buffer.dequeued.size == 2 }
      assert{ @i.buffer.queue.size == 1 && @i.buffer.queue.first.unique_id == chunks[2].unique_id }

      Timecop.freeze( Time.parse('2016-04-13 14:04:15 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 3
      end

      assert{ @i.write_count == 4 }
      assert{ @i.rollback_count == 1 }
      assert{ @i.instance_eval{ @dequeued_chunks.size } == 3 }
      assert{ @i.buffer.dequeued.size == 3 }
      assert{ @i.buffer.queue.size == 0 }

      assert_equal 4, chunks.size
      assert chunks[2].unique_id == chunks[3].unique_id

      ary.reject!{|e| true }
      chunks.reject!{|e| true }

      Timecop.freeze( Time.parse('2016-04-13 14:04:46 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.rollback_count == 4
      end

      assert{ chunks[0...3].all?{|c| !c.empty? } }

      # rollback is in progress, but some may be flushed again in retry state, after rollback
      # retry.next_time is 14:04:49
      Timecop.freeze( Time.parse('2016-04-13 14:04:51 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count == 7
      end

      assert{ @i.write_count == 7 }
      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size
      assert{ chunks.size == 3 }
      assert{ chunks.all?{|c| !c.empty? } }

      chunks.each{|c| @i.commit_write(c.unique_id) }
      assert{ chunks.all?{|c| c.empty? } }

      assert{ @i.buffer.dequeued.size == 0 }
    end

    test '#try_rollback_all will be called for all waiting chunks after shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []
      chunks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:try_write) do |chunk|
        chunks << chunk
        chunk.read.split("\n").reject{|l| l.empty? }.each do |data|
          e = JSON.parse(data)
          ary << e
          metachecks << (e[0] == chunk.metadata.tag)
        end
      end

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}],
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}],
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}],
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit_events(tag, Fluent::ArrayEventStream.new([ [time, record] ]))
      end
      assert{ @i.buffer.stage.size == 2 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }
      assert{ @i.buffer.dequeued.size == 1 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      assert_equal 1, chunks.size
      assert !chunks.first.empty?

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 }

      # to trigger try_flush with flush_thread_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:12 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:14 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 2
      end

      assert{ @i.buffer.stage.size == 0 }
      assert{ @i.buffer.queue.size == 0 }
      assert{ @i.buffer.dequeued.size == 3 }
      assert{ @i.write_count == 3 }
      assert{ @i.rollback_count == 0 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert{ chunks.size == 3 }
      assert{ chunks.all?{|c| !c.empty? } }

      @i.register(:shutdown_hook){ @i.commit_write(chunks[1].unique_id) }

      @i.stop
      @i.before_shutdown
      @i.shutdown

      assert{ @i.buffer.dequeued.size == 2 }
      assert{ !chunks[0].empty? }
      assert{ chunks[1].empty? }
      assert{ !chunks[2].empty? }

      @i.after_shutdown

      assert{ @i.rollback_count == 2 }
    end
  end
end
