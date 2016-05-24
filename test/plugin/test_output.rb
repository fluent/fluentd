require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/event'

require 'json'
require 'time'
require 'timeout'

module FluentPluginOutputTest
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
  class DummyAsyncStandardOutput < DummyBareOutput
    def initialize
      super
      @write = nil
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
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
  class DummyDelayedStandardOutput < DummyBareOutput
    def initialize
      super
      @try_write = nil
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
end

class OutputTest < Test::Unit::TestCase
  def create_output(type=:full)
    case type
    when :bare     then FluentPluginOutputTest::DummyBareOutput.new
    when :sync     then FluentPluginOutputTest::DummySyncOutput.new
    when :buffered then FluentPluginOutputTest::DummyAsyncOutput.new
    when :standard then FluentPluginOutputTest::DummyAsyncStandardOutput.new
    when :delayed  then FluentPluginOutputTest::DummyDelayedOutput.new
    when :sdelayed then FluentPluginOutputTest::DummyDelayedStandardOutput.new
    when :full     then FluentPluginOutputTest::DummyFullFeatureOutput.new
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

  sub_test_case 'basic output feature' do
    setup do
      @i = create_output(:full)
    end

    test '#implement? can return features for plugin instances' do
      i1 = FluentPluginOutputTest::DummyBareOutput.new
      assert !i1.implement?(:synchronous)
      assert !i1.implement?(:buffered)
      assert !i1.implement?(:delayed_commit)
      assert !i1.implement?(:custom_format)

      i2 = FluentPluginOutputTest::DummySyncOutput.new
      assert i2.implement?(:synchronous)
      assert !i2.implement?(:buffered)
      assert !i2.implement?(:delayed_commit)
      assert !i2.implement?(:custom_format)

      i3 = FluentPluginOutputTest::DummyAsyncOutput.new
      assert !i3.implement?(:synchronous)
      assert i3.implement?(:buffered)
      assert !i3.implement?(:delayed_commit)
      assert i3.implement?(:custom_format)

      i4 = FluentPluginOutputTest::DummyAsyncStandardOutput.new
      assert !i4.implement?(:synchronous)
      assert i4.implement?(:buffered)
      assert !i4.implement?(:delayed_commit)
      assert !i4.implement?(:custom_format)

      i5 = FluentPluginOutputTest::DummyDelayedOutput.new
      assert !i5.implement?(:synchronous)
      assert !i5.implement?(:buffered)
      assert i5.implement?(:delayed_commit)
      assert i5.implement?(:custom_format)

      i6 = FluentPluginOutputTest::DummyDelayedStandardOutput.new
      assert !i6.implement?(:synchronous)
      assert !i6.implement?(:buffered)
      assert i6.implement?(:delayed_commit)
      assert !i6.implement?(:custom_format)

      i6 = FluentPluginOutputTest::DummyFullFeatureOutput.new
      assert i6.implement?(:synchronous)
      assert i6.implement?(:buffered)
      assert i6.implement?(:delayed_commit)
      assert i6.implement?(:custom_format)
    end

    test 'plugin lifecycle for configure/start/stop/before_shutdown/shutdown/after_shutdown/close/terminate' do
      assert !@i.configured?
      @i.configure(config_element())
      assert @i.configured?
      assert !@i.started?
      @i.start
      assert @i.started?
      assert !@i.stopped?
      @i.stop
      assert @i.stopped?
      assert !@i.before_shutdown?
      @i.before_shutdown
      assert @i.before_shutdown?
      assert !@i.shutdown?
      @i.shutdown
      assert @i.shutdown?
      assert !@i.after_shutdown?
      @i.after_shutdown
      assert @i.after_shutdown?
      assert !@i.closed?
      @i.close
      assert @i.closed?
      assert !@i.terminated?
      @i.terminate
      assert @i.terminated?
    end

    test '#extract_placeholders does nothing if chunk key is not specified' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal tmpl, @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract time if time key and range are configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/2016/04/11/20-30/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract tag and parts of tag if tag is configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'tag', {})]))
      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/%Y/%m/%d/%H-%M/fluentd.test.output/test/output/${key1}/${key2}/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract variables if variables are configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'key1,key2', {})]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/value1/value2/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract all chunk keys if configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/test/output/value1/value2/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders removes out-of-range tag part and unknown variable placeholders' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[3]}/${tag[4]}/${key3}/${key4}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/////tail", @i.extract_placeholders(tmpl, m)
    end

    test '#metadata returns object which contains tag/timekey/variables from records as specified in configuration' do
      tag = 'test.output'
      time = event_time('2016-04-12 15:31:23 -0700')
      timekey = event_time('2016-04-12 15:00:00 -0700')
      record = {"key1" => "value1", "num1" => 1, "message" => "my message"}

      i1 = create_output(:buffered)
      i1.configure(config_element('ROOT','',{},[config_element('buffer', '')]))
      assert_equal create_metadata(), i1.metadata(tag, time, record)

      i2 = create_output(:buffered)
      i2.configure(config_element('ROOT','',{},[config_element('buffer', 'tag')]))
      assert_equal create_metadata(tag: tag), i2.metadata(tag, time, record)

      i3 = create_output(:buffered)
      i3.configure(config_element('ROOT','',{},[config_element('buffer', 'time', {"timekey" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(timekey: timekey), i3.metadata(tag, time, record)

      i4 = create_output(:buffered)
      i4.configure(config_element('ROOT','',{},[config_element('buffer', 'key1', {})]))
      assert_equal create_metadata(variables: {key1: "value1"}), i4.metadata(tag, time, record)

      i5 = create_output(:buffered)
      i5.configure(config_element('ROOT','',{},[config_element('buffer', 'key1,num1', {})]))
      assert_equal create_metadata(variables: {key1: "value1", num1: 1}), i5.metadata(tag, time, record)

      i6 = create_output(:buffered)
      i6.configure(config_element('ROOT','',{},[config_element('buffer', 'tag,time', {"timekey" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(timekey: timekey, tag: tag), i6.metadata(tag, time, record)

      i7 = create_output(:buffered)
      i7.configure(config_element('ROOT','',{},[config_element('buffer', 'tag,num1', {"timekey" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(tag: tag, variables: {num1: 1}), i7.metadata(tag, time, record)

      i8 = create_output(:buffered)
      i8.configure(config_element('ROOT','',{},[config_element('buffer', 'time,tag,key1', {"timekey" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(timekey: timekey, tag: tag, variables: {key1: "value1"}), i8.metadata(tag, time, record)
    end

    test '#emit calls #process via #emit_sync for non-buffered output' do
      i = create_output(:sync)
      process_called = false
      i.register(:process){|tag, es| process_called = true }
      i.configure(config_element())
      i.start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))

      assert process_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#emit calls #format for buffered output' do
      i = create_output(:buffered)
      format_called_times = 0
      i.register(:format){|tag, time, record| format_called_times += 1; '' }
      i.configure(config_element())
      i.start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))

      assert_equal 2, format_called_times

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_buffered_processing (returns false) decides non-buffered without <buffer> section' do
      i = create_output(:full)

      process_called = false
      format_called_times = 0
      i.register(:process){|tag, es| process_called = true }
      i.register(:format){|tag, time, record| format_called_times += 1; '' }

      i.configure(config_element())
      i.register(:prefer_buffered_processing){ false } # delayed decision is possible to change after (output's) configure
      i.start

      assert !i.prefer_buffered_processing

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))

      waiting(4){ Thread.pass until process_called }

      assert process_called
      assert_equal 0, format_called_times

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_buffered_processing (returns true) decides buffered without <buffer> section' do
      i = create_output(:full)

      process_called = false
      format_called_times = 0
      i.register(:process){|tag, es| process_called = true }
      i.register(:format){|tag, time, record| format_called_times += 1; '' }

      i.configure(config_element())
      i.register(:prefer_buffered_processing){ true } # delayed decision is possible to change after (output's) configure
      i.start

      assert i.prefer_buffered_processing

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))

      assert !process_called
      assert_equal 2, format_called_times

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test 'output plugin will call #write for normal buffered plugin to flush buffer chunks' do
      i = create_output(:buffered)
      write_called = false
      i.register(:write){ |chunk| write_called = true }

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until write_called }

      assert write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test 'output plugin will call #try_write for plugin supports delayed commit only to flush buffer chunks' do
      i = create_output(:delayed)
      try_write_called = false
      i.register(:try_write){|chunk| try_write_called = true; commit_write(chunk.unique_id) }

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until try_write_called }

      assert try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_delayed_commit (returns false) decides delayed commit is disabled if both are implemented' do
      i = create_output(:full)
      write_called = false
      try_write_called = false
      i.register(:write){ |chunk| write_called = true }
      i.register(:try_write){|chunk| try_write_called = true; commit_write(chunk.unique_id) }

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.register(:prefer_delayed_commit){ false } # delayed decision is possible to change after (output's) configure
      i.start

      assert !i.prefer_delayed_commit

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until write_called || try_write_called }

      assert write_called
      assert !try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_delayed_commit (returns true) decides delayed commit is enabled if both are implemented' do
      i = create_output(:full)
      write_called = false
      try_write_called = false
      i.register(:write){ |chunk| write_called = true }
      i.register(:try_write){|chunk| try_write_called = true; commit_write(chunk.unique_id) }

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.register(:prefer_delayed_commit){ true } # delayed decision is possible to change after (output's) configure
      i.start

      assert i.prefer_delayed_commit

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until write_called || try_write_called }

      assert !write_called
      assert try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end
  end

  sub_test_case 'sync output feature' do
    setup do
      @i = create_output(:sync)
    end

    test 'raises configuration error if <buffer> section is specified' do
      assert_raise Fluent::ConfigError do
        @i.configure(config_element('ROOT','',{},[config_element('buffer', '')]))
      end
    end

    test 'raises configuration error if <secondary> section is specified' do
      assert_raise Fluent::ConfigError do
        @i.configure(config_element('ROOT','',{},[config_element('secondary','')]))
      end
    end

    test '#process is called for each event streams' do
      ary = []
      @i.register(:process){|tag, es| ary << [tag, es] }
      @i.configure(config_element())
      @i.start

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])
      5.times do
        @i.emit_events('tag', es)
      end
      assert_equal 5, ary.size

      @i.stop; @i.before_shutdown; @i.shutdown; @i.after_shutdown; @i.close; @i.terminate
    end
  end
end
