require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/msgpack_factory'
require 'fluent/event'

require 'json'
require 'time'
require 'timeout'

require 'flexmock/test_unit'

module FluentPluginStandardBufferedOutputTest
  class DummyBareOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
  end
  class DummyAsyncOutput < DummyBareOutput
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end
  class DummyAsyncStandardOutput < DummyBareOutput
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end
end

class StandardBufferedOutputTest < Test::Unit::TestCase
  def create_output(type=:full)
    case type
    when :bare     then FluentPluginStandardBufferedOutputTest::DummyBareOutput.new
    when :buffered then FluentPluginStandardBufferedOutputTest::DummyAsyncOutput.new
    when :standard then FluentPluginStandardBufferedOutputTest::DummyAsyncStandardOutput.new
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
  def test_event_stream
    es = Fluent::MultiEventStream.new
    es.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
    es.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})
    es.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
    es.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
    es.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
    es.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
    es
  end

  setup do
    @i = nil
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

  sub_test_case 'standard buffered without any chunk keys' do
    test '#execute_chunking calls @buffer.write(bulk: true) just once with predefined msgpack format' do
      @i = create_output(:standard)
      @i.configure(config_element())
      @i.start
      @i.after_start

      m = create_metadata()
      es = test_event_stream

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({m => es}, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM, enqueue: false)

      @i.execute_chunking("mytag.test", es)
    end

    test '#execute_chunking calls @buffer.write(bulk: true) just once with predefined msgpack format, but time will be int if time_as_integer specified' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{"time_as_integer"=>"true"}))
      @i.start
      @i.after_start

      m = create_metadata()
      es = test_event_stream

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({m => es}, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM_TIME_INT, enqueue: false)

      @i.execute_chunking("mytag.test", es)
    end
  end

  sub_test_case 'standard buffered with tag chunk key' do
    test '#execute_chunking calls @buffer.write(bulk: true) just once with predefined msgpack format' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','tag',{'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m = create_metadata(tag: "mytag.test")
      es = test_event_stream

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({m => es}, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM, enqueue: false)

      @i.execute_chunking("mytag.test", es)
    end

    test '#execute_chunking calls @buffer.write(bulk: true) just once with predefined msgpack format, but time will be int if time_as_integer specified' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{"time_as_integer"=>"true"},[config_element('buffer','tag',{'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m = create_metadata(tag: "mytag.test")
      es = test_event_stream

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({m => es}, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM_TIME_INT, enqueue: false)

      @i.execute_chunking("mytag.test", es)
    end
  end

  sub_test_case 'standard buffered with time chunk key' do
    test '#execute_chunking calls @buffer.write(bulk: true) with predefined msgpack format' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','time',{"timekey" => "60",'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m1 = create_metadata(timekey: Time.parse('2016-04-21 17:19:00 -0700').to_i)
      m2 = create_metadata(timekey: Time.parse('2016-04-21 17:20:00 -0700').to_i)
      m3 = create_metadata(timekey: Time.parse('2016-04-21 17:21:00 -0700').to_i)

      es1 = Fluent::MultiEventStream.new
      es1.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      es2 = Fluent::MultiEventStream.new
      es2.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es2.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      es3 = Fluent::MultiEventStream.new
      es3.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({
          m1 => es1,
          m2 => es2,
          m3 => es3,
        }, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM, enqueue: false)

      es = test_event_stream
      @i.execute_chunking("mytag.test", es)
    end

    test '#execute_chunking calls @buffer.write(bulk: true) with predefined msgpack format, but time will be int if time_as_integer specified' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{"time_as_integer" => "true"},[config_element('buffer','time',{"timekey" => "60",'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m1 = create_metadata(timekey: Time.parse('2016-04-21 17:19:00 -0700').to_i)
      m2 = create_metadata(timekey: Time.parse('2016-04-21 17:20:00 -0700').to_i)
      m3 = create_metadata(timekey: Time.parse('2016-04-21 17:21:00 -0700').to_i)

      es1 = Fluent::MultiEventStream.new
      es1.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      es2 = Fluent::MultiEventStream.new
      es2.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es2.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      es3 = Fluent::MultiEventStream.new
      es3.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).with({
          m1 => es1,
          m2 => es2,
          m3 => es3,
        }, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM_TIME_INT, enqueue: false)

      es = test_event_stream
      @i.execute_chunking("mytag.test", es)
    end
  end

  sub_test_case 'standard buffered with variable chunk keys' do
    test '#execute_chunking calls @buffer.write(bulk: true) with predefined msgpack format' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','key,name',{'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m1 = create_metadata(variables: {key: "my value", name: "moris1"})
      es1 = Fluent::MultiEventStream.new
      es1.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      m2 = create_metadata(variables: {key: "my value", name: "moris2"})
      es2 = Fluent::MultiEventStream.new
      es2.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).with({
          m1 => es1,
          m2 => es2,
        }, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM, enqueue: false).once

      es = test_event_stream
      @i.execute_chunking("mytag.test", es)
    end

    test '#execute_chunking calls @buffer.write(bulk: true) in times of # of variable variations with predefined msgpack format, but time will be int if time_as_integer specified' do
      @i = create_output(:standard)
      @i.configure(config_element('ROOT','',{"time_as_integer" => "true"},[config_element('buffer','key,name',{'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m1 = create_metadata(variables: {key: "my value", name: "moris1"})
      es1 = Fluent::MultiEventStream.new
      es1.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      m2 = create_metadata(variables: {key: "my value", name: "moris2"})
      es2 = Fluent::MultiEventStream.new
      es2.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).with({
          m1 => es1,
          m2 => es2,
        }, format: Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM_TIME_INT, enqueue: false).once

      es = test_event_stream
      @i.execute_chunking("mytag.test", es)
    end
  end

  sub_test_case 'custom format buffered without any chunk keys' do
    test '#execute_chunking calls @buffer.write(bulk: true) just once with customized format' do
      @i = create_output(:buffered)
      @i.register(:format){|tag, time, record| [time, record].to_json }
      @i.configure(config_element())
      @i.start
      @i.after_start

      m = create_metadata()
      es = test_event_stream

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({m => es.map{|t,r| [t,r].to_json }}, format: nil, enqueue: false)

      @i.execute_chunking("mytag.test", es)
    end
  end

  sub_test_case 'custom format buffered with tag chunk key' do
    test '#execute_chunking calls @buffer.write(bulk: true) just once with customized format' do
      @i = create_output(:buffered)
      @i.register(:format){|tag, time, record| [time, record].to_json }
      @i.configure(config_element('ROOT','',{},[config_element('buffer','tag',{'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m = create_metadata(tag: "mytag.test")
      es = test_event_stream

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).once.with({m => es.map{|t,r| [t,r].to_json}}, format: nil, enqueue: false)

      @i.execute_chunking("mytag.test", es)
    end
  end
  sub_test_case 'custom format buffered with time chunk key' do
    test '#execute_chunking calls @buffer.write with customized format' do
      @i = create_output(:buffered)
      @i.register(:format){|tag, time, record| [time, record].to_json }
      @i.configure(config_element('ROOT','',{},[config_element('buffer','time',{"timekey" => "60",'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m1 = create_metadata(timekey: Time.parse('2016-04-21 17:19:00 -0700').to_i)
      m2 = create_metadata(timekey: Time.parse('2016-04-21 17:20:00 -0700').to_i)
      m3 = create_metadata(timekey: Time.parse('2016-04-21 17:21:00 -0700').to_i)

      es1 = Fluent::MultiEventStream.new
      es1.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      es2 = Fluent::MultiEventStream.new
      es2.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es2.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      es3 = Fluent::MultiEventStream.new
      es3.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).with({
          m1 => es1.map{|t,r| [t,r].to_json },
          m2 => es2.map{|t,r| [t,r].to_json },
          m3 => es3.map{|t,r| [t,r].to_json },
        }, enqueue: false).once

      es = test_event_stream
      @i.execute_chunking("mytag.test", es)
    end
  end

  sub_test_case 'custom format buffered with variable chunk keys' do
    test '#execute_chunking calls @buffer.write in times of # of variable variations with customized format' do
      @i = create_output(:buffered)
      @i.register(:format){|tag, time, record| [time, record].to_json }
      @i.configure(config_element('ROOT','',{},[config_element('buffer','key,name',{'flush_thread_burst_interval' => 0.01})]))
      @i.start
      @i.after_start

      m1 = create_metadata(variables: {key: "my value", name: "moris1"})
      es1 = Fluent::MultiEventStream.new
      es1.add(event_time('2016-04-21 17:19:00 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:19:25 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:20:01 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:20:13 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})
      es1.add(event_time('2016-04-21 17:21:32 -0700'), {"key" => "my value", "name" => "moris1", "message" => "hello!"})

      m2 = create_metadata(variables: {key: "my value", name: "moris2"})
      es2 = Fluent::MultiEventStream.new
      es2.add(event_time('2016-04-21 17:19:13 -0700'), {"key" => "my value", "name" => "moris2", "message" => "hello!"})

      buffer_mock = flexmock(@i.buffer)
      buffer_mock.should_receive(:write).with({
          m1 => es1.map{|t,r| [t,r].to_json },
          m2 => es2.map{|t,r| [t,r].to_json },
        }, enqueue: false).once

      es = test_event_stream
      @i.execute_chunking("mytag.test", es)
    end
  end
end
