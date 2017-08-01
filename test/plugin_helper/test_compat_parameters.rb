require_relative '../helper'
require 'fluent/plugin_helper/compat_parameters'
require 'fluent/plugin/input'
require 'fluent/plugin/output'
require 'fluent/time'

require 'time'

class CompatParameterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
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

  class DummyI0 < Fluent::Plugin::Input
    helpers :compat_parameters, :parser, :extract
    attr_reader :parser
    def configure(conf)
      compat_parameters_convert(conf, :extract, :parser)
      super
    end
    def start
      super
      @parser = parser_create
    end
    def produce_events(input_data)
      emit_events = [] # tag, time, record
      @parser.parse(input_data) do |time, record|
        tag = extract_tag_from_record(record) || 'dummy_tag'
        emit_events << [tag, time, record]
      end
      emit_events
    end
  end
  class DummyO0 < Fluent::Plugin::Output
    helpers :compat_parameters
    def configure(conf)
      compat_parameters_buffer(conf, default_chunk_key: '')
      super
    end
    def write(chunk); end # dummy
  end
  class DummyO1 < Fluent::Plugin::Output
    helpers :compat_parameters
    def configure(conf)
      compat_parameters_buffer(conf, default_chunk_key: 'time')
      super
    end
    def write(chunk); end # dummy
  end
  class DummyO2 < Fluent::Plugin::Output
    helpers :compat_parameters
    def configure(conf)
      compat_parameters_buffer(conf, default_chunk_key: 'time')
      super
    end
    def write(chunk); end # dummy
  end
  class DummyO3 < Fluent::Plugin::Output
    helpers :compat_parameters
    def configure(conf)
      compat_parameters_buffer(conf, default_chunk_key: 'tag')
      super
    end
    def write(chunk); end # dummy
  end
  class DummyO4 < Fluent::Plugin::Output
    helpers :compat_parameters, :inject, :formatter
    attr_reader :f
    def configure(conf)
      compat_parameters_convert(conf, :buffer, :inject, :formatter, default_chunk_key: 'tag')
      super
    end
    def start
      super
      @f = formatter_create()
    end
    def write(chunk); end # dummy
  end

  sub_test_case 'output plugins which does not have default chunk key' do
    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'num_threads' => 8,
        'flush_interval' => '10s',
        'buffer_chunk_limit' => '8m',
        'buffer_queue_limit' => '1024',
        'flush_at_shutdown' => 'yes',
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyO0.new
      @i.configure(conf)

      assert_equal 'memory', @i.buffer_config[:@type]
      assert_equal [], @i.buffer_config.chunk_keys
      assert_equal 8, @i.buffer_config.flush_thread_count
      assert_equal 10, @i.buffer_config.flush_interval
      assert_equal :default, @i.buffer_config.flush_mode
      assert @i.buffer_config.flush_at_shutdown

      assert_equal 8*1024*1024, @i.buffer.chunk_limit_size
      assert_equal 1024, @i.buffer.queue_limit_length
    end
  end

  sub_test_case 'output plugins which has default chunk key: time' do
    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'buffer_type' => 'file',
        'buffer_path' => '/tmp/mybuffer',
        'disable_retry_limit' => 'yes',
        'max_retry_wait' => '1h',
        'buffer_queue_full_action' => 'block',
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyO1.new
      @i.configure(conf)

      assert_equal 'file', @i.buffer_config[:@type]
      assert_equal 24*60*60, @i.buffer_config.timekey
      assert @i.buffer_config.retry_forever
      assert_equal 60*60, @i.buffer_config.retry_max_interval
      assert_equal :block, @i.buffer_config.overflow_action
      assert_equal :default, @i.buffer_config.flush_mode

      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys

      assert_equal '/tmp/mybuffer/buffer.*.log', @i.buffer.path
    end
  end

  sub_test_case 'output plugins which does not have default chunk key' do
    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'buffer_type' => 'file',
        'buffer_path' => '/tmp/mybuffer',
        'time_slice_format' => '%Y%m%d%H',
        'time_slice_wait' => '10',
        'retry_limit' => '1024',
        'buffer_queue_full_action' => 'drop_oldest_chunk',
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyO2.new
      @i.configure(conf)

      assert_equal 'file', @i.buffer_config[:@type]
      assert_equal 60*60, @i.buffer_config.timekey
      assert_equal 10, @i.buffer_config.timekey_wait
      assert_equal 1024, @i.buffer_config.retry_max_times
      assert_equal :drop_oldest_chunk, @i.buffer_config.overflow_action

      assert @i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys

      assert_equal '/tmp/mybuffer/buffer.*.log', @i.buffer.path
    end
  end

  sub_test_case 'output plugins which has default chunk key: tag' do
    test 'plugin helper converts parameters into plugin configuration parameters' do
      hash = {
        'buffer_type' => 'memory',
        'num_threads' => '10',
        'flush_interval' => '10s',
        'try_flush_interval' => '0.1',
        'queued_chunk_flush_interval' => '0.5',
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyO3.new
      @i.configure(conf)

      assert_equal 'memory', @i.buffer_config[:@type]
      assert_equal 10, @i.buffer_config.flush_thread_count
      assert_equal 10, @i.buffer_config.flush_interval
      assert_equal 0.1, @i.buffer_config.flush_thread_interval
      assert_equal 0.5, @i.buffer_config.flush_thread_burst_interval

      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys
    end
  end

  sub_test_case 'output plugins which has default chunk key: tag, and enables inject and formatter' do
    test 'plugin helper converts parameters into plugin configuration parameters for all of buffer, inject and formatter' do
      hash = {
        'buffer_type' => 'file',
        'buffer_path' => File.expand_path('../../tmp/compat_parameters/mybuffer.*.log', __FILE__),
        'num_threads' => '10',
        'format' => 'ltsv',
        'delimiter' => ',',
        'label_delimiter' => '%',
        'include_time_key' => 'true', # default time_key 'time' and default time format (iso8601: 2016-06-24T15:57:38) at localtime
        'include_tag_key' => 'yes', # default tag_key 'tag'
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyO4.new
      @i.configure(conf)
      @i.start
      @i.after_start

      assert_equal 'file', @i.buffer_config[:@type]
      assert_equal 10, @i.buffer_config.flush_thread_count
      formatter = @i.f
      assert{ formatter.is_a? Fluent::Plugin::LabeledTSVFormatter }
      assert_equal ',', @i.f.delimiter
      assert_equal '%', @i.f.label_delimiter

      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys

      t = event_time('2016-06-24 16:05:01') # localtime
      iso8601str = Time.at(t.to_i).iso8601
      formatted = @i.f.format('tag.test', t, @i.inject_values_to_record('tag.test', t, {"value" => 1}))
      assert_equal "value%1,tag%tag.test,time%#{iso8601str}\n", formatted
    end

    test 'plugin helper setups time injecting as unix time (integer from epoch)' do
      hash = {
        'buffer_type' => 'file',
        'buffer_path' => File.expand_path('../../tmp/compat_parameters/mybuffer.*.log', __FILE__),
        'num_threads' => '10',
        'format' => 'ltsv',
        'delimiter' => ',',
        'label_delimiter' => '%',
        'include_time_key' => 'true', # default time_key 'time' and default time format (iso8601: 2016-06-24T15:57:38) at localtime
        'include_tag_key' => 'yes', # default tag_key 'tag'
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyO4.new
      @i.configure(conf)
      @i.start
      @i.after_start

      assert_equal 'file', @i.buffer_config[:@type]
      assert_equal 10, @i.buffer_config.flush_thread_count
      formatter = @i.f
      assert{ formatter.is_a? Fluent::Plugin::LabeledTSVFormatter }
      assert_equal ',', @i.f.delimiter
      assert_equal '%', @i.f.label_delimiter

      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys

      t = event_time('2016-06-24 16:05:01') # localtime
      iso8601str = Time.at(t.to_i).iso8601
      formatted = @i.f.format('tag.test', t, @i.inject_values_to_record('tag.test', t, {"value" => 1}))
      assert_equal "value%1,tag%tag.test,time%#{iso8601str}\n", formatted
    end
  end

  sub_test_case 'input plugins' do
    test 'plugin helper converts parameters into plugin configuration parameters for extract and parser' do
      hash = {
        'format' => 'ltsv',
        'delimiter' => ',',
        'label_delimiter' => '%',
        'tag_key' => 't2',
        'time_key' => 't',
        'time_format' => '%Y-%m-%d.%H:%M:%S.%N',
        'utc' => 'yes',
        'types' => 'A integer|B string|C bool',
        'types_delimiter' => '|',
        'types_label_delimiter' => ' ',
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyI0.new
      @i.configure(conf)
      @i.start
      @i.after_start

      parser = @i.parser
      assert{ parser.is_a? Fluent::Plugin::LabeledTSVParser }
      assert_equal ',', parser.delimiter
      assert_equal '%', parser.label_delimiter

      events = @i.produce_events("A%1,B%x,C%true,t2%mytag,t%2016-10-20.03:50:11.987654321")
      assert_equal 1, events.size

      tag, time, record = events.first
      assert_equal 'mytag', tag
      assert_equal_event_time event_time("2016-10-20 03:50:11.987654321 +0000"), time
      assert_equal 3, record.keys.size
      assert_equal ['A','B','C'], record.keys.sort
      assert_equal 1, record['A']
      assert_equal 'x', record['B']
      assert_equal true, record['C']
    end

    test 'plugin helper converts parameters into plugin configuration parameters for extract and parser, using numeric time' do
      hash = {
        'format' => 'ltsv',
        'delimiter' => ',',
        'label_delimiter' => '%',
        'tag_key' => 't2',
        'time_key' => 't',
        'time_type' => 'float',
        'localtime' => 'yes',
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyI0.new
      @i.configure(conf)
      @i.start
      @i.after_start

      parser = @i.parser
      assert{ parser.is_a? Fluent::Plugin::LabeledTSVParser }
      assert_equal ',', parser.delimiter
      assert_equal '%', parser.label_delimiter
    end

    test 'plugin helper setups time extraction as unix time (integer from epoch)' do
      # TODO:
    end
  end

  sub_test_case 'parser plugins' do
    test 'syslog parser parameters' do
      hash = {
        'format' => 'syslog',
        'message_format' => 'rfc5424',
        'with_priority' => 'true',
        'rfc5424_time_format' => '%Y'
      }
      conf = config_element('ROOT', '', hash)
      @i = DummyI0.new
      @i.configure(conf)
      @i.start
      @i.after_start

      parser = @i.parser
      assert_kind_of(Fluent::Plugin::SyslogParser, parser)
      assert_equal :rfc5424, parser.message_format
      assert_equal true, parser.with_priority
      assert_equal '%Y', parser.rfc5424_time_format
    end
  end
end
