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
  class << self
    def startup
      $LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '../scripts'))
      require 'fluent/plugin/out_test'
    end

    def shutdown
      $LOAD_PATH.shift
    end
  end

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
  def create_chunk(timekey: nil, tag: nil, variables: nil)
    m = Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
    Fluent::Plugin::Buffer::MemoryChunk.new(m)
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

    test 'are not available with multi workers configuration in default' do
      assert_false @i.multi_workers_ready?
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
      assert !@i.after_started?
      @i.after_start
      assert @i.after_started?
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

    test 'can use metrics plugins and fallback methods' do
      @i.configure(config_element())

      %w[num_errors_metrics emit_count_metrics emit_size_metrics emit_records_metrics
         write_count_metrics rollback_count_metrics flush_time_count_metrics slow_flush_count_metrics].each do |metric_name|
        assert_true @i.instance_variable_get(:"@#{metric_name}").is_a?(Fluent::Plugin::Metrics)
      end

      assert_equal 0, @i.num_errors
      assert_equal 0, @i.emit_count
      assert_equal 0, @i.emit_size
      assert_equal 0, @i.emit_records
      assert_equal 0, @i.write_count
      assert_equal 0, @i.rollback_count
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders does nothing if chunk key is not specified' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal tmpl, @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders can extract time if time key and range are configured' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/2016/04/11/20-30/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail", @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders can extract tag and parts of tag if tag is configured' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'tag', {})]))
      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/%Y/%m/%d/%H-%M/fluentd.test.output/test/output/${key1}/${key2}/tail", @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders can extract variables if variables are configured' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'key1,key2', {})]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/value1/value2/tail", @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders can extract nested variables if variables are configured with dot notation' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'key,$.nest.key', {})]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal ['key','$.nest.key'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key}/${$.nest.key}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {:key => "value1", :"$.nest.key" => "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/value1/value2/tail", @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders can extract all chunk keys if configured' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/test/output/value1/value2/tail", @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders can extract negative index with tag' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[-1]}/${tag[-2]}/${key1}/${key2}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/output/test/value1/value2/tail", @i.extract_placeholders(tmpl, c)
    end

    data(:new_api => :chunk,
         :old_api => :metadata)
    test '#extract_placeholders removes out-of-range tag part and unknown variable placeholders' do |api|
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[3]}/${tag[-4]}/${key3}/${key4}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = if api == :chunk
            create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
          else
            create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
          end
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/////tail", @i.extract_placeholders(tmpl, c)
    end

    test '#extract_placeholders logs warn message if metadata is passed for ${chunk_id} placeholder' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
      tmpl = "/mypath/${chunk_id}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      @i.extract_placeholders(tmpl, m)
      logs = @i.log.out.logs
      assert { logs.any? { |log| log.include?("${chunk_id} is not allowed in this plugin") } }
    end

    test '#extract_placeholders does not log for ${chunk_id} placeholder' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
      tmpl = "/mypath/${chunk_id}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
      @i.log.out.logs.clear
      @i.extract_placeholders(tmpl, c)
      logs = @i.log.out.logs
      assert { logs.none? { |log| log.include?("${chunk_id}") } }
    end

    test '#extract_placeholders does not log for ${chunk_id} placeholder (with @chunk_keys)' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'key1')]))
      tmpl = "/mypath/${chunk_id}/${key1}/tail"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = {key1: "value1", key2: "value2"}
      c = create_chunk(timekey: t, tag: 'fluentd.test.output', variables: v)
      @i.log.out.logs.clear
      @i.extract_placeholders(tmpl, c)
      logs = @i.log.out.logs
      assert { logs.none? { |log| log.include?("${chunk_id}") } }
    end

    test '#extract_placeholders logs warn message with not replaced key' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
      tmpl = "/mypath/${key1}/test"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = { key1: "value1" }
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      @i.extract_placeholders(tmpl, m)
      logs = @i.log.out.logs

      assert { logs.any? { |log| log.include?("chunk key placeholder 'key1' not replaced. template:#{tmpl}") } }
    end

    test '#extract_placeholders logs warn message with not replaced key if variables exist and chunk_key is not empty' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'key1')]))
      tmpl = "/mypath/${key1}/${key2}/test"
      t = event_time('2016-04-11 20:30:00 +0900')
      v = { key1: "value1" }
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      @i.extract_placeholders(tmpl, m)
      logs = @i.log.out.logs

      assert { logs.any? { |log| log.include?("chunk key placeholder 'key2' not replaced. template:#{tmpl}") } }
    end

    sub_test_case '#placeholder_validators' do
      test 'returns validators for time, tag and keys when a template has placeholders even if plugin is not configured with these keys' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        validators = @i.placeholder_validators(:path, "/my/path/${tag}/${username}/file.%Y%m%d_%H%M.log")
        assert_equal 3, validators.size
        assert_equal 1, validators.count(&:time?)
        assert_equal 1, validators.count(&:tag?)
        assert_equal 1, validators.count(&:keys?)
      end

      test 'returns validators for time, tag and keys when a plugin is configured with these keys even if a template does not have placeholders' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,username', {'timekey' => 60})]))
        validators = @i.placeholder_validators(:path, "/my/path/file.log")
        assert_equal 3, validators.size
        assert_equal 1, validators.count(&:time?)
        assert_equal 1, validators.count(&:tag?)
        assert_equal 1, validators.count(&:keys?)
      end

      test 'returns a validator for time if a template has timestamp placeholders' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        validators = @i.placeholder_validators(:path, "/my/path/file.%Y-%m-%d.log")
        assert_equal 1, validators.size
        assert_equal 1, validators.count(&:time?)
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.%Y-%m-%d.log' has timestamp placeholders, but chunk key 'time' is not configured") do
          validators.first.validate!
        end
      end

      test 'returns a validator for time if a plugin is configured with time key' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {'timekey' => '30'})]))
        validators = @i.placeholder_validators(:path, "/my/path/to/file.log")
        assert_equal 1, validators.size
        assert_equal 1, validators.count(&:time?)
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/to/file.log' doesn't have timestamp placeholders for timekey 30") do
          validators.first.validate!
        end
      end

      test 'returns a validator for tag if a template has tag placeholders' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        validators = @i.placeholder_validators(:path, "/my/path/${tag}/file.log")
        assert_equal 1, validators.size
        assert_equal 1, validators.count(&:tag?)
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/${tag}/file.log' has tag placeholders, but chunk key 'tag' is not configured") do
          validators.first.validate!
        end
      end

      test 'returns a validator for tag if a plugin is configured with tag key' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'tag')]))
        validators = @i.placeholder_validators(:path, "/my/path/file.log")
        assert_equal 1, validators.size
        assert_equal 1, validators.count(&:tag?)
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.log' doesn't have tag placeholder") do
          validators.first.validate!
        end
      end

      test 'returns a validator for variable keys if a template has variable placeholders' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        validators = @i.placeholder_validators(:path, "/my/path/${username}/file.${group}.log")
        assert_equal 1, validators.size
        assert_equal 1, validators.count(&:keys?)
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/${username}/file.${group}.log' has placeholders, but chunk keys doesn't have keys group,username") do
          validators.first.validate!
        end
      end

      test 'returns a validator for variable keys if a plugin is configured with variable keys' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'username,group')]))
        validators = @i.placeholder_validators(:path, "/my/path/file.log")
        assert_equal 1, validators.size
        assert_equal 1, validators.count(&:keys?)
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.log' doesn't have enough placeholders for keys group,username") do
          validators.first.validate!
        end
      end
    end

    sub_test_case '#placeholder_validate!' do
      test 'raises configuration error for a template when timestamp placeholders exist but time key is missing' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /path/without/timestamp/file.%Y%m%d-%H%M.log' has timestamp placeholders, but chunk key 'time' is not configured") do
          @i.placeholder_validate!(:path, "/path/without/timestamp/file.%Y%m%d-%H%M.log")
        end
      end

      test 'raises configuration error for a template without timestamp placeholders when timekey is configured' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {"timekey" => 180})]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.log' doesn't have timestamp placeholders for timekey 180") do
          @i.placeholder_validate!(:path, "/my/path/file.log")
        end
        assert_nothing_raised do
          @i.placeholder_validate!(:path, "/my/path/%Y%m%d/file.%H%M.log")
        end
      end

      test 'raises configuration error for a template with timestamp placeholders when plugin is configured more fine timekey' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {"timekey" => 180})]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.%Y%m%d_%H.log' doesn't have timestamp placeholder for hour('%H') for timekey 180") do
          @i.placeholder_validate!(:path, "/my/path/file.%Y%m%d_%H.log")
        end
        assert_nothing_raised do
          @i.placeholder_validate!(:path, "/my/path/file.%Y%m%d_%H%M.log")
        end
      end

      test 'raises configuration error for a template when tag placeholders exist but tag key is missing' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/${tag}/file.${tag[2]}.log' has tag placeholders, but chunk key 'tag' is not configured") do
          @i.placeholder_validate!(:path, "/my/path/${tag}/file.${tag[2]}.log")
        end
      end

      test 'raises configuration error for a template without tag placeholders when tagkey is configured' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'tag')]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.log' doesn't have tag placeholder") do
          @i.placeholder_validate!(:path, "/my/path/file.log")
        end
        assert_nothing_raised do
          @i.placeholder_validate!(:path, "/my/path/${tag}/file.${tag[2]}.log")
        end
        assert_nothing_raised do
          @i.placeholder_validate!(:path, "/my/path/${tag}/file.${tag[-1]}.log")
        end
      end

      test 'raises configuration error for a template when variable key placeholders exist but chunk keys are missing' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/${service}/file.${username}.log' has placeholders, but chunk keys doesn't have keys service,username") do
          @i.placeholder_validate!(:path, "/my/path/${service}/file.${username}.log")
        end
      end

      test 'raises configuration error for a template without variable key placeholders when chunk keys are configured' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'username,service')]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.log' doesn't have enough placeholders for keys service,username") do
          @i.placeholder_validate!(:path, "/my/path/file.log")
        end
        assert_nothing_raised do
          @i.placeholder_validate!(:path, "/my/path/${service}/file.${username}.log")
        end
      end

      test 'raise configuration error for a template and configuration with keys mismatch' do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'username,service')]))
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/file.${username}.log' doesn't have enough placeholders for keys service") do
          @i.placeholder_validate!(:path, "/my/path/file.${username}.log")
        end
        assert_raise Fluent::ConfigError.new("Parameter 'path: /my/path/${service}/file.log' doesn't have enough placeholders for keys username") do
          @i.placeholder_validate!(:path, "/my/path/${service}/file.log")
        end
        assert_nothing_raised do
          @i.placeholder_validate!(:path, "/my/path/${service}/file.${username}.log")
        end
      end
    end

    test '#get_placeholders_time returns seconds,title and example placeholder for a template' do
      s, t, e = @i.get_placeholders_time("/path/to/dir/yay")
      assert_nil s
      assert_nil t
      assert_nil e

      s, t, e = @i.get_placeholders_time("/path/to/%Y%m%d/yay")
      assert_equal 86400, s
      assert_equal :day, t
      assert_equal '%d', e
      s, t, e = @i.get_placeholders_time("my birthday! at %F")
      assert_equal 86400, s
      assert_equal :day, t
      assert_equal '%d', e

      s, t, e = @i.get_placeholders_time("myfile.%Y-%m-%d_%H.log")
      assert_equal 3600, s
      assert_equal :hour, t
      assert_equal '%H', e

      s, t, e = @i.get_placeholders_time("part-%Y%m%d-%H%M.ts")
      assert_equal 60, s
      assert_equal :minute, t
      assert_equal '%M', e

      s, t, e = @i.get_placeholders_time("my first data at %F %T %z")
      assert_equal 1, s
      assert_equal :second, t
      assert_equal '%S', e
    end

    test '#get_placeholders_tag returns a list of tag part position for a template' do
      assert_equal [], @i.get_placeholders_tag("db.table")
      assert_equal [], @i.get_placeholders_tag("db.table_${non_tag}")
      assert_equal [-1], @i.get_placeholders_tag("table_${tag}")
      assert_equal [0, 1], @i.get_placeholders_tag("db_${tag[0]}.table_${tag[1]}")
      assert_equal [-1, 0], @i.get_placeholders_tag("/treedir/${tag[0]}/${tag}")
    end

    test '#get_placeholders_keys returns a list of keys for a template' do
      assert_equal [], @i.get_placeholders_keys("/path/to/my/data/file.log")
      assert_equal [], @i.get_placeholders_keys("/path/to/my/${tag}/file.log")
      assert_equal ['key1', 'key2'], @i.get_placeholders_keys("/path/to/${key2}/${tag}/file.${key1}.log")
      assert_equal ['.hidden', '0001', '@timestamp', 'a_key', 'my-domain'], @i.get_placeholders_keys("http://${my-domain}/${.hidden}/${0001}/${a_key}?timestamp=${@timestamp}")
    end

    data('include space' => 'ke y',
         'bracket notation' => "$['key']",
         'invalid notation' => "$.ke y")
    test 'configure checks invalid chunk keys' do |chunk_keys|
      i = create_output(:buffered)
      assert_raise Fluent::ConfigError do
        i.configure(config_element('ROOT' , '', {}, [config_element('buffer', chunk_keys)]))
      end
    end

    test '#metadata returns object which contains tag/timekey/variables from records as specified in configuration' do
      tag = 'test.output'
      time = event_time('2016-04-12 15:31:23 -0700')
      timekey = event_time('2016-04-12 15:00:00 -0700')
      record = {"key1" => "value1", "num1" => 1, "message" => "my message", "nest" => {"key" => "nested value"}}

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

      i9 = create_output(:buffered)
      i9.configure(config_element('ROOT','',{},[config_element('buffer', 'key1,$.nest.key', {})]))
      assert_equal create_metadata(variables: {:key1 => "value1", :"$.nest.key" => 'nested value'}), i9.metadata(tag, time, record)
    end

    test '#emit calls #process via #emit_sync for non-buffered output' do
      i = create_output(:sync)
      process_called = false
      i.register(:process){|tag, es| process_called = true }
      i.configure(config_element())
      i.start
      i.after_start

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
      i.after_start

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
      i.after_start

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
      i.after_start

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
      i.after_start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until write_called }

      assert write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test 'output plugin will call #try_write for plugin supports delayed commit only to flush buffer chunks' do
      tmp_dir = File.join(__dir__, '../tmp/test_output')

      i = create_output(:delayed)
      i.system_config_override(root_dir: tmp_dir) # Backup files are generated in `tmp_dir`.
      try_write_called = false
      i.register(:try_write){|chunk| try_write_called = true; commit_write(chunk.unique_id) }

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.start
      i.after_start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until try_write_called }

      assert try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    ensure
      FileUtils.rm_rf(tmp_dir)
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
      i.after_start

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
      tmp_dir = File.join(__dir__, '../tmp/test_output')

      i = create_output(:full)
      i.system_config_override(root_dir: tmp_dir) # Backup files are generated in `tmp_dir`.
      write_called = false
      try_write_called = false
      i.register(:write){ |chunk| write_called = true }
      i.register(:try_write){|chunk| try_write_called = true; commit_write(chunk.unique_id) }

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.register(:prefer_delayed_commit){ true } # delayed decision is possible to change after (output's) configure
      i.start
      i.after_start

      assert i.prefer_delayed_commit

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4){ Thread.pass until write_called || try_write_called }

      assert !write_called
      assert try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    ensure
      FileUtils.rm_rf(tmp_dir)
    end

    test 'flush_interval is ignored when flush_mode is not interval' do
      mock(@i.log).warn("'flush_interval' is ignored because default 'flush_mode' is not 'interval': 'lazy'")
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {'timekey' => 60*30, 'flush_interval' => 10})]))
    end

    data(:lazy => 'lazy', :immediate => 'immediate')
    test 'flush_interval and non-interval flush_mode is exclusive ' do |mode|
      assert_raise Fluent::ConfigError.new("'flush_interval' can't be specified when 'flush_mode' is not 'interval' explicitly: '#{mode}'") do
        @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {'flush_mode' => mode, 'flush_interval' => 10})]))
      end
    end

    test 'flush_mode is set to interval when flush_interval with v0.12 configuration is given' do
      mock(@i.log).info("'flush_interval' is configured at out side of <buffer>. 'flush_mode' is set to 'interval' to keep existing behaviour")
      @i.configure(config_element('ROOT', '', {'flush_interval' => 60}, []))
      assert_equal :interval, @i.instance_variable_get(:@flush_mode)
    end

    sub_test_case 'configure secondary' do
      test "Warn if primary type is different from secondary type and either primary or secondary has custom_format" do
        o = create_output(:buffered)
        mock(o.log).warn("Use different plugin for secondary. Check the plugin works with primary like secondary_file",
                         primary: o.class.to_s, secondary: "Fluent::Plugin::TestOutput")

        o.configure(config_element('ROOT','',{},[config_element('secondary','',{'@type'=>'test', 'name' => "cool"})]))
        assert_not_nil o.instance_variable_get(:@secondary)
      end

      test "don't warn if primary type is the same as secondary type" do
        o = Fluent::Plugin::TestOutput.new
        mock(o.log).warn("Use different plugin for secondary. Check the plugin works with primary like secondary_file",
                         primary: o.class.to_s, secondary: "Fluent::Plugin::TestOutput" ).never

        o.configure(config_element('ROOT','',{'name' => "cool2"},
                                   [config_element('secondary','',{'@type'=>'test', 'name' => "cool"}),
                                    config_element('buffer','',{'@type'=>'memory'})]
                                  ))
        assert_not_nil o.instance_variable_get(:@secondary)
      end

      test "don't warn if primary type is different from secondary type and both don't have custom_format" do
        o = create_output(:standard)
        mock(o.log).warn("Use different plugin for secondary. Check the plugin works with primary like secondary_file",
                         primary: o.class.to_s, secondary: "Fluent::Plugin::TestOutput").never

        o.configure(config_element('ROOT','',{},[config_element('secondary','',{'@type'=>'test', 'name' => "cool"})]))
        assert_not_nil o.instance_variable_get(:@secondary)
      end

      test "raise configuration error if secondary type specifies non buffered output" do
        o = create_output(:standard)
        assert_raise Fluent::ConfigError do
          o.configure(config_element('ROOT','',{},[config_element('secondary','',{'@type'=>'copy'})]))
        end
      end
    end
  end

  test 'raises an error if timekey is less than equal 0' do
    i = create_output(:delayed)
    assert_raise Fluent::ConfigError.new("<buffer ...> argument includes 'time', but timekey is not configured") do
      i.configure(config_element('ROOT','',{},[config_element('buffer', 'time', { "timekey" => nil })]))
    end

    i = create_output(:delayed)
    assert_raise Fluent::ConfigError.new('timekey should be greater than 0. current timekey: 0.0') do
      i.configure(config_element('ROOT','',{},[config_element('buffer', 'time', { "timekey" => 0 })]))
    end

    i = create_output(:delayed)
    assert_raise Fluent::ConfigError.new('timekey should be greater than 0. current timekey: -1.0') do
      i.configure(config_element('ROOT','',{},[config_element('buffer', 'time', { "timekey" => -1 })]))
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
      @i.after_start

      t = event_time()
      es = Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])
      5.times do
        @i.emit_events('tag', es)
      end
      assert_equal 5, ary.size

      @i.stop; @i.before_shutdown; @i.shutdown; @i.after_shutdown; @i.close; @i.terminate
    end
  end

  sub_test_case '#generate_format_proc' do
    test "when output doesn't have <buffer>" do
      i = create_output(:sync)
      i.configure(config_element('ROOT', '', {}, []))
      assert_equal Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM, i.generate_format_proc
    end

    test "when output doesn't have <buffer> and time_as_integer is true" do
      i = create_output(:sync)
      i.configure(config_element('ROOT', '', {'time_as_integer' => true}))
      assert_equal Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM_TIME_INT, i.generate_format_proc
    end

    test 'when output has <buffer> and compress is gzip' do
      i = create_output(:buffered)
      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {'compress' => 'gzip'})]))
      assert_equal Fluent::Plugin::Output::FORMAT_COMPRESSED_MSGPACK_STREAM, i.generate_format_proc
    end

    test 'when output has <buffer> and compress is gzip and time_as_integer is true' do
      i = create_output(:buffered)
      i.configure(config_element('ROOT', '', {'time_as_integer' => true}, [config_element('buffer', '', {'compress' => 'gzip'})]))
      assert_equal Fluent::Plugin::Output::FORMAT_COMPRESSED_MSGPACK_STREAM_TIME_INT, i.generate_format_proc
    end

    test 'when output has <buffer> and compress is text' do
      i = create_output(:buffered)
      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {'compress' => 'text'})]))
      assert_equal Fluent::Plugin::Output::FORMAT_MSGPACK_STREAM, i.generate_format_proc
    end
  end

  sub_test_case 'slow_flush_log_threshold' do
    def invoke_slow_flush_log_threshold_test(i)
      i.configure(config_element('ROOT', '', {'slow_flush_log_threshold' => 0.5},
                                 [config_element('buffer', '', {"flush_mode" => "immediate", "flush_thread_interval" => 30})]))
      i.start
      i.after_start

      t = event_time()
      i.emit_events('tag', Fluent::ArrayEventStream.new([ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]))
      i.force_flush

      waiting(4) { Thread.pass until i.test_finished? }

      yield
    ensure
      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#write flush took longer time than slow_flush_log_threshold' do
      i = create_output(:buffered)
      write_called = false
      i.register(:write) { |chunk| sleep 3 }
      i.define_singleton_method(:test_finished?) { write_called }
      i.define_singleton_method(:try_flush) { super(); write_called = true }

      invoke_slow_flush_log_threshold_test(i) {
        assert write_called
        logs = i.log.out.logs
        assert{ logs.any?{|log| log.include?("buffer flush took longer time than slow_flush_log_threshold: elapsed_time") } }
      }
    end

    test '#try_write flush took longer time than slow_flush_log_threshold' do
      i = create_output(:delayed)
      try_write_called = false
      i.register(:try_write){ |chunk| sleep 3 }
      i.define_singleton_method(:test_finished?) { try_write_called }
      i.define_singleton_method(:try_flush) { super(); try_write_called = true }

      invoke_slow_flush_log_threshold_test(i) {
        assert try_write_called
        logs = i.log.out.logs
        assert{ logs.any?{|log| log.include?("buffer flush took longer time than slow_flush_log_threshold: elapsed_time") } }
      }
    end
  end

  sub_test_case "actual_flush_thread_count" do
    data(
      "Not buffered",
      {
        output_type: :sync,
        config: config_element(),
        expected: 0,
      }
    )
    data(
      "Buffered with singile thread",
      {
        output_type: :full,
        config: config_element("ROOT", "", {}, [config_element("buffer", "", {})]),
        expected: 1,
      }
    )
    data(
      "Buffered with multiple threads",
      {
        output_type: :full,
        config: config_element("ROOT", "", {}, [config_element("buffer", "", {"flush_thread_count" => 8})]),
        expected: 8,
      }
    )
    test "actual_flush_thread_count" do |data|
      o = create_output(data[:output_type])
      o.configure(data[:config])
      assert_equal data[:expected], o.actual_flush_thread_count
    end

    data(
      "Buffered with single thread",
      {
        output_type: :full,
        config: config_element(
          "ROOT", "", {},
          [
            config_element("buffer", "", {}), 
            config_element("secondary", "", {"@type" => "test", "name" => "test"}),
          ]
        ),
        expected: 1,
      }
    )
    data(
      "Buffered with multiple threads",
      {
        output_type: :full,
        config: config_element(
          "ROOT", "", {},
          [
            config_element("buffer", "", {"flush_thread_count" => 8}),
            config_element("secondary", "", {"@type" => "test", "name" => "test"}),
          ]
        ),
        expected: 8,
      }
    )
    test "actual_flush_thread_count for secondary" do |data|
      primary = create_output(data[:output_type])
      primary.configure(data[:config])
      assert_equal data[:expected], primary.secondary.actual_flush_thread_count
    end
  end

  sub_test_case "synchronize_path" do
    def setup
      Dir.mktmpdir do |lock_dir|
        ENV['FLUENTD_LOCK_DIR'] = lock_dir
        yield
      end
    end

    def assert_worker_lock(lock_path, expect_locked)
      # With LOCK_NB set, flock() returns:
      #   * `false` when the file is already locked.
      #   * `0` when the file is not locked.
      File.open(lock_path, "w") do |f|
        if expect_locked
          assert_equal false, f.flock(File::LOCK_EX|File::LOCK_NB)
        else
          assert_equal 0, f.flock(File::LOCK_EX|File::LOCK_NB)
        end
      end
    end

    def assert_thread_lock(output_plugin, expect_locked)
      t = Thread.new do
        output_plugin.synchronize_path("test") do
        end
      end
      if expect_locked
        assert_nil t.join(3)
      else
        assert_not_nil t.join(3)
      end
    end

    data(
      "Not buffered with single worker",
      {
        output_type: :sync,
        config: config_element(),
        workers: 1,
        expect_worker_lock: false,
        expect_thread_lock: false,
      }
    )
    data(
      "Not buffered with multiple workers",
      {
        output_type: :sync,
        config: config_element(),
        workers: 4,
        expect_worker_lock: true,
        expect_thread_lock: false,
      }
    )
    data(
      "Buffered with single thread and single worker",
      {
        output_type: :full,
        config: config_element("ROOT", "", {}, [config_element("buffer", "", {})]),
        workers: 1,
        expect_worker_lock: false,
        expect_thread_lock: false,
      }
    )
    data(
      "Buffered with multiple threads and single worker",
      {
        output_type: :full,
        config: config_element("ROOT", "", {}, [config_element("buffer", "", {"flush_thread_count" => 8})]),
        workers: 1,
        expect_worker_lock: false,
        expect_thread_lock: true,
      }
    )
    data(
      "Buffered with single thread and multiple workers",
      {
        output_type: :full,
        config: config_element("ROOT", "", {}, [config_element("buffer", "", {})]),
        workers: 4,
        expect_worker_lock: true,
        expect_thread_lock: false,
      }
    )
    data(
      "Buffered with multiple threads and multiple workers",
      {
        output_type: :full,
        config: config_element("ROOT", "", {}, [config_element("buffer", "", {"flush_thread_count" => 8})]),
        workers: 4,
        expect_worker_lock: true,
        expect_thread_lock: true,
      }
    )
    test "synchronize_path" do |data|
      o = create_output(data[:output_type])
      o.configure(data[:config])
      o.system_config_override(workers: data[:workers])

      test_lock_name = "test_lock_name"
      lock_path = o.get_lock_path(test_lock_name)

      o.synchronize_path(test_lock_name) do
        assert_worker_lock(lock_path, data[:expect_worker_lock])
        assert_thread_lock(o, data[:expect_thread_lock])
      end

      assert_worker_lock(lock_path, false)
      assert_thread_lock(o, false)
    end

    data(
      "Buffered with single thread and single worker",
      {
        output_type: :full,
        config: config_element(
          "ROOT", "", {},
          [
            config_element("buffer", "", {}),
            config_element("secondary", "", {"@type" => "test", "name" => "test"}),
          ]
        ),
        workers: 1,
        expect_worker_lock: false,
        expect_thread_lock: false,
      }
    )
    data(
      "Buffered with multiple threads and single worker",
      {
        output_type: :full,
        config: config_element(
          "ROOT", "", {},
          [
            config_element("buffer", "", {"flush_thread_count" => 8}),
            config_element("secondary", "", {"@type" => "test", "name" => "test"}),
          ]
        ),
        workers: 1,
        expect_worker_lock: false,
        expect_thread_lock: true,
      }
    )
    data(
      "Buffered with single thread and multiple workers",
      {
        output_type: :full,
        config: config_element(
          "ROOT", "", {},
          [
            config_element("buffer", "", {}),
            config_element("secondary", "", {"@type" => "test", "name" => "test"}),
          ]
        ),
        workers: 4,
        expect_worker_lock: true,
        expect_thread_lock: false,
      }
    )
    data(
      "Buffered with multiple threads and multiple workers",
      {
        output_type: :full,
        config: config_element(
          "ROOT", "", {},
          [
            config_element("buffer", "", {"flush_thread_count" => 8}),
            config_element("secondary", "", {"@type" => "test", "name" => "test"}),
          ]
        ),
        workers: 4,
        expect_worker_lock: true,
        expect_thread_lock: true,
      }
    )
    test "synchronize_path for secondary" do |data|
      primary = create_output(data[:output_type])
      primary.configure(data[:config])
      secondary = primary.secondary
      secondary.system_config_override(workers: data[:workers])

      test_lock_name = "test_lock_name"
      lock_path = secondary.get_lock_path(test_lock_name)

      secondary.synchronize_path(test_lock_name) do
        assert_worker_lock(lock_path, data[:expect_worker_lock])
        assert_thread_lock(secondary, data[:expect_thread_lock])
      end

      assert_worker_lock(lock_path, false)
      assert_thread_lock(secondary, false)
    end
  end
end
