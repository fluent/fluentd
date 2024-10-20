require_relative '../helper'

require 'test-unit'
require 'open3'
require 'fluent/plugin/output'
require 'fluent/plugin/in_forward'
require 'fluent/plugin/out_secondary_file'
require 'fluent/test/driver/output'
require 'fluent/test/driver/input'

class TestFluentCat < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.mkdir_p(TMP_DIR)
    @record = { 'key' => 'value' }
    @time = event_time
    @es = Fluent::OneEventStream.new(@time, @record)
    @primary = create_primary
    metadata = @primary.buffer.new_metadata
    @chunk = create_chunk(@primary, metadata, @es)
    @port = unused_port(protocol: :tcp)
  end

  def teardown
    FileUtils.rm_rf(TMP_DIR)
    @port = nil
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/command/fluent_cat#{ENV['TEST_ENV_NUMBER']}")
  FLUENT_CAT_COMMAND = File.expand_path(File.dirname(__FILE__) + "/../../bin/fluent-cat")

  def config
    %[
      port #{@port}
      bind 127.0.0.1
    ]
  end

  SECONDARY_CONFIG = %[
    directory #{TMP_DIR}
  ]

  class DummyOutput < Fluent::Plugin::Output
    def write(chunk); end
  end

  def create_driver(conf=config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ForwardInput).configure(conf)
  end

  def create_primary(buffer_cofig = config_element('buffer'))
    DummyOutput.new.configure(config_element('ROOT','',{}, [buffer_cofig]))
  end

  def create_secondary_driver(conf=SECONDARY_CONFIG)
    c = Fluent::Test::Driver::Output.new(Fluent::Plugin::SecondaryFileOutput)
    c.instance.acts_as_secondary(@primary)
    c.configure(conf)
  end

  def create_chunk(primary, metadata, es)
    primary.buffer.generate_chunk(metadata).tap do |c|
      c.concat(es.to_msgpack_stream, es.size)
      c.commit
    end
  end

  sub_test_case "json" do
    def test_cat_json
      d = create_driver
      d.run(expect_records: 1) do
        Open3.pipeline_w("#{ServerEngine.ruby_bin_path} #{FLUENT_CAT_COMMAND} --port #{@port} json") do |stdin|
          stdin.puts('{"key":"value"}')
          stdin.close
        end
      end
      event = d.events.first
      assert_equal([1, "json", @record],
                   [d.events.size, event.first, event.last])
    end
  end

  sub_test_case "msgpack" do
    def test_cat_secondary_file
      d = create_secondary_driver
      path = d.instance.write(@chunk)
      d = create_driver
      d.run(expect_records: 1) do
        Open3.pipeline_w("#{ServerEngine.ruby_bin_path} #{FLUENT_CAT_COMMAND} --port #{@port} --format msgpack secondary") do |stdin|
          stdin.write(File.read(path))
          stdin.close
        end
      end
      event = d.events.first
      assert_equal([1, "secondary", @record],
                   [d.events.size, event.first, event.last])
    end
  end

  sub_test_case "send specific event time" do
    def test_without_event_time
      event_time = Fluent::EventTime.now
      d = create_driver
      d.run(expect_records: 1) do
        Open3.pipeline_w("#{ServerEngine.ruby_bin_path} #{FLUENT_CAT_COMMAND} --port #{@port} tag") do |stdin|
          stdin.puts('{"key":"value"}')
          stdin.close
        end
      end
      event = d.events.first
      assert_in_delta(event_time.to_f, event[1].to_f, 3.0) # expect command to be finished in 3 seconds
      assert_equal([1, "tag", true, @record],
                   [d.events.size, event.first, event_time.to_f < event[1].to_f, event.last])
    end

    def test_with_event_time
      event_time = "2021-01-02 13:14:15.0+00:00"
      d = create_driver
      d.run(expect_records: 1) do
        Open3.pipeline_w("#{ServerEngine.ruby_bin_path} #{FLUENT_CAT_COMMAND} --port #{@port} --event-time '#{event_time}' tag") do |stdin|
          stdin.puts('{"key":"value"}')
          stdin.close
        end
      end
      assert_equal([["tag", Fluent::EventTime.parse(event_time), @record]], d.events)
    end
  end
end
