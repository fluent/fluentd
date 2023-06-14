require_relative '../helper'
require 'fluent/plugin/multi_output'
require 'fluent/event'

require 'json'
require 'time'
require 'timeout'

module FluentPluginMultiOutputTest
  class DummyMultiOutput < Fluent::Plugin::MultiOutput
    attr_reader :events
    def initialize
      super
      @events = []
    end
    def configure(conf)
      super
    end
    def process(tag, es)
      es.each do |time, record|
        @events << [tag, time, record]
      end
    end
  end
  class DummyCompatMultiOutput < Fluent::Plugin::MultiOutput
    def initialize
      super
      @compat = true
    end
    def configure(conf)
      super
    end
    def process(tag, es)
      # ...
    end
  end

  class Dummy1Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('dummy_test_multi_output_1', self)
    attr_reader :configured
    def configure(conf)
      super
      @configured = true
    end
    def process(tag, es)
    end
  end
  class Dummy2Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('dummy_test_multi_output_2', self)
    attr_reader :configured
    def configure(conf)
      super
      @configured = true
    end
    def process(tag, es)
    end
  end
  class Dummy3Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('dummy_test_multi_output_3', self)
    attr_reader :configured
    def configure(conf)
      super
      @configured = true
    end
    def process(tag, es)
    end
  end
  class Dummy4Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('dummy_test_multi_output_4', self)
    attr_reader :configured
    def configure(conf)
      super
      @configured = true
    end
    def process(tag, es)
    end
  end
end

class MultiOutputTest < Test::Unit::TestCase
  def create_output(type=:multi)
    case type
    when :compat_multi
      FluentPluginMultiOutputTest::DummyCompatMultiOutput.new
    else
      FluentPluginMultiOutputTest::DummyMultiOutput.new
    end
  end

  sub_test_case 'basic multi output plugin' do
    setup do
      Fluent::Test.setup
      @i = create_output()
    end

    teardown do
      @i.log.out.reset
    end

    test '#configure raises error if <store> sections are missing' do
      conf = config_element('ROOT', '', { '@type' => 'dummy_test_multi_output' }, [])
      assert_raise Fluent::ConfigError do
        @i.configure(conf)
      end
    end

    test '#configure initialize child plugins and call these #configure' do
      assert_equal [], @i.outputs

      conf = config_element('ROOT', '', { '@type' => 'dummy_test_multi_output' },
        [
          config_element('store', '', { '@type' => 'dummy_test_multi_output_1' }),
          config_element('store', '', { '@type' => 'dummy_test_multi_output_2' }),
          config_element('store', '', { '@type' => 'dummy_test_multi_output_3' }),
          config_element('store', '', { '@type' => 'dummy_test_multi_output_4' }),
        ]
      )
      @i.configure(conf)

      assert_equal 4, @i.outputs.size

      assert @i.outputs[0].is_a? FluentPluginMultiOutputTest::Dummy1Output
      assert @i.outputs[0].configured

      assert @i.outputs[1].is_a? FluentPluginMultiOutputTest::Dummy2Output
      assert @i.outputs[1].configured

      assert @i.outputs[2].is_a? FluentPluginMultiOutputTest::Dummy3Output
      assert @i.outputs[2].configured

      assert @i.outputs[3].is_a? FluentPluginMultiOutputTest::Dummy4Output
      assert @i.outputs[3].configured
    end

    test '#configure warns if "type" is used in <store> sections instead of "@type"' do
      assert_equal [], @i.log.out.logs

      conf = config_element('ROOT', '', { '@type' => 'dummy_test_multi_output' },
        [
          config_element('store', '', { 'type' => 'dummy_test_multi_output_1' }),
          config_element('store', '', { 'type' => 'dummy_test_multi_output_2' }),
          config_element('store', '', { 'type' => 'dummy_test_multi_output_3' }),
          config_element('store', '', { 'type' => 'dummy_test_multi_output_4' }),
        ]
      )
      @i.configure(conf)
      assert_equal 4, @i.outputs.size

      log_size_for_multi_output_itself = 4
      log_size_for_metrics_plugin_helper = 4
      expected_warn_log_size = log_size_for_multi_output_itself + log_size_for_metrics_plugin_helper
      logs = @i.log.out.logs
      assert{ logs.count{|log| log.include?('[warn]') && log.include?("'type' is deprecated parameter name. use '@type' instead.") } == expected_warn_log_size }
    end

    test '#emit_events calls #process always' do
      conf = config_element('ROOT', '', { '@type' => 'dummy_test_multi_output' },
        [
          config_element('store', '', { '@type' => 'dummy_test_multi_output_1' }),
          config_element('store', '', { '@type' => 'dummy_test_multi_output_2' }),
          config_element('store', '', { '@type' => 'dummy_test_multi_output_3' }),
          config_element('store', '', { '@type' => 'dummy_test_multi_output_4' }),
        ]
      )
      @i.configure(conf)
      @i.start

      assert @i.events.empty?

      @i.emit_events(
        'test.tag',
        Fluent::ArrayEventStream.new(
          [
            [event_time(), {"message" => "multi test 1"}],
            [event_time(), {"message" => "multi test 1"}],
          ]
        )
      )

      assert_equal 2, @i.events.size
    end

    test 'can use metrics plugins and fallback methods' do
      conf = config_element('ROOT', '', { '@type' => 'dummy_test_multi_output' },
        [
          config_element('store', '', { 'type' => 'dummy_test_multi_output_1' }),
          config_element('store', '', { 'type' => 'dummy_test_multi_output_2' }),
          config_element('store', '', { 'type' => 'dummy_test_multi_output_3' }),
          config_element('store', '', { 'type' => 'dummy_test_multi_output_4' }),
        ]
      )
      @i.configure(conf)

      %w[num_errors_metrics emit_count_metrics emit_size_metrics emit_records_metrics].each do |metric_name|
        assert_true @i.instance_variable_get(:"@#{metric_name}").is_a?(Fluent::Plugin::Metrics)
      end

      assert_equal 0, @i.num_errors
      assert_equal 0, @i.emit_count
      assert_equal 0, @i.emit_size
      assert_equal 0, @i.emit_records
    end
  end
end
