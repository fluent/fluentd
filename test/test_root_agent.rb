require_relative 'helper'
require 'fluent/event_router'
require 'fluent/system_config'
require_relative 'test_plugin_classes'

class RootAgentTest < ::Test::Unit::TestCase
  include Fluent
  include FluentTest

  def test_initialize
    ra = RootAgent.new(log: $log)
    assert_equal 0, ra.instance_variable_get(:@suppress_emit_error_log_interval)
    assert_nil ra.instance_variable_get(:@next_emit_error_log_time)
  end

  data(
    'suppress interval' => [{'emit_error_log_interval' => 30}, {:@suppress_emit_error_log_interval => 30}],
    'without source' => [{'without_source' => true}, {:@without_source => true}]
    )
  def test_initialize_with_opt(data)
    opt, expected = data
    ra = RootAgent.new(log: $log, system_config: SystemConfig.new(opt))
    expected.each { |k, v|
      assert_equal v, ra.instance_variable_get(k)
    }
  end

  sub_test_case 'configure' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent { @ra }
    end

    def configure_ra(conf_str)
      conf = Config.parse(conf_str, "(test)", "(test_dir)", true)
      @ra.configure(conf)
      @ra
    end

    test 'empty' do
      ra = configure_ra('')
      assert_empty ra.inputs
      assert_empty ra.labels
      assert_empty ra.outputs
      assert_empty ra.filters
      assert_nil ra.context
      assert_nil ra.error_collector
    end

    test 'raises configuration error for missing type of source' do
      conf = <<-EOC
<source>
</source>
EOC
      errmsg = "Missing '@type' parameter on <source> directive"
      assert_raise Fluent::ConfigError.new(errmsg) do
        configure_ra(conf)
      end
    end

    test 'raises configuration error for missing type of match' do
      conf = <<-EOC
<source>
  @type test_in
</source>
<match *.**>
</match>
EOC
      errmsg = "Missing '@type' parameter on <match> directive"
      assert_raise Fluent::ConfigError.new(errmsg) do
        configure_ra(conf)
      end
    end

    test 'raises configuration error for missing type of filter' do
      conf = <<-EOC
<source>
  @type test_in
</source>
<filter *.**>
</filter>
<match *.**>
  @type test_out
</match>
EOC
      errmsg = "Missing '@type' parameter on <filter> directive"
      assert_raise Fluent::ConfigError.new(errmsg) do
        configure_ra(conf)
      end
    end

    test 'raises configuration error if there are two same label section' do
      conf = <<-EOC
<source>
  @type test_in
  @label @test
</source>
<label @test>
  @type test_out
</label>
<label @test>
  @type test_out
</label>
EOC
      errmsg = "Section <label @test> appears twice"
      assert_raise Fluent::ConfigError.new(errmsg) do
        configure_ra(conf)
      end
    end

    test 'raises configuration error if there are not match sections in label section' do
      conf = <<-EOC
<source>
  @type test_in
  @label @test
</source>
<label @test>
  @type test_out
</label>
EOC
      errmsg = "Missing <match> sections in <label @test> section"
      assert_raise Fluent::ConfigError.new(errmsg) do
        configure_ra(conf)
      end
    end

    test 'with plugins' do
      # check @type and type in one configuration
      conf = <<-EOC
<source>
  @type test_in
  @id test_in
</source>
<filter>
  type test_filter
  id test_filter
</filter>
<match **>
  @type relabel
  @id test_relabel
  @label @test
</match>
<label @test>
  <match **>
    type test_out
    id test_out
  </match>
</label>
<label @ERROR>
  <match>
    @type null
  </match>
</label>
EOC
      ra = configure_ra(conf)
      assert_kind_of FluentTestInput, ra.inputs.first
      assert_kind_of Plugin::RelabelOutput, ra.outputs.first
      assert_kind_of FluentTestFilter, ra.filters.first
      assert ra.error_collector

      %W(@test @ERROR).each { |label_symbol|
        assert_include ra.labels, label_symbol
        assert_kind_of Label, ra.labels[label_symbol]
      }

      test_label = ra.labels['@test']
      assert_kind_of FluentTestOutput, test_label.outputs.first
      assert_equal ra, test_label.root_agent

      error_label = ra.labels['@ERROR']
      assert_kind_of Fluent::Plugin::NullOutput, error_label.outputs.first
      assert_kind_of RootAgent::RootAgentProxyWithoutErrorCollector, error_label.root_agent
    end
  end

  sub_test_case 'start/shutdown' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent { @ra }
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
<source>
  @type test_in
  @id test_in
</source>
<filter>
  type test_filter
  id test_filter
</filter>
<match **>
  @type test_out
  @id test_out
</match>
EOC
      @ra
    end

    test 'plugin status' do
      @ra.start
      assert_true @ra.inputs.first.started
      assert_true @ra.filters.first.started
      assert_true @ra.outputs.first.started

      @ra.shutdown
      assert_false @ra.inputs.first.started
      assert_false @ra.filters.first.started
      assert_false @ra.outputs.first.started
    end
  end

  sub_test_case 'configured with MultiOutput plugins' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent { @ra }
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
<source>
  @type test_in
  @id test_in
</source>
<filter>
  @type test_filter
  @id test_filter
</filter>
<match **>
  @type copy
  @id test_copy
  <store>
    @type test_out
    @id test_out1
  </store>
  <store>
    @type test_out
    @id test_out2
  </store>
</match>
EOC
      @ra
    end

    test 'plugin status with multi output' do
      assert_equal 1, @ra.inputs.size
      assert_equal 1, @ra.filters.size
      assert_equal 3, @ra.outputs.size

      @ra.start
      assert_equal [true], @ra.inputs.map{|i| i.started? }
      assert_equal [true], @ra.filters.map{|i| i.started? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.started? }

      assert_equal [true], @ra.inputs.map{|i| i.after_started? }
      assert_equal [true], @ra.filters.map{|i| i.after_started? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.after_started? }

      @ra.shutdown
      assert_equal [true], @ra.inputs.map{|i| i.stopped? }
      assert_equal [true], @ra.filters.map{|i| i.stopped? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.stopped? }

      assert_equal [true], @ra.inputs.map{|i| i.before_shutdown? }
      assert_equal [true], @ra.filters.map{|i| i.before_shutdown? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.before_shutdown? }

      assert_equal [true], @ra.inputs.map{|i| i.shutdown? }
      assert_equal [true], @ra.filters.map{|i| i.shutdown? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.shutdown? }

      assert_equal [true], @ra.inputs.map{|i| i.after_shutdown? }
      assert_equal [true], @ra.filters.map{|i| i.after_shutdown? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.after_shutdown? }

      assert_equal [true], @ra.inputs.map{|i| i.closed? }
      assert_equal [true], @ra.filters.map{|i| i.closed? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.closed? }

      assert_equal [true], @ra.inputs.map{|i| i.terminated? }
      assert_equal [true], @ra.filters.map{|i| i.terminated? }
      assert_equal [true, true, true], @ra.outputs.map{|i| i.terminated? }
    end
  end

  sub_test_case 'configured with MultiOutput plugins and labels' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent { @ra }
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
<source>
  @type test_in
  @id test_in
  @label @testing
</source>
<label @testing>
  <filter>
    @type test_filter
    @id test_filter
  </filter>
  <match **>
    @type copy
    @id test_copy
    <store>
      @type test_out
      @id test_out1
    </store>
    <store>
      @type test_out
      @id test_out2
    </store>
  </match>
</label>
EOC
      @ra
    end

    test 'plugin status with multi output' do
      assert_equal 1, @ra.inputs.size
      assert_equal 0, @ra.filters.size
      assert_equal 0, @ra.outputs.size
      assert_equal 1, @ra.labels.size
      assert_equal '@testing', @ra.labels.keys.first
      assert_equal 1, @ra.labels.values.first.filters.size
      assert_equal 3, @ra.labels.values.first.outputs.size

      label_filters = @ra.labels.values.first.filters
      label_outputs = @ra.labels.values.first.outputs

      @ra.start
      assert_equal [true], @ra.inputs.map{|i| i.started? }
      assert_equal [true], label_filters.map{|i| i.started? }
      assert_equal [true, true, true], label_outputs.map{|i| i.started? }

      @ra.shutdown
      assert_equal [true], @ra.inputs.map{|i| i.stopped? }
      assert_equal [true], label_filters.map{|i| i.stopped? }
      assert_equal [true, true, true], label_outputs.map{|i| i.stopped? }

      assert_equal [true], @ra.inputs.map{|i| i.before_shutdown? }
      assert_equal [true], label_filters.map{|i| i.before_shutdown? }
      assert_equal [true, true, true], label_outputs.map{|i| i.before_shutdown? }

      assert_equal [true], @ra.inputs.map{|i| i.shutdown? }
      assert_equal [true], label_filters.map{|i| i.shutdown? }
      assert_equal [true, true, true], label_outputs.map{|i| i.shutdown? }

      assert_equal [true], @ra.inputs.map{|i| i.after_shutdown? }
      assert_equal [true], label_filters.map{|i| i.after_shutdown? }
      assert_equal [true, true, true], label_outputs.map{|i| i.after_shutdown? }

      assert_equal [true], @ra.inputs.map{|i| i.closed? }
      assert_equal [true], label_filters.map{|i| i.closed? }
      assert_equal [true, true, true], label_outputs.map{|i| i.closed? }

      assert_equal [true], @ra.inputs.map{|i| i.terminated? }
      assert_equal [true], label_filters.map{|i| i.terminated? }
      assert_equal [true, true, true], label_outputs.map{|i| i.terminated? }
    end
  end
end
