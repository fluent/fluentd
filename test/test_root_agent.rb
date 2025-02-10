require_relative 'helper'
require 'fluent/event_router'
require 'fluent/system_config'
require 'timecop'
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
    'without source' => [{'without_source' => true}, {:@without_source => true}],
    'enable input metrics' => [{'enable_input_metrics' => true}, {:@enable_input_metrics => true}],
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

    test 'raises configuration error for label without name' do
      conf = <<-EOC
<label>
  @type test_out
</label>
EOC
      errmsg = "Missing symbol argument on <label> directive"
      assert_raise Fluent::ConfigError.new(errmsg) do
        configure_ra(conf)
      end
    end

    test 'raises configuration error for <label @ROOT>' do
      conf = <<-EOC
<source>
  @type test_in
  @label @ROOT
</source>
<label @ROOT>
  @type test_out
</label>
EOC
      errmsg = "@ROOT for <label> is not permitted, reserved for getting root router"
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
    end
  end

  sub_test_case 'start/shutdown' do
    def setup_root_agent(conf)
      ra = RootAgent.new(log: $log)
      stub(Engine).root_agent { ra }
      ra.configure(Config.parse(conf, "(test)", "(test_dir)", true))
      ra
    end

    test 'plugin status' do
      ra = setup_root_agent(<<-EOC)
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
      ra.start
      assert_true ra.inputs.first.started
      assert_true ra.filters.first.started
      assert_true ra.outputs.first.started

      ra.shutdown
      assert_false ra.inputs.first.started
      assert_false ra.filters.first.started
      assert_false ra.outputs.first.started
    end

    test 'output plugin threads should run before input plugin is blocked with buffer full' do
      ra = setup_root_agent(<<-EOC)
<source>
  @type test_in_gen
  @id test_in_gen
</source>
<match **>
  @type test_out_buffered
  @id test_out_buffered
  <buffer>
    chunk_limit_size 1k
    queue_limit_length 2
    flush_thread_count 2
    overflow_action block
  </buffer>
</match>
EOC
      waiting(5) { ra.start }
      assert_true ra.inputs.first.started
      assert_true ra.outputs.first.started

      ra.shutdown
      assert_false ra.inputs.first.started
      assert_false ra.outputs.first.started
    end
  end

  sub_test_case 'configured with label and secondary plugin' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent{ @ra }
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
<source>
  @type test_in
  @label @route_a
</source>
<label @route_a>
  <match a.**>
    @type test_out_buffered
    <secondary>
      @type test_out_emit
    </secondary>
  </match>
</label>
<label @route_b>
  <match b.**>
    @type test_out
  </match>
</label>
EOC
    end

    test 'secondary plugin has an event router for the label which the plugin is in' do
      assert_equal 1, @ra.inputs.size
      assert_equal 2, @ra.labels.size
      assert_equal ['@route_a', '@route_b'], @ra.labels.keys
      assert_equal '@route_a', @ra.labels['@route_a'].context
      assert_equal '@route_b', @ra.labels['@route_b'].context

      c1 = @ra.labels['@route_a']

      assert_equal 1, c1.outputs.size
      assert !c1.outputs.first.has_router?

      assert c1.outputs.first.secondary
      assert c1.outputs.first.secondary.has_router?
      assert_equal c1.event_router, c1.outputs.first.secondary.router
    end
  end

  sub_test_case 'configured with label and secondary plugin with @label specifier' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent{ @ra }
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
<source>
  @type test_in
  @label @route_a
</source>
<label @route_a>
  <match a.**>
    @type test_out_buffered
    <secondary>
      @type test_out_emit
      @label @route_b
    </secondary>
  </match>
</label>
<label @route_b>
  <match b.**>
    @type test_out
  </match>
</label>
EOC
    end

    test 'secondary plugin has an event router for the label specified in secondary section' do
      assert_equal 1, @ra.inputs.size
      assert_equal 2, @ra.labels.size
      assert_equal ['@route_a', '@route_b'], @ra.labels.keys
      assert_equal '@route_a', @ra.labels['@route_a'].context
      assert_equal '@route_b', @ra.labels['@route_b'].context

      c1 = @ra.labels['@route_a']
      c2 = @ra.labels['@route_b']

      assert_equal 1, c1.outputs.size
      assert !c1.outputs.first.has_router?

      assert c1.outputs.first.secondary
      assert c1.outputs.first.secondary.has_router?
      assert_equal c2.event_router, c1.outputs.first.secondary.router
    end
  end

  sub_test_case 'configured with label and secondary plugin with @label specifier in primary output' do
    setup do
      @ra = RootAgent.new(log: $log)
      stub(Engine).root_agent{ @ra }
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
<source>
  @type test_in
  @label @route_a
</source>
<label @route_a>
  <match a.**>
    @type test_out_emit
    @label @route_b
    <secondary>
      @type test_out_emit
    </secondary>
  </match>
</label>
<label @route_b>
  <match b.**>
    @type test_out
  </match>
</label>
EOC
    end

    test 'secondary plugin has an event router for the label specified in secondary section' do
      assert_equal 1, @ra.inputs.size
      assert_equal 2, @ra.labels.size
      assert_equal ['@route_a', '@route_b'], @ra.labels.keys
      assert_equal '@route_a', @ra.labels['@route_a'].context
      assert_equal '@route_b', @ra.labels['@route_b'].context

      c1 = @ra.labels['@route_a']
      c2 = @ra.labels['@route_b']

      assert_equal 1, c1.outputs.size
      assert c1.outputs.first.secondary

      p1 = c1.outputs.first
      assert p1.has_router?
      assert_equal c1.event_router, p1.context_router
      assert_equal c2.event_router, p1.router

      s1 = p1.secondary
      assert s1.has_router?
      assert_equal c1.event_router, s1.context_router
      assert_equal c2.event_router, s1.router
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

    test 'plugin #shutdown is not called twice' do
      assert_equal 1, @ra.inputs.size
      assert_equal 0, @ra.filters.size
      assert_equal 0, @ra.outputs.size
      assert_equal 1, @ra.labels.size
      assert_equal '@testing', @ra.labels.keys.first
      assert_equal 1, @ra.labels.values.first.filters.size
      assert_equal 3, @ra.labels.values.first.outputs.size

      @ra.start

      old_level = @ra.log.level
      begin
        @ra.log.instance_variable_get(:@logger).level = Fluent::Log::LEVEL_INFO - 1
        assert_equal Fluent::Log::LEVEL_INFO, @ra.log.level

        @ra.log.out.flush_logs = false

        @ra.shutdown

        test_out1_shutdown_logs = @ra.log.out.logs.select{|line| line =~ /shutting down output plugin type=:test_out plugin_id="test_out1"/ }
        assert_equal 1, test_out1_shutdown_logs.size
      ensure
        @ra.log.out.flush_logs = true
        @ra.log.out.reset
        @ra.log.level = old_level
      end
    end
  end

  sub_test_case 'configured with MultiOutput plugin which creates plugin instances dynamically' do
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
  <match **>
    @type test_dynamic_out
    @id test_dyn
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
      assert_equal 0, @ra.labels.values.first.filters.size
      assert_equal 1, @ra.labels.values.first.outputs.size

      dyn_out = @ra.labels.values.first.outputs.first
      assert_nil dyn_out.child

      @ra.start

      assert_equal 1, @ra.labels.values.first.outputs.size

      assert dyn_out.child
      assert_false dyn_out.child.outputs_statically_created
      assert_equal 2, dyn_out.child.outputs.size

      assert_equal true, dyn_out.child.outputs[0].started?
      assert_equal true, dyn_out.child.outputs[1].started?
      assert_equal true, dyn_out.child.outputs[0].after_started?
      assert_equal true, dyn_out.child.outputs[1].after_started?

      @ra.shutdown

      assert_equal 1, @ra.labels.values.first.outputs.size

      assert_false dyn_out.child.outputs_statically_created
      assert_equal 2, dyn_out.child.outputs.size

      assert_equal [true, true], dyn_out.child.outputs.map{|i| i.stopped? }
      assert_equal [true, true], dyn_out.child.outputs.map{|i| i.before_shutdown? }
      assert_equal [true, true], dyn_out.child.outputs.map{|i| i.shutdown? }
      assert_equal [true, true], dyn_out.child.outputs.map{|i| i.after_shutdown? }
      assert_equal [true, true], dyn_out.child.outputs.map{|i| i.closed? }
      assert_equal [true, true], dyn_out.child.outputs.map{|i| i.terminated? }
    end
  end

  sub_test_case 'configure emit_error_interval' do
    setup do
      system_config = SystemConfig.new
      system_config.emit_error_log_interval = 30
      @ra = RootAgent.new(log: $log, system_config: system_config)
      stub(Engine).root_agent { @ra }
      @ra.log.out.reset
      one_minute_ago = Time.now.to_i - 60
      Timecop.freeze(one_minute_ago)
    end

    teardown do
      Timecop.return
    end

    test 'suppresses errors' do
      mock(@ra.log).warn_backtrace()
      e = StandardError.new('standard error')
      begin
        @ra.handle_emits_error("tag", nil, e)
      rescue
      end

      begin
        @ra.handle_emits_error("tag", nil, e)
      rescue
      end

      assert_equal 1, @ra.log.out.logs.size
    end
  end

  sub_test_case 'configured at worker2 with 4 workers environment' do
    setup do
      ENV['SERVERENGINE_WORKER_ID'] = '2'
      @ra = RootAgent.new(log: $log)
      system_config = SystemConfig.new
      system_config.workers = 4
      stub(Engine).worker_id { 2 }
      stub(Engine).root_agent { @ra }
      stub(Engine).system_config { system_config }
      @ra
    end

    teardown '' do
      ENV.delete('SERVERENGINE_WORKER_ID')
    end

    def configure_ra(conf_str)
      conf = Config.parse(conf_str, "(test)", "(test_dir)", true)
      @ra.configure(conf)
      @ra
    end

    test 'raises configuration error for missing worker id' do
      errmsg = 'Missing worker id on <worker> directive'
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error for too big worker id' do
      errmsg = "worker id 4 specified by <worker> directive is not allowed. Available worker id is between 0 and 3"
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker 4>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error for too big worker id on multi workers syntax' do
      errmsg = "worker id 4 specified by <worker> directive is not allowed. Available worker id is between 0 and 3"
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker 1-4>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error for worker id collisions on multi workers syntax' do
      errmsg = "specified worker_id<2> collisions is detected on <worker> directive. Available worker id(s): [3]"
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker 0-2>
</worker>
<worker 2-4>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error for worker id collisions on multi workers syntax when multi available worker_ids are left' do
      errmsg = "specified worker_id<1> collisions is detected on <worker> directive. Available worker id(s): [2, 3]"
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker 0-1>
</worker>
<worker 1-3>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error for too big worker id on invalid reversed multi workers syntax' do
      errmsg = "greater first_worker_id<3> than last_worker_id<0> specified by <worker> directive is not allowed. Available multi worker assign syntax is <smaller_worker_id>-<greater_worker_id>"
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker 3-0>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error for invalid elements as a child of worker section' do
      errmsg = '<worker> section cannot have <system> directive'
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<worker 2>
<system>
</system>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'raises configuration error when configured plugins do not have support multi worker configuration' do
      errmsg = "Plugin 'test_out' does not support multi workers configuration (FluentTest::FluentTestOutput)"
      assert_raise Fluent::ConfigError.new(errmsg) do
        conf = <<-EOC
<match **>
@type test_out
</match>
EOC
        configure_ra(conf)
      end
    end

    test 'does not raise configuration error when configured plugins in worker section do not have support multi worker configuration' do
      assert_nothing_raised do
        conf = <<-EOC
<worker 2>
<match **>
  @type test_out
</match>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'does not raise configuration error when configured plugins as a children of MultiOutput in worker section do not have support multi worker configuration' do
      assert_nothing_raised do
        conf = <<-EOC
<worker 2>
<match **>
  @type copy
  <store>
    @type test_out
  </store>
  <store>
    @type test_out
  </store>
</match>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'does not raise configuration error when configured plugins owned by plugin do not have support multi worker configuration' do
      assert_nothing_raised do
        conf = <<-EOC
<worker 2>
<match **>
  @type test_out_buffered
  <buffer>
    @type test_buffer
  </buffer>
</match>
</worker>
EOC
        configure_ra(conf)
      end
    end

    test 'with plugins' do
      conf = <<-EOC
<worker 2>
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
</worker>
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
    end

    test 'with plugins but for another worker' do
      conf = <<-EOC
<worker 0>
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
</worker>
EOC
      ra = configure_ra(conf)
      assert_equal 0, ra.inputs.size
      assert_equal 0, ra.outputs.size
      assert_equal 0, ra.filters.size
      assert_equal 0, ra.labels.size
      refute ra.error_collector
    end

    test 'with plugins for workers syntax should match worker_id equals to 2' do
      conf = <<-EOC
<worker 0-2>
  <source>
    @type forward
  </source>
  <filter **>
    @type test_filter
    @id test_filter
  </filter>
  <match pattern>
    @type stdout
  </match>
  <label @ERROR>
    <match>
      @type null
    </match>
  </label>
</worker>
EOC

      ra = configure_ra(conf)
      assert_kind_of Fluent::Plugin::ForwardInput, ra.inputs.first
      assert_kind_of Fluent::Plugin::StdoutOutput, ra.outputs.first
      assert_kind_of FluentTestFilter, ra.filters.first
      assert ra.error_collector
    end
  end

  sub_test_case 'start with-source-only' do
    def conf
      <<~EOC
        <source>
          @type test_in_gen
          @id test_in_gen
          num 20
          interval_sec 0.1
          async
        </source>

        <filter test.**>
          @type record_transformer
          @id record_transformer
          <record>
            foo foo
          </record>
        </filter>

        <match test.**>
          @type test_out
          @id test_out
        </match>
      EOC
    end

    def setup
      omit "Not supported on Windows" if Fluent.windows?
      system_config = SystemConfig.new(
        Config::Element.new('system', '', {
          'with_source_only' => true,
        }, [
          Config::Element.new('source_only_buffer', '', {
            'flush_interval' => 1,
          }, []),
        ])
      )
      @root_agent = RootAgent.new(log: $log, system_config: system_config)
      stub(Engine).root_agent { @root_agent }
      stub(Engine).system_config { system_config }
      @root_agent.configure(Config.parse(conf, "(test)", "(test_dir)"))
    end

    test 'only input plugins should start' do
      @root_agent.start

      assert_equal(
        {
          "input started?" => [true],
          "filter started?" => [false],
          "output started?" => [false],
        },
        {
          "input started?" => @root_agent.inputs.map { |plugin| plugin.started? },
          "filter started?" => @root_agent.filters.map { |plugin| plugin.started? },
          "output started?" => @root_agent.outputs.map { |plugin| plugin.started? },
        }
      )
    ensure
      @root_agent.shutdown
      # Buffer files remain because not cancelling source-only.
      # As a test, they should be clean-up-ed.
      buf_dir = @root_agent.instance_variable_get(:@source_only_buffer_agent).instance_variable_get(:@base_buffer_dir)
      FileUtils.remove_dir(buf_dir)
    end

    test '#cancel_source_only! should start all plugins' do
      @root_agent.start
      @root_agent.cancel_source_only!

      assert_equal(
        {
          "input started?" => [true],
          "filter started?" => [true],
          "output started?" => [true],
        },
        {
          "input started?" => @root_agent.inputs.map { |plugin| plugin.started? },
          "filter started?" => @root_agent.filters.map { |plugin| plugin.started? },
          "output started?" => @root_agent.outputs.map { |plugin| plugin.started? },
        }
      )
    ensure
      @root_agent.shutdown
    end

    test 'buffer should be loaded after #cancel_source_only!' do
      @root_agent.start
      sleep 1
      @root_agent.cancel_source_only!

      waiting(3) do
        # Wait buffer loaded after source-only cancelled
        sleep 1 until @root_agent.outputs[0].events["test.event"].any? { |record| record["num"] == 0 }
      end

      waiting(3) do
        # Wait the last data output
        sleep 1 until @root_agent.outputs[0].events["test.event"].any? { |record| record["num"] == 19 }
      end

      # all data should be outputted
      assert { @root_agent.outputs[0].events["test.event"].size == 20 }
    ensure
      @root_agent.shutdown
    end
  end

  sub_test_case 'start_in_parallel' do
    def conf
      <<~EOC
        <source>
          @type test_in_gen
          @id test_in_gen
          num 20
          interval_sec 0.1
          async
        </source>

        <source>
          @type test_in
          @id test_in
        </source>

        <filter test.**>
          @type record_transformer
          @id record_transformer
          <record>
            foo foo
          </record>
        </filter>

        <match test.**>
          @type test_out
          @id test_out
        </match>
      EOC
    end

    def setup
      omit "Not supported on Windows" if Fluent.windows?
      system_config = SystemConfig.new(
        Config::Element.new('system', '', {}, [
          Config::Element.new('source_only_buffer', '', {
            'flush_interval' => 1,
          }, []),
        ])
      )
      @root_agent = RootAgent.new(log: $log, system_config: system_config, start_in_parallel: true)
      stub(Engine).root_agent { @root_agent }
      stub(Engine).system_config { system_config }
      @root_agent.configure(Config.parse(conf, "(test)", "(test_dir)"))
    end

    test 'only input plugins should start' do
      @root_agent.start

      assert_equal(
        {
          "input started?" => [true, false],
          "filter started?" => [false],
          "output started?" => [false],
        },
        {
          "input started?" => @root_agent.inputs.map { |plugin| plugin.started? },
          "filter started?" => @root_agent.filters.map { |plugin| plugin.started? },
          "output started?" => @root_agent.outputs.map { |plugin| plugin.started? },
        }
      )
    ensure
      @root_agent.shutdown
      # Buffer files remain because not cancelling source-only.
      # As a test, they should be clean-up-ed.
      buf_dir = @root_agent.instance_variable_get(:@source_only_buffer_agent).instance_variable_get(:@base_buffer_dir)
      FileUtils.remove_dir(buf_dir)
    end

    test '#cancel_source_only! should start all plugins' do
      @root_agent.start
      @root_agent.cancel_source_only!

      assert_equal(
        {
          "input started?" => [true, true],
          "filter started?" => [true],
          "output started?" => [true],
        },
        {
          "input started?" => @root_agent.inputs.map { |plugin| plugin.started? },
          "filter started?" => @root_agent.filters.map { |plugin| plugin.started? },
          "output started?" => @root_agent.outputs.map { |plugin| plugin.started? },
        }
      )
    ensure
      @root_agent.shutdown
    end

    test 'buffer should be loaded after #cancel_source_only!' do
      @root_agent.start
      sleep 1
      @root_agent.cancel_source_only!

      waiting(3) do
        # Wait buffer loaded after source-only cancelled
        sleep 1 until @root_agent.outputs[0].events["test.event"].any? { |record| record["num"] == 0 }
      end

      waiting(3) do
        # Wait the last data output
        sleep 1 until @root_agent.outputs[0].events["test.event"].any? { |record| record["num"] == 19 }
      end

      # all data should be outputted
      assert { @root_agent.outputs[0].events["test.event"].size == 20 }
    ensure
      @root_agent.shutdown
    end
  end

  test 'configure with umask should set proper value' do
    conf = <<-EOC
  <system>
    umask 0027
  </system>
  EOC
  
    config = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
    ra = Fluent::RootAgent.new(log: $log)
    
    original_umask = File.umask

    filename = "test_umask_file"

    begin
      ra.configure(config)

      File.open(filename, "w") do |f|
        f.write("Test data")
      end

      file_mode = File.stat(filename).mode & 0777

      # 0666 & ~0027  =>  0640 (octal)
      expected_mode = 0640
  
      assert_equal(expected_mode, file_mode,
        "Expected file mode to be #{sprintf('%o', expected_mode)}, but got #{sprintf('%o', file_mode)}")
    ensure
      File.umask(original_umask)
      File.delete(filename) if File.exist?(filename)
    end
  end
  

  test 'configure with invalid umask should raise error' do
    conf = <<-EOC
  <system>
    umask 0999  # invalid octal
  </system>
  EOC
  
    config = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
    ra = Fluent::RootAgent.new(log: $log)
    original_umask = File.umask
    begin
      assert_raise(Fluent::ConfigError, "Expected configuration with invalid umask to raise Fluent::ConfigError") do
        ra.configure(config)
      end
    ensure
      File.umask(original_umask)
    end
  end

  test 'configure without umask should use default umask and affect file permissions' do
    conf = <<-EOC
  <system>
  </system>
  EOC
  
    config = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
    ra = Fluent::RootAgent.new(log: $log)
    original_umask = File.umask
    filename = "test_umask_default_file"
  
    begin
      ra.configure(config)
      assert_equal 0022, File.umask, "Expected umask to be 0022 after configuration"
      #   0666 & ~0022  => 0644 (rw-r--r--)
      expected_mode = 0644

      File.open(filename, "w") { |f| f.write("Test content") }
      file_mode = File.stat(filename).mode & 0777
  
      assert_equal expected_mode, file_mode,
        "Expected file mode to be #{sprintf('%o', expected_mode)}, but got #{sprintf('%o', file_mode)}"
    ensure
      File.umask(original_umask)
      File.delete(filename) if File.exist?(filename)
    end
  end
end
