require 'fluent/event_router'
require_relative 'test_plugin_classes'

class RootAgentTest < ::Test::Unit::TestCase
  include Fluent
  include FluentTest

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/root_agent")

  def setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  def test_initialize
    ra = RootAgent.new
    assert_equal 0, ra.instance_variable_get(:@suppress_emit_error_log_interval)
    assert_nil ra.instance_variable_get(:@next_emit_error_log_time)
  end

  data(
    'suppress interval' => [{:suppress_interval => 30}, {:@suppress_emit_error_log_interval => 30}],
    'without source' => [{:without_source => ""}, {:@without_source => ""}],
    'without source file' => [{:without_source => "#{TMP_DIR}/without"}, {:@without_source => "#{TMP_DIR}/without"}],
    'without source interval' => [{:without_source_interval=> 1}, {:@without_source_interval => 1}]
    )
  def test_initialize_with_opt(data)
    opt, expected = data
    ra = RootAgent.new(opt)
    expected.each { |k, v|
      assert_equal v, ra.instance_variable_get(k)
    }
  end

  sub_test_case 'configure' do
    setup do
      @ra = RootAgent.new
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
      [:@started_inputs, :@started_outputs, :@started_filters].each { |k|
        assert_empty ra.instance_variable_get(k)
      }
      assert_nil ra.context
      assert_nil ra.error_collector
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
      assert_kind_of RelabelOutput, ra.outputs.first
      assert_kind_of FluentTestFilter, ra.filters.first
      [:@started_inputs, :@started_outputs, :@started_filters].each { |k|
        assert_empty ra.instance_variable_get(k)
      }
      assert ra.error_collector

      %W(@test @ERROR).each { |label_symbol|
        assert_include ra.labels, label_symbol
        assert_kind_of Label, ra.labels[label_symbol]
      }

      test_label = ra.labels['@test']
      assert_kind_of FluentTestOutput, test_label.outputs.first
      assert_equal ra, test_label.root_agent

      error_label = ra.labels['@ERROR']
      assert_kind_of NullOutput, error_label.outputs.first
      assert_kind_of RootAgent::RootAgentProxyWithoutErrorCollector, error_label.root_agent
    end
  end

  sub_test_case 'start/shutdown' do
    setup do
      @ra = RootAgent.new
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

  sub_test_case 'without_source' do
    setup do
      File.unlink(stop_file) rescue nil
      @ra = RootAgent.new(:without_source => "")
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
        <source>
          @type test_in
          @id test_in
        </source>
      EOC
    end

    test 'invoke a fluentd without input plugins' do
      @ra.start
      assert_empty @ra.inputs
      @ra.shutdown
    end
  end

  sub_test_case 'without_source file' do
    def stop_file
      "#{TMP_DIR}/stop"
    end

    setup do
      File.unlink(stop_file) rescue nil
      @ra = RootAgent.new(:without_source => stop_file)
      @ra.configure(Config.parse(<<-EOC, "(test)", "(test_dir)", true))
        <source>
          @type test_in
          @id test_in
        </source>
      EOC
    end

    test 'stop file does not exist on bootup' do
      @ra.start
      assert_true @ra.inputs.first.started
      @ra.shutdown
    end

    test 'stop file exists on bootup' do
      File.open(stop_file, "w").close
      @ra.start
      assert_not_equal true, @ra.inputs.first.started
      @ra.shutdown
    end

    test 'stop file appear => disappear => appear' do
      @ra.start
      File.open(stop_file, "w").close
      @ra.on_without_source_timer
      assert_not_equal true, @ra.inputs.first.started

      File.unlink(stop_file)
      @ra.on_without_source_timer
      assert_true @ra.inputs.first.started

      File.open(stop_file, "w").close
      @ra.on_without_source_timer
      assert_not_equal true, @ra.inputs.first.started
    end
  end
end
