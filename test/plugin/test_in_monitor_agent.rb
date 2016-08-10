require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_monitor_agent'
require 'fluent/engine'
require 'fluent/config'
require 'fluent/event_router'
require 'fluent/supervisor'
require_relative '../test_plugin_classes'

class MonitorAgentInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = '')
    Fluent::Test::Driver::Input.new(Fluent::Plugin::MonitorAgentInput).configure(conf)
  end

  def configure_ra(ra, conf_str)
    conf = Fluent::Config.parse(conf_str, "(test)", "(test_dir)", true)
    ra.configure(conf)
    ra
  end

  def test_configure
    d = create_driver
    assert_equal("0.0.0.0", d.instance.bind)
    assert_equal(24220, d.instance.port)
    assert_equal(nil, d.instance.tag)
    assert_equal(60, d.instance.emit_interval)
  end

  sub_test_case "collect plugin information" do
    setup do
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
      @ra = Fluent::RootAgent.new(log: $log)
      stub(Fluent::Engine).root_agent { @ra }
      @ra = configure_ra(@ra, conf)
    end

    test "plugin_category" do
      d = create_driver
      test_label = @ra.labels['@test']
      error_label = @ra.labels['@ERROR']
      assert_equal("input", d.instance.plugin_category(@ra.inputs.first))
      assert_equal("filter", d.instance.plugin_category(@ra.filters.first))
      assert_equal("output", d.instance.plugin_category(test_label.outputs.first))
      assert_equal("output", d.instance.plugin_category(error_label.outputs.first))
    end

    test "get_monitor_info" do
      d = create_driver
      test_label = @ra.labels['@test']
      error_label = @ra.labels['@ERROR']
      input_info = {
        "config" => {
          "@id"   => "test_in",
          "@type" => "test_in"
        },
        "output_plugin"  => false,
        "plugin_category"=> "input",
        "plugin_id"      => "test_in",
        "retry_count"    => nil,
        "type"           => "test_in"
      }
      filter_info = {
        "config" => {
          "id"   => "test_filter",
          "type" => "test_filter"
        },
        "output_plugin"   => false,
        "plugin_category" => "filter",
        "plugin_id"       => "test_filter",
        "retry_count"     => nil,
        "type"            => "test_filter"
      }
      output_info = {
        "config" => {
          "id"   => "test_out",
          "type" => "test_out"
        },
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "test_out",
        "retry_count"     => 0,
        "type"            => "test_out"
      }
      assert_equal(input_info, d.instance.get_monitor_info(@ra.inputs.first))
      assert_equal(filter_info, d.instance.get_monitor_info(@ra.filters.first))
      assert_equal(output_info, d.instance.get_monitor_info(test_label.outputs.first))
    end

    test "fluentd opts" do
      d = create_driver
      opts = Fluent::Supervisor.default_options
      Fluent::Supervisor.new(opts)
      expected_opts = {
        "config_path" => "/etc/fluent/fluent.conf",
        "pid_file"    => nil,
        "plugin_dirs" => ["/etc/fluent/plugin"],
        "log_path"    => nil
      }
      assert_equal(expected_opts, d.instance.fluentd_opts)
    end

    test "all_plugins" do
      d = create_driver
      plugins = []
      d.instance.all_plugins.each {|plugin| plugins << plugin.class }
      assert_equal([FluentTest::FluentTestInput,
                    Fluent::Plugin::RelabelOutput,
                    FluentTest::FluentTestFilter,
                    FluentTest::FluentTestOutput,
                    Fluent::Plugin::NullOutput], plugins)
    end
  end
end
