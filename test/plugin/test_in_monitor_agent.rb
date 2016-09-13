require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_monitor_agent'
require 'fluent/engine'
require 'fluent/config'
require 'fluent/event_router'
require 'fluent/supervisor'
require 'net/http'
require 'json'
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
  @type test_filter
  @id test_filter
</filter>
<match **>
  @type relabel
  @id test_relabel
  @label @test
</match>
<label @test>
  <match **>
    @type test_out
    @id test_out
  </match>
</label>
<label @copy>
  <match **>
    @type copy
    <store>
      @type test_out
      @id copy_out_1
    </store>
    <store>
      @type test_out
      @id copy_out_2
    </store>
  </match>
</label>
<label @ERROR>
  <match>
    @type null
    @id null
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
          "@id"   => "test_filter",
          "@type" => "test_filter"
        },
        "output_plugin"   => false,
        "plugin_category" => "filter",
        "plugin_id"       => "test_filter",
        "retry_count"     => nil,
        "type"            => "test_filter"
      }
      output_info = {
        "config" => {
          "@id"   => "test_out",
          "@type" => "test_out"
        },
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "test_out",
        "retry_count"     => 0,
        "type"            => "test_out"
      }
      error_label_info = {
        "config" => {
          "@id"=>"null",
          "@type" => "null"
        },
        "buffer_queue_length" => 0,
        "buffer_total_queued_size" => 0,
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "null",
        "retry_count"     => 0,
        "type"            => "null"
      }
      assert_equal(input_info, d.instance.get_monitor_info(@ra.inputs.first))
      assert_equal(filter_info, d.instance.get_monitor_info(@ra.filters.first))
      assert_equal(output_info, d.instance.get_monitor_info(test_label.outputs.first))
      assert_equal(error_label_info, d.instance.get_monitor_info(error_label.outputs.first))
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
                    FluentTest::FluentTestOutput, # in label @test
                    Fluent::Plugin::CopyOutput,
                    FluentTest::FluentTestOutput, # in label @copy 1
                    FluentTest::FluentTestOutput, # in label @copy 2
                    Fluent::Plugin::NullOutput], plugins)
    end

    test "emit" do
      port = unused_port
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{port}
  tag monitor
  emit_interval 1
")
      d.instance.start
      d.end_if do
        d.events.size >= 5
      end
      d.run
      expect_relabel_record = {
        "plugin_id"       => "test_relabel",
        "plugin_category" => "output",
        "type"            => "relabel",
        "output_plugin"   => true,
        "retry_count"     => 0}
      expect_test_out_record = {
        "plugin_id"       => "test_out",
        "plugin_category" => "output",
        "type"            => "test_out",
        "output_plugin"   => true,
        "retry_count"     => 0
      }
      assert_equal(expect_relabel_record, d.events[1][2])
      assert_equal(expect_test_out_record, d.events[3][2])
    end
  end

  def get(uri, header = {})
    url = URI.parse(uri)
    req = Net::HTTP::Get.new(url.path, header)
    unless header.has_key?('Content-Type')
      header['Content-Type'] = 'application/octet-stream'
    end
    res = Net::HTTP.start(url.host, url.port) {|http|
      http.request(req)
    }
    res.body
  end

  sub_test_case "servlets" do
    setup do
      @port = unused_port
      # check @type and type in one configuration
      conf = <<-EOC
<source>
  @type test_in
  @id test_in
</source>
<source>
  @type monitor_agent
  bind "127.0.0.1"
  port #{@port}
  tag monitor
  @id monitor_agent
</source>
<filter>
  @type test_filter
  @id test_filter
</filter>
<match **>
  @type relabel
  @id test_relabel
  @label @test
</match>
<label @test>
  <match **>
    @type test_out
    @id test_out
  </match>
</label>
<label @ERROR>
  <match>
    @type null
    @id null
  </match>
</label>
EOC
      @ra = Fluent::RootAgent.new(log: $log)
      stub(Fluent::Engine).root_agent { @ra }
      @ra = configure_ra(@ra, conf)
    end

    test "/api/plugins" do
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      expected_test_in_response = "\
plugin_id:test_in\tplugin_category:input\ttype:test_in\toutput_plugin:false\tretry_count:"
      expected_test_filter_response = "\
plugin_id:test_filter\tplugin_category:filter\ttype:test_filter\toutput_plugin:false\tretry_count:"

      response = get("http://127.0.0.1:#{@port}/api/plugins")
      test_in = response.split("\n")[0]
      test_filter = response.split("\n")[3]
      assert_equal(expected_test_in_response, test_in)
      assert_equal(expected_test_filter_response, test_filter)
    end

    test "/api/plugins.json" do
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      expected_test_in_response =
        {"config" => {
          "@id"   => "test_in",
          "@type" => "test_in"
        },
        "output_plugin"   => false,
        "plugin_category" => "input",
        "plugin_id"       => "test_in",
        "retry_count"     => nil,
        "type"            => "test_in"}
      expected_null_response =
        {"config" => {
          "@id"   => "null",
          "@type" => "null"
        },
        "buffer_queue_length" => 0,
        "buffer_total_queued_size" => 0,
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "null",
        "retry_count"     => 0,
        "type"            => "null"}
      response = JSON.parse(get("http://127.0.0.1:#{@port}/api/plugins.json"))
      test_in_response = response["plugins"][0]
      null_response = response["plugins"][5]
      assert_equal(expected_test_in_response, test_in_response)
      assert_equal(expected_null_response, null_response)
    end

    test "/api/config" do
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      expected_response_regex = /pid:\d+\tppid:\d+\tconfig_path:\/etc\/fluent\/fluent.conf\tpid_file:\tplugin_dirs:\[\"\/etc\/fluent\/plugin\"\]\tlog_path:/

      assert_match(expected_response_regex,
                   get("http://127.0.0.1:#{@port}/api/config"))
    end

    test "/api/config.json" do
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      res = JSON.parse(get("http://127.0.0.1:#{@port}/api/config.json"))
      assert_equal("/etc/fluent/fluent.conf", res["config_path"])
      assert_nil(res["pid_file"])
      assert_equal(["/etc/fluent/plugin"], res["plugin_dirs"])
      assert_nil(res["log_path"])
    end
  end
end
