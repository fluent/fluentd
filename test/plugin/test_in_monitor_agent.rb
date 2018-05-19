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
    assert_true d.instance.include_config
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

    data(:with_config_yes => true,
         :with_config_no => false)
    test "get_monitor_info" do |with_config|
      d = create_driver
      test_label = @ra.labels['@test']
      error_label = @ra.labels['@ERROR']
      input_info = {
        "output_plugin"  => false,
        "plugin_category"=> "input",
        "plugin_id"      => "test_in",
        "retry_count"    => nil,
        "type"           => "test_in"
      }
      input_info.merge!("config" => {"@id" => "test_in", "@type" => "test_in"}) if with_config
      filter_info = {
        "output_plugin"   => false,
        "plugin_category" => "filter",
        "plugin_id"       => "test_filter",
        "retry_count"     => nil,
        "type"            => "test_filter"
      }
      filter_info.merge!("config" => {"@id" => "test_filter", "@type" => "test_filter"}) if with_config
      output_info = {
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "test_out",
        "retry_count"     => 0,
        "type"            => "test_out"
      }
      output_info.merge!("config" => {"@id" => "test_out", "@type" => "test_out"}) if with_config
      error_label_info = {
        "buffer_queue_length" => 0,
        "buffer_total_queued_size" => 0,
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "null",
        "retry_count"     => 0,
        "type"            => "null"
      }
      error_label_info.merge!("config" => {"@id"=>"null", "@type" => "null"}) if with_config
      opts = {with_config: with_config}
      assert_equal(input_info, d.instance.get_monitor_info(@ra.inputs.first, opts))
      assert_equal(filter_info, d.instance.get_monitor_info(@ra.filters.first, opts))
      assert_equal(output_info, d.instance.get_monitor_info(test_label.outputs.first, opts))
      assert_equal(error_label_info, d.instance.get_monitor_info(error_label.outputs.first, opts))
    end

    test "fluentd opts" do
      d = create_driver
      opts = Fluent::Supervisor.default_options
      Fluent::Supervisor.new(opts)
      expected_opts = {
        "config_path" => "/etc/fluent/fluent.conf",
        "pid_file"    => nil,
        "plugin_dirs" => ["/etc/fluent/plugin"],
        "log_path"    => nil,
        "root_dir"    => nil,
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
    req = Net::HTTP::Get.new(url, header)
    unless header.has_key?('Content-Type')
      header['Content-Type'] = 'application/octet-stream'
    end
    Net::HTTP.start(url.host, url.port) {|http|
      http.request(req)
    }
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
      # store Supervisor instance to avoid collected by GC
      @supervisor = Fluent::Supervisor.new(Fluent::Supervisor.default_options)
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

      response = get("http://127.0.0.1:#{@port}/api/plugins").body
      test_in = response.split("\n")[0]
      test_filter = response.split("\n")[3]
      assert_equal(expected_test_in_response, test_in)
      assert_equal(expected_test_filter_response, test_filter)
    end

    data(:include_config_and_retry_yes => [true, true, "include_config yes", "include_retry yes"],
         :include_config_and_retry_no => [false, false, "include_config no", "include_retry no"],)
    test "/api/plugins.json" do |(with_config, with_retry, include_conf, retry_conf)|
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
  #{include_conf}
  #{retry_conf}
")
      d.instance.start
      expected_test_in_response = {
        "output_plugin"   => false,
        "plugin_category" => "input",
        "plugin_id"       => "test_in",
        "retry_count"     => nil,
        "type"            => "test_in"
      }
      expected_test_in_response.merge!("config" => {"@id" => "test_in", "@type" => "test_in"}) if with_config
      expected_null_response = {
        "buffer_queue_length" => 0,
        "buffer_total_queued_size" => 0,
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "null",
        "retry_count"     => 0,
        "type"            => "null"
      }
      expected_null_response.merge!("config" => {"@id" => "null", "@type" => "null"}) if with_config
      expected_null_response.merge!("retry" => {}) if with_retry
      response = JSON.parse(get("http://127.0.0.1:#{@port}/api/plugins.json").body)
      test_in_response = response["plugins"][0]
      null_response = response["plugins"][5]
      assert_equal(expected_test_in_response, test_in_response)
      assert_equal(expected_null_response, null_response)
    end

    data(:with_config_and_retry_yes => [true, true, "?with_config=yes&with_retry"],
         :with_config_and_retry_no => [false, false, "?with_config=no&with_retry=no"])
    test "/api/plugins.json with query parameter. query parameter is preferred than include_config" do |(with_config, with_retry, query_param)|

      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      expected_test_in_response = {
        "output_plugin"   => false,
        "plugin_category" => "input",
        "plugin_id"       => "test_in",
        "retry_count"     => nil,
        "type"            => "test_in"
      }
      expected_test_in_response.merge!("config" => {"@id" => "test_in", "@type" => "test_in"}) if with_config
      expected_null_response = {
        "buffer_queue_length" => 0,
        "buffer_total_queued_size" => 0,
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "null",
        "retry_count"     => 0,
        "type"            => "null"
      }
      expected_null_response.merge!("config" => {"@id" => "null", "@type" => "null"}) if with_config
      expected_null_response.merge!("retry" => {}) if with_retry
      response = JSON.parse(get("http://127.0.0.1:#{@port}/api/plugins.json#{query_param}").body)
      test_in_response = response["plugins"][0]
      null_response = response["plugins"][5]
      assert_equal(expected_test_in_response, test_in_response)
      assert_equal(expected_null_response, null_response)
    end

    test "/api/plugins.json with 'with_ivars'. response contains specified instance variables of each plugin" do
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      expected_test_in_response = {
        "output_plugin"   => false,
        "plugin_category" => "input",
        "plugin_id"       => "test_in",
        "retry_count"     => nil,
        "type"            => "test_in",
        "instance_variables" => {"id" => "test_in"}
      }
      expected_null_response = {
        "buffer_queue_length" => 0,
        "buffer_total_queued_size" => 0,
        "output_plugin"   => true,
        "plugin_category" => "output",
        "plugin_id"       => "null",
        "retry_count"     => 0,
        "type"            => "null",
        "instance_variables" => {"id" => "null", "num_errors" => 0}
      }
      response = JSON.parse(get("http://127.0.0.1:#{@port}/api/plugins.json?with_config=no&with_retry=no&with_ivars=id,num_errors").body)
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
                   get("http://127.0.0.1:#{@port}/api/config").body)
    end

    test "/api/config.json" do
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  tag monitor
")
      d.instance.start
      res = JSON.parse(get("http://127.0.0.1:#{@port}/api/config.json").body)
      assert_equal("/etc/fluent/fluent.conf", res["config_path"])
      assert_nil(res["pid_file"])
      assert_equal(["/etc/fluent/plugin"], res["plugin_dirs"])
      assert_nil(res["log_path"])
    end
  end

  sub_test_case "check retry of buffered plugins" do
    class FluentTestFailWriteOutput < ::Fluent::Plugin::Output
      ::Fluent::Plugin.register_output('test_out_fail_write', self)

      def write(chunk)
        raise "chunk error!"
      end
    end

    setup do
      @port = unused_port
      # check @type and type in one configuration
      conf = <<-EOC
<source>
  @type monitor_agent
  @id monitor_agent
  bind "127.0.0.1"
  port #{@port}
</source>
<match **>
  @type test_out_fail_write
  @id test_out_fail_write
  <buffer>
    flush_mode immediate
  </buffer>
</match>
      EOC
      @ra = Fluent::RootAgent.new(log: $log)
      stub(Fluent::Engine).root_agent { @ra }
      @ra = configure_ra(@ra, conf)
    end

    test "/api/plugins.json retry object should be filled if flush was failed" do

      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{@port}
  include_config no
")
      d.instance.start
      expected_test_out_fail_write_response = {
          "buffer_queue_length" => 1,
          "buffer_total_queued_size" => 40,
          "output_plugin" => true,
          "plugin_category" => "output",
          "plugin_id" => "test_out_fail_write",
          "type" => "test_out_fail_write",
      }
      output = @ra.outputs[0]
      output.start
      output.after_start
      output.emit_events('test.tag', Fluent::ArrayEventStream.new([[event_time, {"message" => "test failed flush 1"}]]))
      # flush few times to check steps
      2.times do
        output.force_flush
        # output.force_flush calls #submit_flush_all, but #submit_flush_all skips to call #submit_flush_once when @retry exists.
        # So that forced flush in retry state should be done by calling #submit_flush_once directly.
        output.submit_flush_once
        sleep 0.1 until output.buffer.queued?
      end
      response = JSON.parse(get("http://127.0.0.1:#{@port}/api/plugins.json").body)
      test_out_fail_write_response = response["plugins"][1]
      # remove dynamic keys
      response_retry_count = test_out_fail_write_response.delete("retry_count")
      response_retry = test_out_fail_write_response.delete("retry")
      assert_equal(expected_test_out_fail_write_response, test_out_fail_write_response)
      assert{ response_retry.has_key?("steps") }
      # it's very hard to check exact retry count (because retries are called by output flush thread scheduling)
      assert{ response_retry_count >= 1 && response_retry["steps"] >= 0 }
      assert{ response_retry_count == response_retry["steps"] + 1 }
    end
  end

  sub_test_case "check the port number of http server" do
    test "on single worker environment" do
      port = unused_port
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{port}
")
      d.instance.start
      assert_equal("200", get("http://127.0.0.1:#{port}/api/plugins").code)
    end

    test "worker_id = 2 on multi worker environment" do
      port = unused_port
      Fluent::SystemConfig.overwrite_system_config('workers' => 4) do
        d = Fluent::Test::Driver::Input.new(Fluent::Plugin::MonitorAgentInput)
        d.instance.instance_eval{ @_fluentd_worker_id = 2 }
        d.configure("
  @type monitor_agent
  bind '127.0.0.1'
  port #{port - 2}
")
        d.instance.start
      end
      assert_equal("200", get("http://127.0.0.1:#{port}/api/plugins").code)
    end
  end

  sub_test_case "check NoMethodError does not happen" do
    class FluentTestBufferVariableOutput < ::Fluent::Plugin::Output
      ::Fluent::Plugin.register_output('test_out_buffer_variable', self)
      def configure(conf)
        super
        @buffer = []
      end

      def write(chunk)
      end
    end
    class FluentTestBufferVariableFilter < ::Fluent::Plugin::Filter
      ::Fluent::Plugin.register_filter("test_filter_buffer_variable", self)
      def initialize
        super
        @buffer = {}
      end
      def filter(tag, time, record)
        record
      end
    end

    setup do
      conf = <<-EOC
<match **>
  @type test_out_buffer_variable
  @id test_out_buffer_variable
</match>
<filter **>
  @type test_filter_buffer_variable
  @id test_filter_buffer_variable
</filter>
EOC
      @ra = Fluent::RootAgent.new(log: $log)
      stub(Fluent::Engine).root_agent { @ra }
      @ra = configure_ra(@ra, conf)
    end

    test "plugins have a variable named buffer does not throws NoMethodError" do
      port = unused_port
      d = create_driver("
  @type monitor_agent
  bind '127.0.0.1'
  port #{port}
  include_config no
")
      d.instance.start

      assert_equal("200", get("http://127.0.0.1:#{port}/api/plugins.json").code)
      assert{ d.logs.none?{|log| log.include?("NoMethodError") } }
      assert_equal(false, d.instance.instance_variable_get(:@first_warn))
    end
  end
end
