require_relative 'helper'
require 'fluent/event_router'
require 'fluent/system_config'
require 'fluent/supervisor'
require 'fluent/file_wrapper'
require_relative 'test_plugin_classes'

require 'net/http'
require 'uri'
require 'fileutils'
require 'tempfile'
require 'securerandom'
require 'pathname'

if Fluent.windows?
  require 'win32/event'
end

class SupervisorTest < ::Test::Unit::TestCase
  class DummyServer
    include Fluent::ServerModule
    attr_accessor :rpc_endpoint, :enable_get_dump, :socket_manager_server
    def config
      {}
    end
  end

  def tmp_dir
    File.join(File.dirname(__FILE__), "tmp", "supervisor#{ENV['TEST_ENV_NUMBER']}", SecureRandom.hex(10))
  end

  def setup
    @stored_global_logger = $log
    @tmp_dir = tmp_dir
    @tmp_root_dir = File.join(@tmp_dir, 'root')
    FileUtils.mkdir_p(@tmp_dir)
    @sigdump_path = "/tmp/sigdump-#{Process.pid}.log"
  end

  def teardown
    $log = @stored_global_logger
    begin
      FileUtils.rm_rf(@tmp_dir)
    rescue Errno::EACCES
      # It may occur on Windows because of delete pending state due to delayed GC.
      # Ruby 3.2 or later doesn't ignore Errno::EACCES:
      # https://github.com/ruby/ruby/commit/983115cf3c8f75b1afbe3274f02c1529e1ce3a81
    end
  end

  def write_config(path, data)
    FileUtils.mkdir_p(File.dirname(path))
    Fluent::FileWrapper.open(path, "w") {|f| f.write data }
  end


  def test_system_config
    sv = Fluent::Supervisor.new({})
    conf_data = <<-EOC
<system>
  rpc_endpoint 127.0.0.1:24445
  suppress_repeated_stacktrace false
  suppress_config_dump true
  without_source true
  with_source_only true
  enable_get_dump true
  enable_input_metrics false
  process_name "process_name"
  log_level info
  root_dir #{@tmp_root_dir}
  <log>
    path /tmp/fluentd.log
    format json
    time_format %Y
  </log>
  <counter_server>
    bind 127.0.0.1
    port 24321
    scope server1
    backup_path /tmp/backup
  </counter_server>
  <counter_client>
    host 127.0.0.1
    port 24321
    timeout 2
  </counter_client>
  <source_only_buffer>
    flush_thread_count 4
    overflow_action throw_exception
    path /tmp/source-only-buffer
    flush_interval 1
    chunk_limit_size 100
    total_limit_size 1000
    compress gzip
  </source_only_buffer>
</system>
    EOC
    conf = Fluent::Config.parse(conf_data, "(test)", "(test_dir)", true)
    sys_conf = sv.__send__(:build_system_config, conf)

    assert_equal '127.0.0.1:24445', sys_conf.rpc_endpoint
    assert_equal false, sys_conf.suppress_repeated_stacktrace
    assert_equal true, sys_conf.suppress_config_dump
    assert_equal true, sys_conf.without_source
    assert_equal true, sys_conf.with_source_only
    assert_equal true, sys_conf.enable_get_dump
    assert_equal false, sys_conf.enable_input_metrics
    assert_equal "process_name", sys_conf.process_name
    assert_equal 2, sys_conf.log_level
    assert_equal @tmp_root_dir, sys_conf.root_dir
    assert_equal "/tmp/fluentd.log", sys_conf.log.path
    assert_equal :json, sys_conf.log.format
    assert_equal '%Y', sys_conf.log.time_format
    counter_server = sys_conf.counter_server
    assert_equal '127.0.0.1', counter_server.bind
    assert_equal 24321, counter_server.port
    assert_equal 'server1', counter_server.scope
    assert_equal '/tmp/backup', counter_server.backup_path
    counter_client = sys_conf.counter_client
    assert_equal '127.0.0.1', counter_client.host
    assert_equal 24321, counter_client.port
    assert_equal 2, counter_client.timeout
    source_only_buffer = sys_conf.source_only_buffer
    assert_equal 4, source_only_buffer.flush_thread_count
    assert_equal :throw_exception, source_only_buffer.overflow_action
    assert_equal "/tmp/source-only-buffer", source_only_buffer.path
    assert_equal 1, source_only_buffer.flush_interval
    assert_equal 100, source_only_buffer.chunk_limit_size
    assert_equal 1000, source_only_buffer.total_limit_size
    assert_equal :gzip, source_only_buffer.compress
  end

  sub_test_case "yaml config" do
    def parse_yaml(yaml)
      context = Kernel.binding

      config = nil
      Tempfile.open do |file|
        file.puts(yaml)
        file.flush
        s = Fluent::Config::YamlParser::Loader.new(context).load(Pathname.new(file))
        config = Fluent::Config::YamlParser::Parser.new(s).build.to_element
      end
      config
    end

    def test_system_config
      sv = Fluent::Supervisor.new({})
      conf_data = <<-EOC
      system:
        rpc_endpoint: 127.0.0.1:24445
        suppress_repeated_stacktrace: true
        suppress_config_dump: true
        without_source: true
        with_source_only: true
        enable_get_dump: true
        process_name: "process_name"
        log_level: info
        root_dir: !fluent/s "#{@tmp_root_dir}"
        log:
          path: /tmp/fluentd.log
          format: json
          time_format: "%Y"
        counter_server:
          bind: 127.0.0.1
          port: 24321
          scope: server1
          backup_path: /tmp/backup
        counter_client:
          host: 127.0.0.1
          port: 24321
          timeout: 2
        source_only_buffer:
          flush_thread_count: 4
          overflow_action: throw_exception
          path: /tmp/source-only-buffer
          flush_interval: 1
          chunk_limit_size: 100
          total_limit_size: 1000
          compress: gzip
      EOC
      conf = parse_yaml(conf_data)
      sys_conf = sv.__send__(:build_system_config, conf)

    counter_client = sys_conf.counter_client
    counter_server = sys_conf.counter_server
    source_only_buffer = sys_conf.source_only_buffer
    assert_equal(
      [
        '127.0.0.1:24445',
        true,
        true,
        true,
        true,
        true,
        "process_name",
        2,
        @tmp_root_dir,
        "/tmp/fluentd.log",
        :json,
        '%Y',
        '127.0.0.1',
        24321,
        'server1',
        '/tmp/backup',
        '127.0.0.1',
        24321,
        2,
        4,
        :throw_exception,
        "/tmp/source-only-buffer",
        1,
        100,
        1000,
        :gzip,
      ],
      [
        sys_conf.rpc_endpoint,
        sys_conf.suppress_repeated_stacktrace,
        sys_conf.suppress_config_dump,
        sys_conf.without_source,
        sys_conf.with_source_only,
        sys_conf.enable_get_dump,
        sys_conf.process_name,
        sys_conf.log_level,
        sys_conf.root_dir,
        sys_conf.log.path,
        sys_conf.log.format,
        sys_conf.log.time_format,
        counter_server.bind,
        counter_server.port,
        counter_server.scope,
        counter_server.backup_path,
        counter_client.host,
        counter_client.port,
        counter_client.timeout,
        source_only_buffer.flush_thread_count,
        source_only_buffer.overflow_action,
        source_only_buffer.path,
        source_only_buffer.flush_interval,
        source_only_buffer.chunk_limit_size,
        source_only_buffer.total_limit_size,
        source_only_buffer.compress,
      ])
    end
  end

  def test_usr1_in_main_process_signal_handlers
    omit "Windows cannot handle signals" if Fluent.windows?

    create_info_dummy_logger

    sv = Fluent::Supervisor.new({})
    sv.send(:install_main_process_signal_handlers)

    Process.kill :USR1, Process.pid

    sleep 1

    info_msg = "[info]: force flushing buffered events\n"
    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  def test_cont_in_main_process_signal_handlers
    omit "Windows cannot handle signals" if Fluent.windows?

    # https://github.com/fluent/fluentd/issues/4063
    GC.start

    sv = Fluent::Supervisor.new({})
    sv.send(:install_main_process_signal_handlers)

    Process.kill :CONT, Process.pid

    sleep 1

    assert{ File.exist?(@sigdump_path) }
  ensure
    File.delete(@sigdump_path) if File.exist?(@sigdump_path)
  end

  def test_term_cont_in_main_process_signal_handlers
    omit "Windows cannot handle signals" if Fluent.windows?

    # https://github.com/fluent/fluentd/issues/4063
    GC.start

    create_debug_dummy_logger

    sv = Fluent::Supervisor.new({})
    sv.send(:install_main_process_signal_handlers)

    Process.kill :TERM, Process.pid
    Process.kill :CONT, Process.pid

    sleep 1

    debug_msg = "[debug]: fluentd main process get SIGTERM\n"
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }

    assert{ not File.exist?(@sigdump_path) }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
    File.delete(@sigdump_path) if File.exist?(@sigdump_path)
  end

  def test_winch_in_main_process_signal_handlers
    omit "Windows cannot handle signals" if Fluent.windows?

    mock(Fluent::Engine).cancel_source_only!
    create_info_dummy_logger

    sv = Fluent::Supervisor.new({})
    sv.send(:install_main_process_signal_handlers)

    Process.kill :WINCH, Process.pid

    sleep 1

    info_msg = "[info]: try to cancel with-source-only mode\n"
    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  def test_main_process_command_handlers
    omit "Only for Windows, alternative to UNIX signals" unless Fluent.windows?

    create_info_dummy_logger

    sv = Fluent::Supervisor.new({})
    r, w = IO.pipe
    $stdin = r
    sv.send(:install_main_process_signal_handlers)

    begin
      w.write("GRACEFUL_RESTART\n")
      w.flush
    ensure
      $stdin = STDIN
    end

    sleep 1

    info_msg = "[info]: force flushing buffered events\n"
    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  def test_usr1_in_supervisor_signal_handler
    omit "Windows cannot handle signals" if Fluent.windows?

    create_debug_dummy_logger

    server = DummyServer.new
    server.install_supervisor_signal_handlers

    Process.kill :USR1, Process.pid

    sleep 1

    debug_msg = '[debug]: fluentd supervisor process get SIGUSR1'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  def test_cont_in_supervisor_signal_handler
    omit "Windows cannot handle signals" if Fluent.windows?

    # https://github.com/fluent/fluentd/issues/4063
    GC.start

    server = DummyServer.new
    server.install_supervisor_signal_handlers

    Process.kill :CONT, Process.pid

    sleep 1

    assert{ File.exist?(@sigdump_path) }
  ensure
    File.delete(@sigdump_path) if File.exist?(@sigdump_path)
  end

  def test_term_cont_in_supervisor_signal_handler
    omit "Windows cannot handle signals" if Fluent.windows?

    # https://github.com/fluent/fluentd/issues/4063
    GC.start

    server = DummyServer.new
    server.install_supervisor_signal_handlers

    Process.kill :TERM, Process.pid
    Process.kill :CONT, Process.pid

    assert{ not File.exist?(@sigdump_path) }
  ensure
    File.delete(@sigdump_path) if File.exist?(@sigdump_path)
  end

  def test_winch_in_supervisor_signal_handler
    omit "Windows cannot handle signals" if Fluent.windows?

    create_debug_dummy_logger

    server = DummyServer.new
    server.install_supervisor_signal_handlers

    Process.kill :WINCH, Process.pid

    sleep 1

    debug_msg = '[debug]: fluentd supervisor process got SIGWINCH'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  def test_windows_shutdown_event
    omit "Only for Windows platform" unless Fluent.windows?

    create_debug_dummy_logger

    server = DummyServer.new
    def server.config
      {:signame => "TestFluentdEvent"}
    end

    mock(server).stop(true)
    stub(Process).kill.times(0)

    server.install_windows_event_handler
    begin
      sleep 0.1 # Wait for starting windows event thread
      event = Win32::Event.open("TestFluentdEvent")
      event.set
      event.close
      sleep 1.0 # Wait for dumping
    ensure
      server.stop_windows_event_thread
    end

    debug_msg = '[debug]: Got Win32 event "TestFluentdEvent"'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  def test_supervisor_event_handler
    omit "Only for Windows, alternative to UNIX signals" unless Fluent.windows?

    create_debug_dummy_logger

    server = DummyServer.new
    def server.config
      {:signame => "TestFluentdEvent"}
    end
    server.install_windows_event_handler
    begin
      sleep 0.1 # Wait for starting windows event thread
      event = Win32::Event.open("TestFluentdEvent_USR1")
      event.set
      event.close
      sleep 1.0 # Wait for dumping
    ensure
      server.stop_windows_event_thread
    end

    debug_msg = '[debug]: Got Win32 event "TestFluentdEvent_USR1"'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log&.out&.respond_to?(:reset)
  end

  data("Normal", {raw_path: "C:\\Windows\\Temp\\sigdump.log", expected: "C:\\Windows\\Temp\\sigdump-#{Process.pid}.log"})
  data("UNIX style", {raw_path: "/Windows/Temp/sigdump.log", expected: "/Windows/Temp/sigdump-#{Process.pid}.log"})
  data("No extension", {raw_path: "C:\\Windows\\Temp\\sigdump", expected: "C:\\Windows\\Temp\\sigdump-#{Process.pid}"})
  data("Multi-extension", {raw_path: "C:\\Windows\\Temp\\sig.dump.bk", expected: "C:\\Windows\\Temp\\sig.dump-#{Process.pid}.bk"})
  def test_fluentsigdump_get_path_with_pid(data)
    path = Fluent::FluentSigdump.get_path_with_pid(data[:raw_path])
    assert_equal(data[:expected], path)
  end

  def test_supervisor_event_dump_windows
    omit "Only for Windows, alternative to UNIX signals" unless Fluent.windows?

    # https://github.com/fluent/fluentd/issues/4063
    GC.start

    ENV['SIGDUMP_PATH'] = @tmp_dir + "/sigdump.log"

    server = DummyServer.new
    def server.config
      {:signame => "TestFluentdEvent"}
    end
    server.install_windows_event_handler

    begin
      sleep 0.1 # Wait for starting windows event thread
      event = Win32::Event.open("TestFluentdEvent_CONT")
      event.set
      event.close
      sleep 1.0 # Wait for dumping
    ensure
      server.stop_windows_event_thread
    end

    result_filepaths = Dir.glob("#{@tmp_dir}/*")
    assert {result_filepaths.length > 0}
  ensure
    ENV.delete('SIGDUMP_PATH')
  end

  data(:ipv4 => ["0.0.0.0", "127.0.0.1", false],
       :ipv6 => ["[::]", "[::1]", true],
       :localhost_ipv4 => ["localhost", "127.0.0.1", false])
  def test_rpc_server(data)
    omit "Windows cannot handle signals" if Fluent.windows?

    bindaddr, localhost, ipv6 = data
    omit "IPv6 is not supported on this environment" if ipv6 && !ipv6_enabled?

    create_info_dummy_logger

    sv = Fluent::Supervisor.new({})
    conf_data = <<-EOC
  <system>
    rpc_endpoint "#{bindaddr}:24447"
  </system>
    EOC
    conf = Fluent::Config.parse(conf_data, "(test)", "(test_dir)", true)
    sys_conf = sv.__send__(:build_system_config, conf)

    server = DummyServer.new
    server.rpc_endpoint = sys_conf.rpc_endpoint
    server.enable_get_dump = sys_conf.enable_get_dump

    server.run_rpc_server

    sv.send(:install_main_process_signal_handlers)
    response = Net::HTTP.get(URI.parse("http://#{localhost}:24447/api/plugins.flushBuffers"))
    info_msg = "[info]: force flushing buffered events\n"

    server.stop_rpc_server

    # In TravisCI with OSX(Xcode), it seems that can't use rpc server.
    # This test will be passed in such environment.
    pend unless $log.out.logs.first

    assert_equal('{"ok":true}', response)
    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset if $log.out.is_a?(Fluent::Test::DummyLogDevice)
  end

  data(:no_port => ["127.0.0.1"],
       :invalid_addr => ["*:24447"])
  def test_invalid_rpc_endpoint(data)
    endpoint = data[0]

    sv = Fluent::Supervisor.new({})
    conf_data = <<-EOC
  <system>
    rpc_endpoint "#{endpoint}"
  </system>
    EOC
    conf = Fluent::Config.parse(conf_data, "(test)", "(test_dir)", true)
    sys_conf = sv.__send__(:build_system_config, conf)

    server = DummyServer.new
    server.rpc_endpoint = sys_conf.rpc_endpoint

    assert_raise(Fluent::ConfigError.new("Invalid rpc_endpoint: #{endpoint}")) do
      server.run_rpc_server
    end
  end

  data(:ipv4 => ["0.0.0.0", "127.0.0.1", false],
       :ipv6 => ["[::]", "[::1]", true],
       :localhost_ipv4 => ["localhost", "127.0.0.1", true])
  def test_rpc_server_windows(data)
    omit "Only for windows platform" unless Fluent.windows?

    bindaddr, localhost, ipv6 = data
    omit "IPv6 is not supported on this environment" if ipv6 && !ipv6_enabled?

    create_info_dummy_logger

    sv = Fluent::Supervisor.new({})
    conf_data = <<-EOC
  <system>
    rpc_endpoint "#{bindaddr}:24447"
  </system>
    EOC
    conf = Fluent::Config.parse(conf_data, "(test)", "(test_dir)", true)
    sys_conf = sv.__send__(:build_system_config, conf)

    server = DummyServer.new
    def server.config
      {
        :signame => "TestFluentdEvent",
        :worker_pid => 5963,
      }
    end
    server.rpc_endpoint = sys_conf.rpc_endpoint

    server.run_rpc_server

    mock(server).restart(true) { nil }
    response = Net::HTTP.get(URI.parse("http://#{localhost}:24447/api/plugins.flushBuffers"))

    server.stop_rpc_server
    assert_equal('{"ok":true}', response)
  end

  sub_test_case "serverengine_config" do
    def test_normal
      params = {}
      params['workers'] = 1
      params['fluentd_conf_path'] = "fluentd.conf"
      params['use_v1_config'] = true
      params['conf_encoding'] = 'utf-8'
      params['log_level'] = Fluent::Log::LEVEL_INFO
      load_config_proc =  Proc.new { Fluent::Supervisor.serverengine_config(params) }

      se_config = load_config_proc.call
      assert_equal Fluent::Log::LEVEL_INFO, se_config[:log_level]
      assert_equal 'spawn', se_config[:worker_type]
      assert_equal 1, se_config[:workers]
      assert_equal false, se_config[:log_stdin]
      assert_equal false, se_config[:log_stdout]
      assert_equal false, se_config[:log_stderr]
      assert_equal true, se_config[:enable_heartbeat]
      assert_equal false, se_config[:auto_heartbeat]
      assert_equal "fluentd.conf", se_config[:config_path]
      assert_equal false, se_config[:daemonize]
      assert_nil se_config[:pid_path]
    end

    def test_daemonize
      params = {}
      params['workers'] = 1
      params['fluentd_conf_path'] = "fluentd.conf"
      params['use_v1_config'] = true
      params['conf_encoding'] = 'utf-8'
      params['log_level'] = Fluent::Log::LEVEL_INFO
      params['daemonize'] = './fluentd.pid'
      load_config_proc = Proc.new { Fluent::Supervisor.serverengine_config(params) }

      se_config = load_config_proc.call
      assert_equal Fluent::Log::LEVEL_INFO, se_config[:log_level]
      assert_equal 'spawn', se_config[:worker_type]
      assert_equal 1, se_config[:workers]
      assert_equal false, se_config[:log_stdin]
      assert_equal false, se_config[:log_stdout]
      assert_equal false, se_config[:log_stderr]
      assert_equal true, se_config[:enable_heartbeat]
      assert_equal false, se_config[:auto_heartbeat]
      assert_equal "fluentd.conf", se_config[:config_path]
      assert_equal true, se_config[:daemonize]
      assert_equal './fluentd.pid', se_config[:pid_path]
    end

    data("nil", [nil, nil])
    data("default", ["0", 0])
    data("000", ["000", 0])
    data("0000", ["0000", 0])
    data("2", ["2", 2])
    data("222", ["222", 146])
    data("0222", ["0222", 146])
    data("0 as integer", [0, 0])
    def test_chumask((chumask, expected))
      params = { "chumask" => chumask }
      load_config_proc = Proc.new { Fluent::Supervisor.serverengine_config(params) }

      se_config = load_config_proc.call

      assert_equal expected, se_config[:chumask]
    end
  end

  data("default", [{}, "0"])
  data("222", [{chumask: "222"}, "222"])
  def test_chumask_should_be_passed_to_ServerEngine((cl_opt, expected_chumask_value))
    proxy.mock(Fluent::Supervisor).serverengine_config(hash_including("chumask" => expected_chumask_value))
    any_instance_of(ServerEngine::Daemon) { |daemon| mock(daemon).run.once }

    supervisor = Fluent::Supervisor.new(cl_opt)
    stub(Fluent::Config).build { config_element('ROOT') }
    stub(supervisor).build_spawn_command { "dummy command line" }
    supervisor.configure(supervisor: true)

    supervisor.run_supervisor
  end

  sub_test_case "init logger" do
    data(supervisor: true)
    data(worker: false)
    def test_init_for_logger(supervisor)
      tmp_conf_path = "#{@tmp_dir}/dir/test_init_for_logger.conf"
      conf_info_str = <<~EOC
        <system>
          log_level warn # To suppress logs
          suppress_repeated_stacktrace false
          ignore_repeated_log_interval 10s
          ignore_same_log_interval 20s
          <log>
            format json
            time_format %FT%T.%L%z
            forced_stacktrace_level info
          </log>
        </system>
      EOC
      write_config tmp_conf_path, conf_info_str

      s = Fluent::Supervisor.new({config_path: tmp_conf_path})
      s.configure(supervisor: supervisor)

      assert_equal :json, $log.format
      assert_equal '%FT%T.%L%z', $log.time_format
      assert_equal false, $log.suppress_repeated_stacktrace
      assert_equal 10, $log.ignore_repeated_log_interval
      assert_equal 20, $log.ignore_same_log_interval
      assert_equal Fluent::Log::LEVEL_INFO, $log.instance_variable_get(:@forced_stacktrace_level)
      assert_true $log.force_stacktrace_level?
    end

    data(
      daily_age: 'daily',
      weekly_age: 'weekly',
      monthly_age: 'monthly',
      integer_age: 2,
    )
    def test_logger_with_rotate_age_and_rotate_size(rotate_age)
      config_path = "#{@tmp_dir}/empty.conf"
      write_config config_path, ""

      sv = Fluent::Supervisor.new(
        config_path: config_path,
        log_path: "#{@tmp_dir}/test",
        log_rotate_age: rotate_age,
        log_rotate_size: 10,
      )
      sv.__send__(:setup_global_logger)

      assert_equal Fluent::LogDeviceIO, $log.out.class
      assert_equal rotate_age, $log.out.instance_variable_get(:@shift_age)
      assert_equal 10, $log.out.instance_variable_get(:@shift_size)
    end

    def test_can_start_with_rotate_but_no_log_path
      config_path = "#{@tmp_dir}/empty.conf"
      write_config config_path, ""

      sv = Fluent::Supervisor.new(
        config_path: config_path,
        log_rotate_age: 5,
      )
      sv.__send__(:setup_global_logger)

      assert_true $log.stdout?
    end

    sub_test_case "system log rotation" do
      def parse_text(text)
        basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
        Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
      end

      def test_override_default_log_rotate
        Tempfile.open do |file|
          config = parse_text(<<-EOS)
            <system>
              <log>
                rotate_age 3
                rotate_size 300
              </log>
            </system>
          EOS
          file.puts(config)
          file.flush
          sv = Fluent::Supervisor.new({log_path: "#{@tmp_dir}/test.log", config_path: file.path})

          sv.__send__(:setup_global_logger)
          logger = $log.instance_variable_get(:@logger)

          assert_equal Fluent::LogDeviceIO, $log.out.class
          assert_equal 3, $log.out.instance_variable_get(:@shift_age)
          assert_equal 300, $log.out.instance_variable_get(:@shift_size)
        end
      end

      def test_override_default_log_rotate_with_yaml_config
        Tempfile.open do |file|
          config = <<-EOS
            system:
              log:
                rotate_age: 3
                rotate_size: 300
          EOS
          file.puts(config)
          file.flush
          sv = Fluent::Supervisor.new({log_path: "#{@tmp_dir}/test.log", config_path: file.path, config_file_type: :yaml})

          sv.__send__(:setup_global_logger)
          logger = $log.instance_variable_get(:@logger)

          assert_equal Fluent::LogDeviceIO, $log.out.class
          assert_equal 3, $log.out.instance_variable_get(:@shift_age)
          assert_equal 300, $log.out.instance_variable_get(:@shift_size)
        end
      end
    end

    def test_log_level_affects
      sv = Fluent::Supervisor.new({})

      c = Fluent::Config::Element.new('system', '', { 'log_level' => 'error' }, [])
      stub(Fluent::Config).build { config_element('ROOT', '', {}, [c]) }

      sv.configure
      assert_equal Fluent::Log::LEVEL_ERROR, $log.level
    end

    data(supervisor: true)
    data(worker: false)
    def test_log_path(supervisor)
      log_path = Pathname(@tmp_dir) + "fluentd.log"
      config_path = Pathname(@tmp_dir) + "fluentd.conf"
      write_config config_path.to_s, ""

      s = Fluent::Supervisor.new(config_path: config_path.to_s, log_path: log_path.to_s)
      assert_rr do
        mock.proxy(File).chmod(0o777, log_path.parent.to_s).never
        s.__send__(:setup_global_logger, supervisor: supervisor)
      end

      assert { log_path.parent.exist? }
    ensure
      $log.out.close
    end

    data(supervisor: true)
    data(worker: false)
    def test_dir_permission(supervisor)
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      log_path = Pathname(@tmp_dir) + "fluentd.log"
      config_path = Pathname(@tmp_dir) + "fluentd.conf"
      conf = <<~EOC
        <system>
          dir_permission 0o777
        </system>
      EOC
      write_config config_path.to_s, conf

      s = Fluent::Supervisor.new(config_path: config_path.to_s, log_path: log_path.to_s)
      assert_rr do
        mock.proxy(File).chmod(0o777, log_path.parent.to_s).once
        s.__send__(:setup_global_logger, supervisor: supervisor)
      end

      assert { log_path.parent.exist? }
      assert { (File.stat(log_path.parent).mode & 0xFFF) == 0o777 }
    ensure
      $log.out.close
    end

    def test_files_for_each_process_with_rotate_on_windows
      omit "Only for Windows." unless Fluent.windows?

      log_path = Pathname(@tmp_dir) + "log" + "fluentd.log"
      config_path = Pathname(@tmp_dir) + "fluentd.conf"
      conf = <<~EOC
        <system>
          <log>
            rotate_age 5
          </log>
        </system>
      EOC
      write_config config_path.to_s, conf

      s = Fluent::Supervisor.new(config_path: config_path.to_s, log_path: log_path.to_s)
      s.__send__(:setup_global_logger, supervisor: true)
      $log.out.close

      s = Fluent::Supervisor.new(config_path: config_path.to_s, log_path: log_path.to_s)
      s.__send__(:setup_global_logger, supervisor: false)
      $log.out.close

      ENV["SERVERENGINE_WORKER_ID"] = "1"
      s = Fluent::Supervisor.new(config_path: config_path.to_s, log_path: log_path.to_s)
      s.__send__(:setup_global_logger, supervisor: false)
      $log.out.close

      assert { log_path.parent.entries.size == 5 } # [".", "..", "logfile.log", ...]
    ensure
      ENV.delete("SERVERENGINE_WORKER_ID")
    end
  end

  def test_enable_shared_socket
    server = DummyServer.new
    begin
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
      server.before_run
      sleep 0.1 if Fluent.windows? # Wait for starting windows event thread
      assert_not_nil(ENV['SERVERENGINE_SOCKETMANAGER_PATH'])
    ensure
      server.after_run
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
    end
  end

  def test_disable_shared_socket
    server = DummyServer.new
    def server.config
      {
        :disable_shared_socket => true,
      }
    end
    begin
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
      server.before_run
      sleep 0.1 if Fluent.windows? # Wait for starting windows event thread
      assert_nil(ENV['SERVERENGINE_SOCKETMANAGER_PATH'])
    ensure
      server.after_run
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
    end
  end

  sub_test_case "zero_downtime_restart" do
    setup do
      omit "Not supported on Windows" if Fluent.windows?
    end

    data(
      # When daemonize, exit-status is important. The new spawned process does double-fork and exits soon.
      "daemonize and succeeded double-fork of new process" => [true, true, 0, false],
      "daemonize and failed double-fork of new process" => [true, false, 0, true],
      # When no daemon, whether the new spawned process is alive is important, not exit-status.
      "no daemon and new process alive" => [false, false, 3, false],
      "no daemon and new process dead" => [false, false, 0, true],
    )
    def test_zero_downtime_restart((daemonize, wait_success, wait_sleep, restart_canceled))
      # == Arrange ==
      env_spawn = {}
      pid_wait = nil

      server = DummyServer.new

      stub(server).config do
        {
          daemonize: daemonize,
          pid_path: "test-pid-file",
        }
      end
      process_stub = stub(Process)
      process_stub.spawn do |env, commands|
        env_spawn = env
        -1
      end
      process_stub.wait2 do |pid|
        pid_wait = pid
        sleep wait_sleep
        if wait_success
          status = Class.new{def success?; true; end}.new
        else
          status = Class.new{def success?; false; end}.new
        end
        [pid, status]
      end
      stub(File).read("test-pid-file") { -1 }

      # mock to check notify_new_supervisor_that_old_one_has_stopped sends SIGWINCH
      if restart_canceled
        mock(Process).kill(:WINCH, -1).never
      else
        mock(Process).kill(:WINCH, -1)
      end

      # == Act and Assert ==
      server.before_run
      server.zero_downtime_restart.join
      sleep 1 # To wait a sub thread for waitpid in zero_downtime_restart
      server.after_run

      assert_equal(
        [
          !restart_canceled,
          true,
          Process.pid,
          -1,
        ],
        [
          server.instance_variable_get(:@starting_new_supervisor_with_zero_downtime),
          env_spawn.key?("SERVERENGINE_SOCKETMANAGER_INTERNAL_TOKEN"),
          env_spawn["FLUENT_RUNNING_IN_PARALLEL_WITH_OLD"].to_i,
          pid_wait,
        ]
      )
    ensure
      Fluent::Supervisor.cleanup_socketmanager_path
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
    end

    def test_share_sockets
      server = DummyServer.new
      server.before_run
      path = ENV['SERVERENGINE_SOCKETMANAGER_PATH']

      client = ServerEngine::SocketManager::Client.new(path)
      udp_port = unused_port(protocol: :udp)
      tcp_port = unused_port(protocol: :tcp)
      client.listen_udp("localhost", udp_port)
      client.listen_tcp("localhost", tcp_port)

      ENV['FLUENT_RUNNING_IN_PARALLEL_WITH_OLD'] = ""
      new_server = DummyServer.new
      stub(new_server).stop_parallel_old_supervisor_after_delay
      new_server.before_run

      assert_equal(
        [[udp_port], [tcp_port]],
        [
          new_server.socket_manager_server.udp_sockets.values.map { |v| v.addr[1] },
          new_server.socket_manager_server.tcp_sockets.values.map { |v| v.addr[1] },
        ]
      )
    ensure
      server&.after_run
      new_server&.after_run
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
      ENV.delete("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD")
    end

    def test_stop_parallel_old_supervisor_after_delay
      ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = ""
      ENV['FLUENT_RUNNING_IN_PARALLEL_WITH_OLD'] = "-1"
      stub(ServerEngine::SocketManager::Server).share_sockets_with_another_server
      mock(Process).kill(:TERM, -1)

      server = DummyServer.new
      server.before_run
      sleep 12 # Can't we skip the delay for this test?
    ensure
      server&.after_run
      ENV.delete('SERVERENGINE_SOCKETMANAGER_PATH')
      ENV.delete("FLUENT_RUNNING_IN_PARALLEL_WITH_OLD")
    end
  end

  sub_test_case "include additional configuration" do
    setup do
      @config_include_dir = File.join(@tmp_dir, "conf.d")
      FileUtils.mkdir_p(@config_include_dir)
    end

    test "no additional configuration" do
      c = Fluent::Config::Element.new('system', '', { 'config_include_dir' => '' }, [])
      stub(Fluent::Config).build { config_element('ROOT', '', {}, [c]) }
      supervisor = Fluent::Supervisor.new({})
      supervisor.configure(supervisor: true)
      assert_equal([c], supervisor.instance_variable_get(:@conf).elements)
    end

    data(
      "single source" => ["forward"],
      "multiple sources" => ["forward", "tcp"])
    test "additional configuration" do |sources|
      c = Fluent::Config::Element.new('system', '',
                                      { 'config_include_dir' => @config_include_dir }, [])
      config_path = "#{@config_include_dir}/dummy.conf"
      stub.proxy(Fluent::Config).build
      stub(Fluent::Config).build(config_path: "/etc/fluent/fluent.conf", encoding: "utf-8",
                                 additional_config: anything, use_v1_config: anything,
                                 type: anything,
                                 on_file_parsed: anything) { config_element('ROOT', '', {}, [c]) }
      sources.each do |type|
        config = <<~EOF
        <source>
          @type #{type}
        </source>
        EOF
        additional_config_path = "#{@config_include_dir}/#{type}.conf"
        write_config(additional_config_path, config)
      end
      supervisor = Fluent::Supervisor.new({})
      supervisor.configure(supervisor: true)
      expected = [c].concat(sources.collect { |type| {"@type" => type} })
      assert_equal(expected, supervisor.instance_variable_get(:@conf).elements)
    end

    data(
      "single YAML source" => ["forward"],
      "multiple YAML sources" => ["forward", "tcp"])
    test "additional YAML configuration" do |sources|
      c = Fluent::Config::Element.new('system', '',
                                      { 'config_include_dir' => @config_include_dir }, [])
      config_path = "#{@config_include_dir}/dummy.yml"
      stub.proxy(Fluent::Config).build
      stub(Fluent::Config).build(config_path: "/etc/fluent/fluent.conf", encoding: "utf-8",
                                 additional_config: anything, use_v1_config: anything,
                                 type: anything,
                                 on_file_parsed: anything) { config_element('ROOT', '', {}, [c]) }
      sources.each do |type|
        config = <<~EOF
        config:
          - source:
              $type: #{type}
        EOF
        additional_config_path = "#{@config_include_dir}/#{type}.yml"
        write_config(additional_config_path, config)
      end
      supervisor = Fluent::Supervisor.new({})
      supervisor.configure(supervisor: true)
      expected = [c].concat(sources.collect { |type| {"@type" => type} })
      assert_equal(expected, supervisor.instance_variable_get(:@conf).elements)
    end

    data(
      "single source" => [false, ["forward"]],
      "multiple sources" => [false, ["forward", "tcp"]],
      "single YAML source" => [true, ["forward"]],
      "multiple YAML sources" => [true, ["forward", "tcp"]])
    test "reload with additional configuration" do |(yaml, sources)|
      c = Fluent::Config::Element.new('system', '',
                                      { 'config_include_dir' => @config_include_dir }, [])
      config_path = "#{@config_include_dir}/dummy.yml"
      stub.proxy(Fluent::Config).build
      stub(Fluent::Config).build(config_path: "/etc/fluent/fluent.conf", encoding: "utf-8",
                                 additional_config: anything, use_v1_config: anything,
                                 type: anything,
                                 on_file_parsed: anything) { config_element('ROOT', '', {}, [c]) }
      sources.each do |type|
        if yaml
          config = <<~EOF
          config:
            - source:
                $type: #{type}
          EOF
          additional_config_path = "#{@config_include_dir}/#{type}.yml"
          write_config(additional_config_path, config)
        else
          config = <<~EOF
          <source>
            @type #{type}
          </source>
          EOF
          additional_config_path = "#{@config_include_dir}/#{type}.conf"
          write_config(additional_config_path, config)
        end
      end
      supervisor = Fluent::Supervisor.new({})
      supervisor.configure(supervisor: true)
      supervisor.__send__(:reload_config)
      expected = [c].concat(sources.collect { |type| {"@type" => type} })
      assert_equal(expected, supervisor.instance_variable_get(:@conf).elements)
    end

    test "prevent duplicate loading" do
      write_config("#{@config_include_dir}/system.conf", <<~EOF)
        <system>
          config_include_dir #{@config_include_dir}
        </system>
      EOF
      write_config("#{@config_include_dir}/forward.conf", <<~EOF)
        <match test>
          @type forward
          <buffer>
            @include #{@config_include_dir}/common_param.conf
          </buffer>
          <server>
            host 127.0.0.1
            port 24224
          </server>
        </match>
      EOF
      write_config("#{@config_include_dir}/common_param.conf", <<~EOF)
        flush_interval 5s
      EOF
      write_config("#{@config_include_dir}/obsolete_plugins.conf", <<~EOF)
        <source>
          @type obsolete_plugins
        </source>
      EOF

      write_config("#{@tmp_dir}/fluent.conf", <<~EOF)
        <match sample.*>
          @type file
          <buffer>
            @include #{@config_include_dir}/common_param.conf
          </buffer>
        </match>

        @include #{@config_include_dir}/forward.conf
        @include #{@config_include_dir}/system.conf
      EOF

      supervisor = Fluent::Supervisor.new({ config_path: "#{@tmp_dir}/fluent.conf" })
      stub(supervisor).setup_global_logger { create_debug_dummy_logger }

      supervisor.configure(supervisor: true)
      elements = supervisor.instance_variable_get(:@conf).elements
      assert_equal(4, elements.size)

      assert_equal('match', elements[0].name)
      assert_equal('file', elements[0]['@type'])
      assert_equal('buffer', elements[0].elements[0].name)
      assert_equal('5s', elements[0].elements[0]['flush_interval'])

      assert_equal('match', elements[1].name)
      assert_equal('forward', elements[1]['@type'])
      assert_equal('buffer', elements[1].elements[0].name)
      assert_equal('5s', elements[1].elements[0]['flush_interval'])

      assert_equal('system', elements[2].name)
      assert_equal(@config_include_dir, elements[2]['config_include_dir'])

      assert_equal('source', elements[3].name)
      assert_equal('obsolete_plugins', elements[3]['@type'])

      skipped_files = %W[
        #{@config_include_dir}/common_param.conf
        #{@config_include_dir}/forward.conf
        #{@config_include_dir}/system.conf
      ]
      loaded_files = %W[
        #{@config_include_dir}/obsolete_plugins.conf
      ]

      logs_line = $log.out.logs.join
      skipped_files.each do |path|
        assert { logs_line.include?("skip auto loading, it was already loaded path=\"#{path}\"") }
      end
      loaded_files.each do |path|
        assert { logs_line.include?("loading additional configuration file path=\"#{path}\"") }
      end

      # reload
      $log.out.reset
      supervisor.__send__(:reload_config)
      sleep 0.2 # wait to finish reloading

      reload_elements = supervisor.instance_variable_get(:@conf).elements
      assert_equal(elements, reload_elements)

      logs_line = $log.out.logs.join
      skipped_files.each do |path|
        assert { logs_line.include?("skip auto loading, it was already loaded path=\"#{path}\"") }
      end
      loaded_files.each do |path|
        assert { logs_line.include?("loading additional configuration file path=\"#{path}\"") }
      end
    ensure
      $log.out.reset if $log&.out&.respond_to?(:reset)
    end

    test "do not load additional configuration when loaded all files with @include" do
      write_config("#{@config_include_dir}/forward.conf", <<~EOF)
        <match test>
          @type forward
          <server>
            host 127.0.0.1
            port 24224
          </server>
        </match>
      EOF
      write_config("#{@config_include_dir}/obsolete_plugins.conf", <<~EOF)
        <source>
          @type obsolete_plugins
        </source>
      EOF

      write_config("#{@tmp_dir}/fluent.conf", <<~EOF)
        <system>
          config_include_dir #{@config_include_dir}
        </system>

        <match sample.*>
          @type file
        </match>

        @include #{@config_include_dir}/*.conf
      EOF

      supervisor = Fluent::Supervisor.new({ config_path: "#{@tmp_dir}/fluent.conf" })
      stub(supervisor).setup_global_logger { create_debug_dummy_logger }

      supervisor.configure(supervisor: true)
      elements = supervisor.instance_variable_get(:@conf).elements
      assert_equal(4, elements.size)

      assert_equal('system', elements[0].name)
      assert_equal(@config_include_dir, elements[0]['config_include_dir'])

      assert_equal('match', elements[1].name)
      assert_equal('file', elements[1]['@type'])

      assert_equal('match', elements[2].name)
      assert_equal('forward', elements[2]['@type'])

      assert_equal('source', elements[3].name)
      assert_equal('obsolete_plugins', elements[3]['@type'])

      # no additional load, all files were skipped
      skipped_files = %W[
        #{@config_include_dir}/forward.conf
        #{@config_include_dir}/obsolete_plugins.conf
      ]

      logs_line = $log.out.logs.join
      skipped_files.each do |path|
        assert { logs_line.include?("skip auto loading, it was already loaded path=\"#{path}\"") }
      end
      assert_not_match(/loading additional configuration file/, logs_line)
    ensure
      $log.out.reset if $log&.out&.respond_to?(:reset)
    end

    test "can load partial config loaded config_include_dir feature by even if already loaded" do
      write_config("#{@config_include_dir}/system.conf", <<~EOF)
        <system>
          config_include_dir #{@config_include_dir}
        </system>
      EOF
      write_config("#{@config_include_dir}/forward.conf", <<~EOF)
        <match test>
          @type forward
          <buffer>
            @include #{@config_include_dir}/common_param.conf
          </buffer>
          <server>
            host 127.0.0.1
            port 24224
          </server>
        </match>
      EOF
      write_config("#{@config_include_dir}/common_param.conf", <<~EOF)
        flush_interval 5s
      EOF
      write_config("#{@config_include_dir}/obsolete_plugins.conf", <<~EOF)
        <source>
          @type obsolete_plugins
        </source>
      EOF

      write_config("#{@tmp_dir}/fluent.conf", <<~EOF)
        <match sample.*>
          @type file
          <buffer>
            @include #{@config_include_dir}/common_param.conf
          </buffer>
        </match>

        @include #{@config_include_dir}/system.conf
      EOF

      supervisor = Fluent::Supervisor.new({ config_path: "#{@tmp_dir}/fluent.conf" })
      stub(supervisor).setup_global_logger { create_debug_dummy_logger }

      supervisor.configure(supervisor: true)
      elements = supervisor.instance_variable_get(:@conf).elements
      assert_equal(4, elements.size)

      assert_equal('match', elements[0].name)
      assert_equal('file', elements[0]['@type'])
      assert_equal('buffer', elements[0].elements[0].name)
      assert_equal('5s', elements[0].elements[0]['flush_interval'])

      assert_equal('system', elements[1].name)
      assert_equal(@config_include_dir, elements[1]['config_include_dir'])

      # include forward.conf using config_include_dir feature
      assert_equal('match', elements[2].name)
      assert_equal('forward', elements[2]['@type'])
      assert_equal('buffer', elements[2].elements[0].name)
      assert_equal('5s', elements[2].elements[0]['flush_interval'])

      assert_equal('source', elements[3].name)
      assert_equal('obsolete_plugins', elements[3]['@type'])

      skipped_files = %W[
        #{@config_include_dir}/common_param.conf
        #{@config_include_dir}/system.conf
      ]
      loaded_files = %W[
        #{@config_include_dir}/forward.conf
        #{@config_include_dir}/obsolete_plugins.conf
      ]

      logs_line = $log.out.logs.join
      skipped_files.each do |path|
        assert { logs_line.include?("skip auto loading, it was already loaded path=\"#{path}\"") }
      end
      loaded_files.each do |path|
        assert { logs_line.include?("loading additional configuration file path=\"#{path}\"") }
      end
    ensure
      $log.out.reset if $log&.out&.respond_to?(:reset)
    end

    test "can load config files even if disable config_include_dir" do
      write_config("#{@config_include_dir}/forward.conf", <<~EOF)
        <match test>
          @type forward
          <server>
            host 127.0.0.1
            port 24224
          </server>
        </match>
      EOF

      write_config("#{@tmp_dir}/fluent.conf", <<~EOF)
        <system>
          config_include_dir "" 
        </system>

        <match sample.*>
          @type file
        </match>

        @include #{@config_include_dir}/*.conf
      EOF

      supervisor = Fluent::Supervisor.new({ config_path: "#{@tmp_dir}/fluent.conf" })
      stub(supervisor).setup_global_logger { create_debug_dummy_logger }

      supervisor.configure(supervisor: true)
      elements = supervisor.instance_variable_get(:@conf).elements
      assert_equal(3, elements.size)

      assert_equal('system', elements[0].name)
      assert_equal("", elements[0]['config_include_dir'])

      assert_equal('match', elements[1].name)
      assert_equal('file', elements[1]['@type'])

      assert_equal('match', elements[2].name)
      assert_equal('forward', elements[2]['@type'])

      # There is no logs for additional loading
      logs_line = $log.out.logs.join
      assert_not_match(/skip auto loading, it was already loaded/, logs_line)
      assert_not_match(/loading additional configuration file/, logs_line)
    ensure
      $log.out.reset if $log&.out&.respond_to?(:reset)
    end
  end

  def create_debug_dummy_logger
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::DEBUG
    logdev = Fluent::Test::DummyLogDevice.new
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    $log = Fluent::Log.new(logger)
  end

  def create_info_dummy_logger
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::INFO
    logdev = Fluent::Test::DummyLogDevice.new
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    $log = Fluent::Log.new(logger)
  end
end
