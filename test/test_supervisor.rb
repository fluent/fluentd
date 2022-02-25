require_relative 'helper'
require 'fluent/event_router'
require 'fluent/system_config'
require 'fluent/supervisor'
require_relative 'test_plugin_classes'

require 'net/http'
require 'uri'
require 'fileutils'
require 'tempfile'

if Fluent.windows?
  require 'win32/event'
end

class SupervisorTest < ::Test::Unit::TestCase
  class DummyServer
    include Fluent::ServerModule
    attr_accessor :rpc_endpoint, :enable_get_dump
    def config
      {}
    end
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/supervisor#{ENV['TEST_ENV_NUMBER']}")
  TMP_ROOT_DIR = File.join(TMP_DIR, 'root')

  def setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  def write_config(path, data)
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w") {|f| f.write data }
  end


  def test_system_config
    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
    conf_data = <<-EOC
<system>
  rpc_endpoint 127.0.0.1:24445
  suppress_repeated_stacktrace true
  suppress_config_dump true
  without_source true
  enable_get_dump true
  process_name "process_name"
  log_level info
  root_dir #{TMP_ROOT_DIR}
  <log>
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
</system>
    EOC
    conf = Fluent::Config.parse(conf_data, "(test)", "(test_dir)", true)
    sys_conf = sv.__send__(:build_system_config, conf)

    assert_equal '127.0.0.1:24445', sys_conf.rpc_endpoint
    assert_equal true, sys_conf.suppress_repeated_stacktrace
    assert_equal true, sys_conf.suppress_config_dump
    assert_equal true, sys_conf.without_source
    assert_equal true, sys_conf.enable_get_dump
    assert_equal "process_name", sys_conf.process_name
    assert_equal 2, sys_conf.log_level
    assert_equal TMP_ROOT_DIR, sys_conf.root_dir
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
  end

  def test_main_process_signal_handlers
    omit "Windows cannot handle signals" if Fluent.windows?

    create_info_dummy_logger

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
    sv.send(:install_main_process_signal_handlers)

    begin
      Process.kill :USR1, $$
    rescue
    end

    sleep 1

    info_msg = '[info]: force flushing buffered events' + "\n"
    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset if $log && $log.out && $log.out.respond_to?(:reset)
  end

  def test_main_process_command_handlers
    omit "Only for Windows, alternative to UNIX signals" unless Fluent.windows?

    create_info_dummy_logger

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
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

    info_msg = '[info]: force flushing buffered events' + "\n"
    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset if $log && $log.out && $log.out.respond_to?(:reset)
  end

  def test_supervisor_signal_handler
    omit "Windows cannot handle signals" if Fluent.windows?

    create_debug_dummy_logger

    server = DummyServer.new
    server.install_supervisor_signal_handlers
    begin
      Process.kill :USR1, $$
    rescue
    end

    sleep 1

    debug_msg = '[debug]: fluentd supervisor process get SIGUSR1'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log && $log.out && $log.out.respond_to?(:reset)
  end

  def test_windows_shutdown_event
    omit "Only for Windows platform" unless Fluent.windows?

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
    ensure
      server.stop_windows_event_thread
    end

    debug_msg = '[debug]: Got Win32 event "TestFluentdEvent"'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log && $log.out && $log.out.respond_to?(:reset)
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
    ensure
      server.stop_windows_event_thread
    end

    debug_msg = '[debug]: Got Win32 event "TestFluentdEvent_USR1"'
    logs = $log.out.logs
    assert{ logs.any?{|log| log.include?(debug_msg) } }
  ensure
    $log.out.reset if $log && $log.out && $log.out.respond_to?(:reset)
  end

  data(:ipv4 => ["0.0.0.0", "127.0.0.1", false],
       :ipv6 => ["[::]", "[::1]", true],
       :localhost_ipv4 => ["localhost", "127.0.0.1", false])
  def test_rpc_server(data)
    omit "Windows cannot handle signals" if Fluent.windows?

    bindaddr, localhost, ipv6 = data
    omit "IPv6 is not supported on this environment" if ipv6 && !ipv6_enabled?

    create_info_dummy_logger

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
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
    info_msg = '[info]: force flushing buffered events' + "\n"

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

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
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

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
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

  def test_load_config
    tmp_dir = "#{TMP_DIR}/dir/test_load_config.conf"
    conf_info_str = %[
<system>
  log_level info
</system>
]
    conf_debug_str = %[
<system>
  log_level debug
</system>
]
    now = Time.now
    Timecop.freeze(now)

    write_config tmp_dir, conf_info_str

    params = {}
    params['workers'] = 1
    params['use_v1_config'] = true
    params['log_path'] = 'test/tmp/supervisor/log'
    params['suppress_repeated_stacktrace'] = true
    params['log_level'] = Fluent::Log::LEVEL_INFO
    params['conf_encoding'] = 'utf-8'
    load_config_proc =  Proc.new { Fluent::Supervisor.load_config(tmp_dir, params) }

    # first call
    se_config = load_config_proc.call
    assert_equal Fluent::Log::LEVEL_INFO, se_config[:log_level]
    assert_equal true, se_config[:suppress_repeated_stacktrace]
    assert_equal 'spawn', se_config[:worker_type]
    assert_equal 1, se_config[:workers]
    assert_equal false, se_config[:log_stdin]
    assert_equal false, se_config[:log_stdout]
    assert_equal false, se_config[:log_stderr]
    assert_equal true, se_config[:enable_heartbeat]
    assert_equal false, se_config[:auto_heartbeat]
    assert_equal false, se_config[:daemonize]
    assert_nil se_config[:pid_path]

    # second call immediately(reuse config)
    se_config = load_config_proc.call
    pre_config_mtime = se_config[:windows_daemon_cmdline][5]['pre_config_mtime']
    pre_loadtime = se_config[:windows_daemon_cmdline][5]['pre_loadtime']
    assert_nil pre_config_mtime
    assert_nil pre_loadtime

    Timecop.freeze(now + 5)

    # third call after 5 seconds(don't reuse config)
    se_config = load_config_proc.call
    pre_config_mtime = se_config[:windows_daemon_cmdline][5]['pre_config_mtime']
    pre_loadtime = se_config[:windows_daemon_cmdline][5]['pre_loadtime']
    assert_not_nil pre_config_mtime
    assert_not_nil pre_loadtime

    # forth call immediately(reuse config)
    se_config = load_config_proc.call
    # test that pre_config_mtime and pre_loadtime are not changed from previous one because reused pre_config
    assert_equal pre_config_mtime, se_config[:windows_daemon_cmdline][5]['pre_config_mtime']
    assert_equal pre_loadtime, se_config[:windows_daemon_cmdline][5]['pre_loadtime']

    write_config tmp_dir, conf_debug_str

    # fifth call after changed conf file(don't reuse config)
    se_config = load_config_proc.call
    assert_equal Fluent::Log::LEVEL_INFO, se_config[:log_level]
  ensure
    Timecop.return
  end

  def test_load_config_for_logger
    tmp_dir = "#{TMP_DIR}/dir/test_load_config_log.conf"
    conf_info_str = %[
<system>
  <log>
    format json
    time_format %FT%T.%L%z
  </log>
</system>
]
    write_config tmp_dir, conf_info_str
    params = {
      'use_v1_config' => true,
      'conf_encoding' => 'utf8',
      'log_level' => Fluent::Log::LEVEL_INFO,
      'log_path' => 'test/tmp/supervisor/log',

      'workers' => 1,
      'log_format' => :json,
      'log_time_format' => '%FT%T.%L%z',
    }

    r = Fluent::Supervisor.load_config(tmp_dir, params)
    assert_equal :json, r[:logger].format
    assert_equal '%FT%T.%L%z', r[:logger].time_format
  end

  def test_load_config_for_daemonize
    tmp_dir = "#{TMP_DIR}/dir/test_load_config.conf"
    conf_info_str = %[
<system>
  log_level info
</system>
]
    conf_debug_str = %[
<system>
  log_level debug
</system>
]

    now = Time.now
    Timecop.freeze(now)

    write_config tmp_dir, conf_info_str

    params = {}
    params['workers'] = 1
    params['use_v1_config'] = true
    params['log_path'] = 'test/tmp/supervisor/log'
    params['suppress_repeated_stacktrace'] = true
    params['log_level'] = Fluent::Log::LEVEL_INFO
    params['daemonize'] = './fluentd.pid'
    params['conf_encoding'] = 'utf-8'
    load_config_proc = Proc.new { Fluent::Supervisor.load_config(tmp_dir, params) }

    # first call
    se_config = load_config_proc.call
    assert_equal Fluent::Log::LEVEL_INFO, se_config[:log_level]
    assert_equal true, se_config[:suppress_repeated_stacktrace]
    assert_equal 'spawn', se_config[:worker_type]
    assert_equal 1, se_config[:workers]
    assert_equal false, se_config[:log_stdin]
    assert_equal false, se_config[:log_stdout]
    assert_equal false, se_config[:log_stderr]
    assert_equal true, se_config[:enable_heartbeat]
    assert_equal false, se_config[:auto_heartbeat]
    assert_equal true, se_config[:daemonize]
    assert_equal './fluentd.pid', se_config[:pid_path]

    # second call immediately(reuse config)
    se_config = load_config_proc.call
    pre_config_mtime = se_config[:windows_daemon_cmdline][5]['pre_config_mtime']
    pre_loadtime = se_config[:windows_daemon_cmdline][5]['pre_loadtime']
    assert_nil pre_config_mtime
    assert_nil pre_loadtime

    Timecop.freeze(now + 5)

    # third call after 6 seconds(don't reuse config)
    se_config = load_config_proc.call
    pre_config_mtime = se_config[:windows_daemon_cmdline][5]['pre_config_mtime']
    pre_loadtime = se_config[:windows_daemon_cmdline][5]['pre_loadtime']
    assert_not_nil pre_config_mtime
    assert_not_nil pre_loadtime

    # forth call immediately(reuse config)
    se_config = load_config_proc.call
    # test that pre_config_mtime and pre_loadtime are not changed from previous one because reused pre_config
    assert_equal pre_config_mtime, se_config[:windows_daemon_cmdline][5]['pre_config_mtime']
    assert_equal pre_loadtime, se_config[:windows_daemon_cmdline][5]['pre_loadtime']

    write_config tmp_dir, conf_debug_str

    # fifth call after changed conf file(don't reuse config)
    se_config = load_config_proc.call
    assert_equal Fluent::Log::LEVEL_INFO, se_config[:log_level]
  ensure
    Timecop.return
  end

  def test_logger
    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
    log = sv.instance_variable_get(:@log)
    log.init(:standalone, 0)
    logger = $log.instance_variable_get(:@logger)

    assert_equal Fluent::Log::LEVEL_INFO, $log.level

    # test that DamonLogger#level= overwrites Fluent.log#level
    logger.level = 'debug'
    assert_equal Fluent::Log::LEVEL_DEBUG, $log.level

    assert_equal 5, logger.instance_variable_get(:@rotate_age)
    assert_equal 1048576, logger.instance_variable_get(:@rotate_size)
  end

  data(
    daily_age: 'daily',
    weekly_age: 'weekly',
    monthly_age: 'monthly',
    integer_age: 2,
  )
  def test_logger_with_rotate_age_and_rotate_size(rotate_age)
    opts = Fluent::Supervisor.default_options.merge(
      log_path: "#{TMP_DIR}/test", log_rotate_age: rotate_age, log_rotate_size: 10
    )
    sv = Fluent::Supervisor.new(opts)
    log = sv.instance_variable_get(:@log)
    log.init(:standalone, 0)

    assert_equal Fluent::LogDeviceIO, $log.out.class
    assert_equal rotate_age, $log.out.instance_variable_get(:@shift_age)
    assert_equal 10, $log.out.instance_variable_get(:@shift_size)
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
        opts = Fluent::Supervisor.default_options.merge(
          log_path: "#{TMP_DIR}/test.log", config_path: file.path
        )
        sv = Fluent::Supervisor.new(opts)

        log = sv.instance_variable_get(:@log)
        log.init(:standalone, 0)
        logger = $log.instance_variable_get(:@logger)

        assert_equal([3, 300],
                     [logger.instance_variable_get(:@rotate_age),
                      logger.instance_variable_get(:@rotate_size)])
      end
    end
  end

  def test_inline_config
    omit 'this feature is deprecated. see https://github.com/fluent/fluentd/issues/2711'

    opts = Fluent::Supervisor.default_options
    opts[:inline_config] = '-'
    sv = Fluent::Supervisor.new(opts)
    assert_equal '-', sv.instance_variable_get(:@inline_config)

    inline_config = '<match *>\n@type stdout\n</match>'
    stub(STDIN).read { inline_config }
    stub(Fluent::Config).build                                # to skip
    stub(sv).build_system_config { Fluent::SystemConfig.new } # to skip

    sv.configure
    assert_equal inline_config, sv.instance_variable_get(:@inline_config)
  end

  def test_log_level_affects
    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)

    c = Fluent::Config::Element.new('system', '', { 'log_level' => 'error' }, [])
    stub(Fluent::Config).build { config_element('ROOT', '', {}, [c]) }

    sv.configure
    assert_equal Fluent::Log::LEVEL_ERROR, $log.level
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
