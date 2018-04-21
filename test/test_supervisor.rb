require_relative 'helper'
require 'fluent/event_router'
require 'fluent/system_config'
require 'fluent/supervisor'
require_relative 'test_plugin_classes'

require 'net/http'
require 'uri'
require 'fileutils'

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

  def test_initialize
    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
    opts.each { |k, v|
      assert_equal v, sv.instance_variable_get("@#{k}")
    }
  end

  def test_read_config
    create_info_dummy_logger

    tmp_dir = "#{TMP_DIR}/dir/test_read_config.conf"
    conf_str = %[
<source>
  @type forward
  @id forward_input
</source>
<match debug.**>
  @type stdout
  @id stdout_output
</match>
]
    write_config tmp_dir, conf_str
    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)

    use_v1_config = {}
    use_v1_config['use_v1_config'] = true

    sv.instance_variable_set(:@config_path, tmp_dir)
    sv.instance_variable_set(:@use_v1_config, use_v1_config)
    sv.send(:read_config)

    conf = sv.instance_variable_get(:@conf)

    elem = conf.elements.find { |e| e.name == 'source' }
    assert_equal "forward", elem['@type']
    assert_equal "forward_input", elem['@id']

    elem = conf.elements.find { |e| e.name == 'match' }
    assert_equal "debug.**", elem.arg
    assert_equal "stdout", elem['@type']
    assert_equal "stdout_output", elem['@id']

    $log.out.reset
  end

  def test_read_config_with_multibyte_string
    tmp_path = "#{TMP_DIR}/dir/test_multibyte_config.conf"
    conf_str = %[
<source>
  @type forward
  @id forward_input
  @label @INPUT
</source>
<label @INPUT>
  <filter>
    @type record_transformer
    <record>
      message こんにちは. ${record["name"]} has made a order of ${record["item"]} just now.
    </record>
  </filter>
  <match>
    @type stdout
  </match>
</label>
]
    FileUtils.mkdir_p(File.dirname(tmp_path))
    File.open(tmp_path, "w:utf-8") {|file| file.write(conf_str) }

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)

    use_v1_config = {}
    use_v1_config['use_v1_config'] = true

    sv.instance_variable_set(:@config_path, tmp_path)
    sv.instance_variable_set(:@use_v1_config, use_v1_config)
    sv.send(:read_config)

    conf = sv.instance_variable_get(:@conf)
    label = conf.elements.detect {|e| e.name == "label" }
    filter = label.elements.detect {|e| e.name == "filter" }
    record_transformer = filter.elements.detect {|e| e.name = "record_transformer" }
    assert_equal(Encoding::UTF_8, record_transformer["message"].encoding)
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
    sv.instance_variable_set(:@conf, conf)
    sv.send(:set_system_config)
    sys_conf = sv.instance_variable_get(:@system_config)

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

  def test_rpc_server
    omit "Windows cannot handle signals" if Fluent.windows?

    create_info_dummy_logger

    opts = Fluent::Supervisor.default_options
    sv = Fluent::Supervisor.new(opts)
    conf_data = <<-EOC
  <system>
    rpc_endpoint 0.0.0.0:24447
  </system>
    EOC
    conf = Fluent::Config.parse(conf_data, "(test)", "(test_dir)", true)
    sv.instance_variable_set(:@conf, conf)
    sv.send(:set_system_config)
    sys_conf = sv.instance_variable_get(:@system_config)

    server = DummyServer.new
    server.rpc_endpoint = sys_conf.rpc_endpoint
    server.enable_get_dump = sys_conf.enable_get_dump

    server.run_rpc_server

    sv.send(:install_main_process_signal_handlers)
    Net::HTTP.get URI.parse('http://0.0.0.0:24447/api/plugins.flushBuffers')
    info_msg = '[info]: force flushing buffered events' + "\n"

    server.stop_rpc_server

    # In TravisCI with OSX(Xcode), it seems that can't use rpc server.
    # This test will be passed in such environment.
    pend unless $log.out.logs.first

    assert{ $log.out.logs.first.end_with?(info_msg) }
  ensure
    $log.out.reset
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
    write_config tmp_dir, conf_info_str

    params = {}
    params['workers'] = 1
    params['use_v1_config'] = true
    params['log_path'] = 'test/tmp/supervisor/log'
    params['suppress_repeated_stacktrace'] = true
    params['log_level'] = Fluent::Log::LEVEL_INFO
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

    sleep 5

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
    write_config tmp_dir, conf_info_str

    params = {}
    params['workers'] = 1
    params['use_v1_config'] = true
    params['log_path'] = 'test/tmp/supervisor/log'
    params['suppress_repeated_stacktrace'] = true
    params['log_level'] = Fluent::Log::LEVEL_INFO
    params['daemonize'] = './fluentd.pid'
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

    sleep 5

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
  end

  def test_load_config_with_multibyte_string
    tmp_path = "#{TMP_DIR}/dir/test_multibyte_config.conf"
    conf_str = %[
<source>
  @type forward
  @id forward_input
  @label @INPUT
</source>
<label @INPUT>
  <filter>
    @type record_transformer
    <record>
      message こんにちは. ${record["name"]} has made a order of ${record["item"]} just now.
    </record>
  </filter>
  <match>
    @type stdout
  </match>
</label>
]
    FileUtils.mkdir_p(File.dirname(tmp_path))
    File.open(tmp_path, "w:utf-8") {|file| file.write(conf_str) }

    params = {}
    params['workers'] = 1
    params['use_v1_config'] = true
    params['log_path'] = 'test/tmp/supervisor/log'
    params['suppress_repeated_stacktrace'] = true
    params['log_level'] = Fluent::Log::LEVEL_INFO
    load_config_proc =  Proc.new { Fluent::Supervisor.load_config(tmp_path, params) }

    se_config = load_config_proc.call
    conf = se_config[:fluentd_conf]
    label = conf.elements.detect {|e| e.name == "label" }
    filter = label.elements.detect {|e| e.name == "filter" }
    record_transformer = filter.elements.detect {|e| e.name = "record_transformer" }
    assert_equal(Encoding::UTF_8, record_transformer["message"].encoding)
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
