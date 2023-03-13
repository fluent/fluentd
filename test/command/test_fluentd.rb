require_relative '../helper'

# require 'fluent/command/fluentd'
# don't require it... it runs immediately

require 'fileutils'
require 'timeout'
require 'securerandom'
require 'fluent/file_wrapper'

class TestFluentdCommand < ::Test::Unit::TestCase
  SUPERVISOR_PID_PATTERN = /starting fluentd-[.0-9]+ pid=(\d+)/
  WORKER_PID_PATTERN = /starting fluentd worker pid=(\d+) /

  def tmp_dir
    File.join(File.dirname(__FILE__), "..", "tmp", "command" "fluentd#{ENV['TEST_ENV_NUMBER']}", SecureRandom.hex(10))
  end

  setup do
    @tmp_dir = tmp_dir
    FileUtils.mkdir_p(@tmp_dir)
    @supervisor_pid = nil
    @worker_pids = []
    ENV["TEST_RUBY_PATH"] = nil
  end

  teardown do
    begin
      FileUtils.rm_rf(@tmp_dir)
    rescue Errno::EACCES
      # It may occur on Windows because of delete pending state due to delayed GC.
      # Ruby 3.2 or later doesn't ignore Errno::EACCES:
      # https://github.com/ruby/ruby/commit/983115cf3c8f75b1afbe3274f02c1529e1ce3a81
    end
  end

  def process_exist?(pid)
    begin
      r = Process.waitpid(pid, Process::WNOHANG)
      return true if r.nil?
      false
    rescue SystemCallError
      false
    end
  end

  def create_conf_file(name, content, ext_enc = 'utf-8')
    conf_path = File.join(@tmp_dir, name)
    Fluent::FileWrapper.open(conf_path, "w:#{ext_enc}:utf-8") do |file|
      file.write content
    end
    conf_path
  end

  def create_plugin_file(name, content)
    file_path = File.join(@tmp_dir, 'plugin', name)
    FileUtils.mkdir_p(File.dirname(file_path))
    Fluent::FileWrapper.open(file_path, 'w') do |file|
      file.write content
    end
    file_path
  end

  def create_cmdline(conf_path, *fluentd_options)
    if Fluent.windows?
      cmd_path = File.expand_path(File.dirname(__FILE__) + "../../../bin/fluentd")
      ["bundle", "exec", ServerEngine.ruby_bin_path, cmd_path, "-c", conf_path, *fluentd_options]
    else
      cmd_path = File.expand_path(File.dirname(__FILE__) + "../../../bin/fluentd")
      ["bundle", "exec", cmd_path, "-c", conf_path, *fluentd_options]
    end
  end

  def process_kill(pid)
    if Fluent.windows?
      Process.kill(:KILL, pid) rescue nil
      return
    end

    begin
      Process.kill(:TERM, pid) rescue nil
      Timeout.timeout(10){ sleep 0.1 while process_exist?(pid) }
    rescue Timeout::Error
      Process.kill(:KILL, pid) rescue nil
    end
  end

  def execute_command(cmdline, chdir=@tmp_dir, env = {})
    null_stream = Fluent::FileWrapper.open(File::NULL, 'w')
    gemfile_path = File.expand_path(File.dirname(__FILE__) + "../../../Gemfile")

    env = { "BUNDLE_GEMFILE" => gemfile_path }.merge(env)
    cmdname = cmdline.shift
    arg0 = "testing-fluentd"
    # p(here: "executing process", env: env, cmdname: cmdname, arg0: arg0, args: cmdline)
    IO.popen(env, [[cmdname, arg0], *cmdline], chdir: chdir, err: [:child, :out]) do |io|
      pid = io.pid
      begin
        yield pid, io
        # p(here: "execute command", pid: pid, worker_pids: @worker_pids)
      ensure
        process_kill(pid)
        if @supervisor_pid
          process_kill(@supervisor_pid)
        end
        @worker_pids.each do |cpid|
          process_kill(cpid)
        end
        # p(here: "execute command", pid: pid, exist: process_exist?(pid), worker_pids: @worker_pids, exists: @worker_pids.map{|i| process_exist?(i) })
        Timeout.timeout(10){ sleep 0.1 while process_exist?(pid) }
      end
    end
  ensure
    null_stream.close rescue nil
  end

  def eager_read(io)
    buf = +''

    loop do
      b = io.read_nonblock(1024, nil, exception: false)
      if b == :wait_readable || b.nil?
        return buf
      end
      buf << b
    end
  end

  def assert_log_matches(cmdline, *pattern_list, patterns_not_match: [], timeout: 10, env: {})
    matched = false
    matched_wrongly = false
    assert_error_msg = ""
    stdio_buf = ""
    begin
      execute_command(cmdline, @tmp_dir, env) do |pid, stdout|
        begin
          waiting(timeout) do
            while process_exist?(pid)
              readables, _, _ = IO.select([stdout], nil, nil, 1)
              next unless readables
              break if readables.first.eof?

              buf = eager_read(readables.first)
              # puts buf
              stdio_buf << buf
              lines = stdio_buf.split("\n")
              if pattern_list.all?{|ptn| lines.any?{|line| ptn.is_a?(Regexp) ? ptn.match(line) : line.include?(ptn) } }
                matched = true
              end

              if Fluent.windows?
                # https://github.com/fluent/fluentd/issues/4095
                # On Windows, the initial process is different from the supervisor process,
                # so we need to wait until `SUPERVISOR_PID_PATTERN` appears in the logs to get the pid.
                # (Worker processes will be killed by the supervisor process, so we don't need it-)
                break if matched && SUPERVISOR_PID_PATTERN =~ stdio_buf
              else
                # On Non-Windows, the initial process is the supervisor process,
                # so we don't need to wait `SUPERVISOR_PID_PATTERN`.
                break if matched
              end
            end
          end
        ensure
          if SUPERVISOR_PID_PATTERN =~ stdio_buf
            @supervisor_pid = $1.to_i
          end
          stdio_buf.scan(WORKER_PID_PATTERN) do |worker_pid|
            @worker_pids << worker_pid.first.to_i
          end
        end
      end
    rescue Timeout::Error
      assert_error_msg = "execution timeout"
      # https://github.com/fluent/fluentd/issues/4095
      # On Windows, timeout without `@supervisor_pid` means that the test is invalid,
      # since the supervisor process will survive without being killed correctly.
      flunk("Invalid test: The pid of supervisor could not be taken, which is necessary on Windows.") if Fluent.windows? && @supervisor_pid.nil?
    rescue => e
      assert_error_msg = "unexpected error in launching fluentd: #{e.inspect}"
    else
      assert_error_msg = "log doesn't match" unless matched
    end

    if patterns_not_match.empty?
      assert_error_msg = build_message(assert_error_msg,
                                       "<?>\nwas expected to include:\n<?>",
                                       stdio_buf, pattern_list)
    else
      lines = stdio_buf.split("\n")
      patterns_not_match.each do |ptn|
        matched_wrongly = if ptn.is_a? Regexp
                            lines.any?{|line| ptn.match(line) }
                          else
                            lines.any?{|line| line.include?(ptn) }
                          end
        if matched_wrongly
          assert_error_msg << "\n" unless assert_error_msg.empty?
          assert_error_msg << "pattern exists in logs wrongly: #{ptn}"
        end
      end
      assert_error_msg = build_message(assert_error_msg,
                                       "<?>\nwas expected to include:\n<?>\nand not include:\n<?>",
                                       stdio_buf, pattern_list, patterns_not_match)
    end

    assert matched && !matched_wrongly, assert_error_msg
  end

  def assert_fluentd_fails_to_start(cmdline, *pattern_list, timeout: 10)
    # empty_list.all?{ ... } is always true
    matched = false
    running = false
    assert_error_msg = "failed to start correctly"
    stdio_buf = ""
    begin
      execute_command(cmdline) do |pid, stdout|
        begin
          waiting(timeout) do
            while process_exist?(pid) && !running
              readables, _, _ = IO.select([stdout], nil, nil, 1)
              next unless readables
              next if readables.first.eof?

              stdio_buf << eager_read(readables.first)
              lines = stdio_buf.split("\n")
              if lines.any?{|line| line.include?("fluentd worker is now running") }
                running = true
              end
              if pattern_list.all?{|ptn| lines.any?{|line| ptn.is_a?(Regexp) ? ptn.match(line) : line.include?(ptn) } }
                matched = true
              end
            end
          end
        ensure
          if SUPERVISOR_PID_PATTERN =~ stdio_buf
            @supervisor_pid = $1.to_i
          end
          stdio_buf.scan(WORKER_PID_PATTERN) do |worker_pid|
            @worker_pids << worker_pid.first.to_i
          end
        end
      end
    rescue Timeout::Error
      assert_error_msg = "execution timeout with command out:\n" + stdio_buf
      # https://github.com/fluent/fluentd/issues/4095
      # On Windows, timeout without `@supervisor_pid` means that the test is invalid,
      # since the supervisor process will survive without being killed correctly.
      flunk("Invalid test: The pid of supervisor could not be taken, which is necessary on Windows.") if Fluent.windows? && @supervisor_pid.nil?
    rescue => e
      assert_error_msg = "unexpected error in launching fluentd: #{e.inspect}\n" + stdio_buf
      assert false, assert_error_msg
    end
    assert !running, "fluentd started to run incorrectly:\n" + stdio_buf
    unless matched
      assert_error_msg = "fluentd failed to start, without specified regular expressions:\n" + stdio_buf
    end
    assert matched, assert_error_msg
  end

  sub_test_case 'with valid configuration' do
    test 'runs successfully' do
      conf = <<CONF
<source>
  @type dummy
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('valid.conf', conf)
      assert File.exist?(conf_path)

      assert_log_matches(create_cmdline(conf_path), "fluentd worker is now running", 'worker=0')
    end
  end

  sub_test_case 'with --conf-encoding' do
    test 'runs successfully' do
      conf = <<CONF
# テスト
<source>
  @type dummy
  tag dummy
  dummy {"message": "yay!"}
</source>
<match dummy>
  @type null
</match>
CONF
      conf_path = create_conf_file('shift_jis.conf', conf, 'shift_jis')
      assert_log_matches(create_cmdline(conf_path, '--conf-encoding', 'shift_jis'), "fluentd worker is now running", 'worker=0')
    end

    test 'failed to run by invalid encoding' do
      conf = <<CONF
# テスト
<source>
  @type dummy
  tag dummy
  dummy {"message": "yay!"}
</source>
<match dummy>
  @type null
</match>
CONF
      conf_path = create_conf_file('shift_jis.conf', conf, 'shift_jis')
      assert_fluentd_fails_to_start(create_cmdline(conf_path), "invalid byte sequence in UTF-8")
    end
  end

  sub_test_case 'with system configuration about root directory' do
    setup do
      @root_path = File.join(@tmp_dir, "rootpath")
      FileUtils.rm_rf(@root_path)
      @conf = <<CONF
<system>
  root_dir #{@root_path}
</system>
<source>
  @type dummy
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
    end

    test 'use the specified existing directory as root' do
      FileUtils.mkdir_p(@root_path)
      conf_path = create_conf_file('existing_root_dir.conf', @conf)
      assert Dir.exist?(@root_path)

      assert_log_matches(create_cmdline(conf_path), "fluentd worker is now running", 'worker=0')
    end

    test 'creates the specified root directory if missing' do
      conf_path = create_conf_file('missing_root_dir.conf', @conf)
      assert_false Dir.exist?(@root_path)

      assert_log_matches(create_cmdline(conf_path), "fluentd worker is now running", 'worker=0')
      assert Dir.exist?(@root_path)
    end

    test 'fails to launch fluentd if specified root path is invalid path for directory' do
      Fluent::FileWrapper.open(@root_path, 'w') do |_|
        # create file and close it
      end
      conf_path = create_conf_file('existing_root_dir.conf', @conf)

      assert_fluentd_fails_to_start(
        create_cmdline(conf_path),
        "non directory entry exists:#{@root_path}",
      )
    end
  end

  sub_test_case 'configured to route log events to plugins' do
    setup do
      @basic_conf = <<CONF
<source>
  @type dummy
  @id dummy
  tag dummy
  dummy {"message": "yay!"}
</source>
<match dummy>
  @type null
  @id   blackhole
</match>
CONF
    end

    test 'by top level <match fluent.*> section' do
      conf = @basic_conf + <<CONF
<match fluent.**>
  @type stdout
</match>
CONF
      conf_path = create_conf_file('logevent_1.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        "fluentd worker is now running",
        'fluent.info: {"worker":0,"message":"fluentd worker is now running worker=0"}',
        "define <match fluent.**> to capture fluentd logs in top level is deprecated. Use <label @FLUENT_LOG> instead",
        patterns_not_match: ['[warn]: some tags for log events are not defined in top level (to be ignored) tags=["fluent.trace", "fluent.debug"]'],
      )
    end

    test 'by top level <match> section with warning for missing log levels (and warnings for each log event records)' do
      conf = @basic_conf + <<CONF
<match fluent.warn fluent.error fluent.fatal>
  @type stdout
</match>
CONF
      conf_path = create_conf_file('logevent_2.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        "fluentd worker is now running",
        '[warn]: #0 match for some tags of log events are not defined in top level (to be ignored) tags=["fluent.trace", "fluent.debug", "fluent.info"]',
        "define <match fluent.warn>, <match fluent.error>, <match fluent.fatal> to capture fluentd logs in top level is deprecated. Use <label @FLUENT_LOG> instead",
        '[warn]: #0 no patterns matched tag="fluent.info"',
      )
    end

    test 'by <label @FLUENT_LOG> section' do
      conf = @basic_conf + <<CONF
<label @FLUENT_LOG>
  <match **>
    @type stdout
  </match>
</label>
CONF
      conf_path = create_conf_file('logevent_3.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        "fluentd worker is now running",
        'fluent.info: {"worker":0,"message":"fluentd worker is now running worker=0"}',
        patterns_not_match: ['[warn]: some tags for log events are not defined in @FLUENT_LOG label (to be ignored)'],
      )
    end

    test 'by <label> section with warning for missing log levels' do
      conf = @basic_conf + <<CONF
<label @FLUENT_LOG>
  <match fluent.{trace,debug}>
    @type null
  </match>
  <match fluent.warn fluent.error>
    @type stdout
  </match>
</label>
CONF
      conf_path = create_conf_file('logevent_4.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        "fluentd worker is now running",
        '[warn]: #0 match for some tags of log events are not defined in @FLUENT_LOG label (to be ignored) tags=["fluent.info", "fluent.fatal"]',
        patterns_not_match: ['[warn]: no patterns matched tag="fluent.info"'],
      )
    end
  end

  sub_test_case 'configured to suppress configuration dump' do
    setup do
      @basic_conf = <<CONF
<source>
  @type dummy
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
    end

    test 'configured by system config' do
      conf = <<SYSTEM + @basic_conf
<system>
  suppress_config_dump
</system>
SYSTEM
      conf_path = create_conf_file('suppress_conf_dump_1.conf', conf)
      assert_log_matches(create_cmdline(conf_path), "fluentd worker is now running", patterns_not_match: ["tag dummy"])
    end

    test 'configured by command line option' do
      conf_path = create_conf_file('suppress_conf_dump_2.conf', @basic_conf)
      assert_log_matches(create_cmdline(conf_path, '--suppress-config-dump'), "fluentd worker is now running", patterns_not_match: ["tag dummy"])
    end

    test 'configured as false by system config, but overridden as true by command line option' do
      conf = <<SYSTEM + @basic_conf
<system>
  suppress_config_dump false
</system>
SYSTEM
      conf_path = create_conf_file('suppress_conf_dump_3.conf', conf)
      assert_log_matches(create_cmdline(conf_path, '--suppress-config-dump'), "fluentd worker is now running", patterns_not_match: ["tag dummy"])
    end
  end

  sub_test_case 'configuration with wrong plugin type' do
    test 'failed to start' do
      conf = <<CONF
<source>
  @type
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('type_missing.conf', conf)
      assert File.exist?(conf_path)

      assert_fluentd_fails_to_start(
        create_cmdline(conf_path),
        "config error",
        "error=\"Unknown input plugin ''. Run 'gem search -rd fluent-plugin' to find plugins",
      )
    end
  end

  sub_test_case 'configuration to load plugin file with syntax error' do
    test 'failed to start' do
      script =  "require 'fluent/plugin/input'\n"
      script << "module Fluent::Plugin\n"
      script << "  class BuggyInput < Input\n"
      script << "    Fluent::Plugin.register_input('buggy', self)\n"
      script << "  end\n"
      plugin_path = create_plugin_file('in_buggy.rb', script)

      conf = <<CONF
<source>
  @type buggy
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('buggy_plugin.conf', conf)
      assert File.exist?(conf_path)

      assert_fluentd_fails_to_start(
        create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
        "in_buggy.rb:5: syntax error, unexpected end-of-input"
      )
    end
  end

  sub_test_case 'configuration to load plugin which raises unrecoverable error in #start' do
    test 'failed to start' do
      script =  "require 'fluent/plugin/input'\n"
      script << "require 'fluent/error'\n"
      script << "module Fluent::Plugin\n"
      script << "  class CrashingInput < Input\n"
      script << "    Fluent::Plugin.register_input('crashing', self)\n"
      script << "    def start\n"
      script << "      raise Fluent::UnrecoverableError"
      script << "    end\n"
      script << "  end\n"
      script << "end\n"
      plugin_path = create_plugin_file('in_crashing.rb', script)

      conf = <<CONF
<source>
  @type crashing
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('crashing_plugin.conf', conf)
      assert File.exist?(conf_path)

      assert_fluentd_fails_to_start(
        create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
        'unexpected error error_class=Fluent::UnrecoverableError error="an unrecoverable error occurs in Fluentd process"',
      )
    end
  end

  sub_test_case 'configured to run 2 workers' do
    setup do
      @root_path = File.join(@tmp_dir, "rootpath")
      FileUtils.rm_rf(@root_path)
      FileUtils.mkdir_p(@root_path)
    end

    test 'success to start the number of workers specified in configuration' do
      conf = <<'CONF'
<system>
  workers 2
  root_dir #{@root_path}
</system>
<source>
  @type dummy
  @id "dummy#{worker_id}" # check worker_id works or not with actual command
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('workers1.conf', conf)
      assert Dir.exist?(@root_path)

      assert_log_matches(
        create_cmdline(conf_path),
        "#0 fluentd worker is now running worker=0",
        "#1 fluentd worker is now running worker=1"
      )
    end

    sub_test_case "YAML config format" do
      test 'success to start the number of workers specified in configuration' do
        conf = <<'CONF'
        system:
          workers: 2
          root_dir: "#{@root_path}"
        config:
          - source:
              $type: dummy
              $id: !fluent/s "dummy.#{worker_id}" # check worker_id works or not with actual command
              $label: '@dummydata'
              tag: dummy
              dummy: !fluent/json {"message": !fluent/s "yay from #{hostname}!"}

          - label:
              $name: '@dummydata'
              config:
               - match:
                  $tag: dummy
                  $type: "null"
                  $id: blackhole
CONF
        conf_path = create_conf_file('workers1.yaml', conf)
        assert Dir.exist?(@root_path)

        assert_log_matches(
          create_cmdline(conf_path),
          "#0 fluentd worker is now running worker=0",
          "#1 fluentd worker is now running worker=1"
        )
      end
    end

    test 'success to start the number of workers specified by command line option' do
      conf = <<CONF
<system>
  root_dir #{@root_path}
</system>
<source>
  @type dummy
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('workers2.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path, '--workers', '2'),
        "#0 fluentd worker is now running worker=0",
        "#1 fluentd worker is now running worker=1"
      )
    end

    test 'failed to start workers when configured plugins do not support multi worker configuration' do
      script =  "require 'fluent/plugin/input'\n"
      script << "module Fluent::Plugin\n"
      script << "  class SingleInput < Input\n"
      script << "    Fluent::Plugin.register_input('single', self)\n"
      script << "    def multi_workers_ready?\n"
      script << "      false\n"
      script << "    end\n"
      script << "  end\n"
      script << "end\n"
      plugin_path = create_plugin_file('in_single.rb', script)

      conf = <<CONF
<system>
  workers 2
</system>
<source>
  @type single
  @id single
  @label @dummydata
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('workers_invalid1.conf', conf)
      assert_fluentd_fails_to_start(
        create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
        "Plugin 'single' does not support multi workers configuration (Fluent::Plugin::SingleInput)",
      )
    end

    test 'failed to start workers when file buffer is configured in non-workers way' do
      conf = <<CONF
<system>
  workers 2
</system>
<source>
  @type dummy
  tag dummy
  @id single
  @label @dummydata
</source>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
    <buffer>
      @type file
      path #{File.join(@root_path, "buf", "file.*.log")}
    </buffer>
  </match>
</label>
CONF
      conf_path = create_conf_file('workers_invalid2.conf', conf)
      assert_fluentd_fails_to_start(
        create_cmdline(conf_path),
        "[blackhole] file buffer with multi workers should be configured to use directory 'path', or system root_dir and plugin id",
        "config error file=\"#{conf_path}\" error_class=Fluent::ConfigError error=\"Plugin 'file' does not support multi workers configuration (Fluent::Plugin::FileBuffer)\"",
      )
    end

    test 'failed to start workers when configured plugins as children of MultiOutput do not support multi worker configuration' do
      script = <<-EOC
require 'fluent/plugin/output'
module Fluent::Plugin
  class SingleOutput < Output
    Fluent::Plugin.register_output('single', self)
    def multi_workers_ready?
      false
    end
    def write(chunk)
    end
  end
end
EOC
      plugin_path = create_plugin_file('out_single.rb', script)

      conf = <<CONF
<system>
  workers 2
</system>
<source>
  @type dummy
  tag dummy
  @id single
  @label @dummydata
</source>
<label @dummydata>
  <match dummy>
    @type copy
    <store>
      @type single
    </store>
    <store>
      @type single
    </store>
  </match>
</label>
CONF
      conf_path = create_conf_file('workers_invalid3.conf', conf)
      assert_fluentd_fails_to_start(
        create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
        "Plugin 'single' does not support multi workers configuration (Fluent::Plugin::SingleOutput)",
      )
    end

    test 'success to start a worker2 with worker specific configuration' do
      conf = <<CONF
<system>
  root_dir #{@root_path}
  dir_permission 0744
</system>
CONF
      conf_path = create_conf_file('worker_section0.conf', conf)

      FileUtils.rm_rf(@root_path) rescue nil

      assert_path_not_exist(@root_path)
      assert_log_matches(create_cmdline(conf_path), 'spawn command to main') # any message is ok
      assert_path_exist(@root_path)
      if Fluent.windows?
        # In Windows, dir permission is always 755.
        assert_equal '755', File.stat(@root_path).mode.to_s(8)[-3, 3]
      else
        assert_equal '744', File.stat(@root_path).mode.to_s(8)[-3, 3]
      end
    end

    test 'success to start a worker with worker specific configuration' do
      conf = <<CONF
<system>
  workers 2
  root_dir #{@root_path}
</system>
<source>
  @type dummy
  @id dummy
  @label @dummydata
  tag dummy
  dummy {"message": "yay!"}
</source>
<worker 1>
  <source>
    @type dummy
    @id dummy_in_worker
    @label @dummydata
    tag dummy
    dummy {"message": "yay!"}
  </source>
</worker>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('worker_section0.conf', conf)
      assert Dir.exist?(@root_path)

      assert_log_matches(
        create_cmdline(conf_path),
        "#0 fluentd worker is now running worker=0",
        "#1 fluentd worker is now running worker=1",
        /(?!#\d) adding source type="dummy"/,
        '#1 adding source type="dummy"'
      )
    end

    test 'success to start workers when configured plugins only for specific worker do not support multi worker configuration' do
      script =  <<-EOC
require 'fluent/plugin/input'
module Fluent::Plugin
  class SingleInput < Input
    Fluent::Plugin.register_input('single', self)
    def multi_workers_ready?
      false
    end
  end
end
EOC
      plugin_path = create_plugin_file('in_single.rb', script)

      conf = <<CONF
<system>
  workers 2
</system>
<worker 1>
  <source>
    @type single
    @id single
    @label @dummydata
  </source>
</worker>
<label @dummydata>
  <match dummy>
    @type null
    @id   blackhole
  </match>
</label>
CONF
      conf_path = create_conf_file('worker_section1.conf', conf)
      assert Dir.exist?(@root_path)

      assert_log_matches(
        create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
        "#0 fluentd worker is now running worker=0",
        "#1 fluentd worker is now running worker=1",
        '#1 adding source type="single"'
      )
    end

    test "multiple values are set to RUBYOPT" do
      conf = <<CONF
<source>
  @type dummy
  tag dummy
</source>
<match>
  @type null
</match>
CONF
      conf_path = create_conf_file('rubyopt_test.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        '#0 fluentd worker is now running worker=0',
        patterns_not_match: ['(LoadError)'],
        env: { 'RUBYOPT' => '-rtest-unit -rbundler/setup' },
      )
    end

    data(
      '-E' => '-Eutf-8',
      '-encoding' => '--encoding=utf-8',
      '-external-encoding' => '--external-encoding=utf-8',
      '-internal-encoding' => '--internal-encoding=utf-8',
    )
    test "-E option is set to RUBYOPT" do |opt|
      conf = <<CONF
<source>
  @type dummy
  tag dummy
</source>
<match>
  @type null
</match>
CONF
      conf_path = create_conf_file('rubyopt_test.conf', conf)
      opt << " #{ENV['RUBYOPT']}" if ENV['RUBYOPT']
      assert_log_matches(
        create_cmdline(conf_path),
        *opt.split(' '),
        patterns_not_match: ['-Eascii-8bit:ascii-8bit'],
        env: { 'RUBYOPT' => opt },
      )
    end

    test "without RUBYOPT" do
      saved_ruby_opt = ENV["RUBYOPT"]
      ENV["RUBYOPT"] = nil
      conf = <<CONF
<source>
  @type dummy
  tag dummy
</source>
<match>
  @type null
</match>
CONF
      conf_path = create_conf_file('rubyopt_test.conf', conf)
      assert_log_matches(create_cmdline(conf_path), '-Eascii-8bit:ascii-8bit')
    ensure
      ENV["RUBYOPT"] = saved_ruby_opt
    end

    test 'invalid values are set to RUBYOPT' do
      omit "hard to run correctly because RUBYOPT=-r/path/to/bundler/setup is required on Windows while this test set invalid RUBYOPT" if Fluent.windows?
      conf = <<CONF
<source>
  @type dummy
  tag dummy
</source>
<match>
  @type null
</match>
CONF
      conf_path = create_conf_file('rubyopt_invalid_test.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        'Invalid option is passed to RUBYOPT',
        env: { 'RUBYOPT' => 'a' },
      )
    end

    # https://github.com/fluent/fluentd/issues/2915
    test "ruby path contains spaces" do
      saved_ruby_opt = ENV["RUBYOPT"]
      ENV["RUBYOPT"] = nil
      conf = <<CONF
<source>
  @type dummy
  tag dummy
</source>
<match>
  @type null
</match>
CONF
      ruby_path = ServerEngine.ruby_bin_path
      tmp_ruby_path = File.join(@tmp_dir, "ruby with spaces")
      if Fluent.windows?
        tmp_ruby_path << ".bat"
        Fluent::FileWrapper.open(tmp_ruby_path, "w") do |file|
          file.write "#{ruby_path} %*"
        end
      else
        FileUtils.ln_sf(ruby_path, tmp_ruby_path)
      end
      ENV["TEST_RUBY_PATH"] = tmp_ruby_path
      cmd_path = File.expand_path(File.dirname(__FILE__) + "../../../bin/fluentd")
      conf_path = create_conf_file('space_mixed_ruby_path_test.conf', conf)
      args = ["bundle", "exec", tmp_ruby_path, cmd_path, "-c", conf_path]
      assert_log_matches(
        args,
        'spawn command to main:',
        '-Eascii-8bit:ascii-8bit'
      )
    ensure
      ENV["RUBYOPT"] = saved_ruby_opt
    end

    test 'success to start workers when file buffer is configured in non-workers way only for specific worker' do
      conf = <<CONF
<system>
  workers 2
</system>
<source>
  @type dummy
  @id dummy
  tag dummy
  dummy {"message": "yay!"}
</source>
<worker 1>
  <match dummy>
    @type null
    @id   blackhole
    <buffer>
      @type file
      path #{File.join(@root_path, "buf")}
    </buffer>
  </match>
</worker>
CONF
      conf_path = create_conf_file('worker_section2.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path),
        "#0 fluentd worker is now running worker=0",
        "#1 fluentd worker is now running worker=1",
        '#1 adding match pattern="dummy" type="null"'
      )
    end

    test 'success to start workers when configured plugins as a children of MultiOutput only for specific worker do not support multi worker configuration' do
      script = <<-EOC
require 'fluent/plugin/output'
module Fluent::Plugin
  class SingleOutput < Output
    Fluent::Plugin.register_output('single', self)
    def multi_workers_ready?
      false
    end
    def write(chunk)
    end
  end
end
EOC
      plugin_path = create_plugin_file('out_single.rb', script)

      conf = <<CONF
<system>
  workers 2
</system>
<source>
  @type dummy
  @id dummy
  tag dummy
  dummy {"message": "yay!"}
</source>
<worker 1>
  <match dummy>
    @type copy
    <store>
      @type single
    </store>
    <store>
      @type single
    </store>
  </match>
</worker>
CONF
      conf_path = create_conf_file('worker_section3.conf', conf)
      assert_log_matches(
        create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
        "#0 fluentd worker is now running worker=0",
        "#1 fluentd worker is now running worker=1",
        '#1 adding match pattern="dummy" type="copy"'
      )
    end
  end

  sub_test_case 'config dump' do
    test 'all secret parameters in worker section is sealed' do
      script =  <<-EOC
require 'fluent/plugin/input'
module Fluent::Plugin
  class FakeInput < Input
    Fluent::Plugin.register_input('fake', self)
    config_param :secret, :string, secret: true
    def multi_workers_ready?; true; end
  end
end
EOC
      plugin_path = create_plugin_file('in_fake.rb', script)

      conf = <<CONF
<system>
  workers 2
</system>
<worker 0>
  <source>
    @type fake
    secret secret0
  </source>
  <match>
    @type null
  </match>
</worker>
<worker 1>
  <source>
    @type fake
    secret secret1
  </source>
  <match>
    @type null
  </match>
</worker>
CONF
      conf_path = create_conf_file('secret_in_worker.conf', conf)
      assert File.exist?(conf_path)

      assert_log_matches(create_cmdline(conf_path, "-p", File.dirname(plugin_path)),
                         "secret xxxxxx", patterns_not_match: ["secret secret0", "secret secret1"])
    end
  end

  sub_test_case 'sahred socket options' do
    test 'enable shared socket by default' do
      conf = ""
      conf_path = create_conf_file('empty.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path),
                         patterns_not_match: ["shared socket for multiple workers is disabled"])
    end

    test 'disable shared socket by command line option' do
      conf = ""
      conf_path = create_conf_file('empty.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path, "--disable-shared-socket"),
                         "shared socket for multiple workers is disabled",)
    end

    test 'disable shared socket by system config' do
      conf = <<CONF
<system>
  disable_shared_socket
</system>
CONF
      conf_path = create_conf_file('empty.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path, "--disable-shared-socket"),
                         "shared socket for multiple workers is disabled",)
    end
  end

  sub_test_case 'log_level by command line option' do
    test 'info' do
      conf = ""
      conf_path = create_conf_file('empty.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path),
                         "[info]",
                         patterns_not_match: ["[debug]"])
    end

    test 'debug' do
      conf = ""
      conf_path = create_conf_file('empty.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path, "-v"),
                         "[debug]",
                         patterns_not_match: ["[trace]"])
    end

    data("Trace" => "-vv")
    data("Invalid low level should be treated as Trace level": "-vvv")
    test 'trace' do |option|
      conf = <<CONF
<source>
  @type sample
  tag test
</source>
CONF
      conf_path = create_conf_file('sample.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path, option),
                         "[trace]",)
    end

    test 'warn' do
      omit "Can't run on Windows since there is no way to take pid of the supervisor." if Fluent.windows?
      conf = <<CONF
<source>
  @type sample
  tag test
</source>
CONF
      conf_path = create_conf_file('sample.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path, "-q"),
                         "[warn]",
                         patterns_not_match: ["[info]"])
    end

    data("Error" => "-qq")
    data("Fatal should be treated as Error level" => "-qqq")
    data("Invalid high level should be treated as Error level": "-qqqq")
    test 'error' do |option|
      # This test can run on Windows correctly,
      # since the process will stop automatically with an error.
      conf = <<CONF
<source>
  @type plugin_not_found
  tag test
</source>
CONF
      conf_path = create_conf_file('plugin_not_found.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path, option),
                         "[error]",
                         patterns_not_match: ["[warn]"])
    end

    test 'system config one should not be overwritten when cmd line one is not specified' do
      conf = <<CONF
<system>
  log_level debug
</system>
CONF
      conf_path = create_conf_file('debug.conf', conf)
      assert File.exist?(conf_path)
      assert_log_matches(create_cmdline(conf_path),
                         "[debug]")
    end
  end
end
