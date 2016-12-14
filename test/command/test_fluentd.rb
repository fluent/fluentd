require_relative '../helper'

# require 'fluent/command/fluentd'
# don't require it... it runs immediately

require 'fileutils'
require 'timeout'

class TestFluentdCommand < ::Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/command/fluentd#{ENV['TEST_ENV_NUMBER']}")
  SUPERVISOR_PID_PATTERN = /starting fluentd-[.0-9]+ pid=(\d+)/
  WORKER_PID_PATTERN = /starting fluentd worker pid=(\d+) /

  setup do
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
    @supervisor_pid = nil
    @worker_pids = []
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

  def create_conf_file(name, content)
    conf_path = File.join(TMP_DIR, name)
    File.open(conf_path, 'w') do |file|
      file.write content
    end
    conf_path
  end

  def create_plugin_file(name, content)
    file_path = File.join(TMP_DIR, 'plugin', name)
    FileUtils.mkdir_p(File.dirname(file_path))
    File.open(file_path, 'w') do |file|
      file.write content
    end
    file_path
  end

  def create_cmdline(conf_path, *fluentd_options)
    cmd_path = File.expand_path(File.dirname(__FILE__) + "../../../bin/fluentd")
    ["bundle", "exec", "ruby", cmd_path, "-c", conf_path, *fluentd_options]
  end

  def execute_command(cmdline, chdir=TMP_DIR)
    null_stream = File.open(File::NULL, 'w')
    gemfile_path = File.expand_path(File.dirname(__FILE__) + "../../../Gemfile")

    env = {
      "BUNDLE_GEMFILE" => gemfile_path,
    }
    cmdname = cmdline.shift
    arg0 = "testing-fluentd"
    # p(here: "executing process", env: env, cmdname: cmdname, arg0: arg0, args: cmdline)
    IO.popen(env, [[cmdname, arg0], *cmdline], chdir: chdir, err: [:child, :out]) do |io|
      pid = io.pid
      begin
        yield pid, io
        # p(here: "execute command", pid: pid, worker_pids: @worker_pids)
      ensure
        Process.kill(:KILL, pid) rescue nil
        if @supervisor_pid
          Process.kill(:KILL, @supervisor_pid) rescue nil
        end
        @worker_pids.each do |cpid|
          Process.kill(:KILL, cpid) rescue nil
        end
        # p(here: "execute command", pid: pid, exist: process_exist?(pid), worker_pids: @worker_pids, exists: @worker_pids.map{|i| process_exist?(i) })
        Timeout.timeout(10){ sleep 0.1 while process_exist?(pid) }
      end
    end
  ensure
    null_stream.close rescue nil
  end

  def assert_log_matches(cmdline, *pattern_list, timeout: 10)
    matched = false
    assert_error_msg = "matched correctly"
    stdio_buf = ""
    begin
      execute_command(cmdline) do |pid, stdout|
        begin
          waiting(timeout) do
            while process_exist?(pid) && !matched
              readables, _, _ = IO.select([stdout], nil, nil, 1)
              next unless readables
              break if readables.first.eof?

              buf = readables.first.readpartial(1024)
              # puts buf
              stdio_buf << buf
              lines = stdio_buf.split("\n")
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
    rescue => e
      assert_error_msg = "unexpected error in launching fluentd: #{e.inspect}\n" + stdio_buf
    end
    assert matched, assert_error_msg
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

              stdio_buf << readables.first.readpartial(1024)
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

      assert_log_matches(create_cmdline(conf_path), "fluentd worker is now running")
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
        "error_class=SyntaxError",
        "in_buggy.rb:5: syntax error, unexpected end-of-input, expecting keyword_end",
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
end
