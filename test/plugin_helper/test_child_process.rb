require_relative '../helper'
require 'fluent/plugin_helper/child_process'
require 'fluent/plugin/base'
require 'timeout'
require 'tempfile'

class ChildProcessTest < Test::Unit::TestCase
  TEST_DEADLOCK_TIMEOUT = 30
  TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING = 0.1 # This may be shorter than ruby's threading timer, but work well
  # @nalsh says that ruby's cpu assignments for threads are almost 200ms or so.
  # Loop interval (expected that it work as specified) should be longer than it.
  TEST_WAIT_INTERVAL_FOR_LOOP = 0.5

  setup do
    @d = Dummy.new
    @d.configure(config_element())
    @d.start
  end

  teardown do
    if @d
      @d.stop      unless @d.stopped?
      @d.shutdown  unless @d.shutdown?
      @d.close     unless @d.closed?
      @d.terminate unless @d.terminated?
      @d.log.reset
    end
  end

  class Dummy < Fluent::Plugin::TestBase
    helpers :child_process
    def configure(conf)
      super
      @_child_process_kill_timeout = 1
    end
  end

  test 'can be instantiated' do
    d1 = Dummy.new
    assert d1.respond_to?(:_child_process_processes)
  end

  test 'can be configured and started' do
    d1 = Dummy.new
    assert_nothing_raised do
      d1.configure(config_element())
    end
    assert d1.plugin_id
    assert d1.log

    d1.start
  end

  test 'can execute external command asyncronously' do
    m = Mutex.new
    m.lock
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t0, 'echo', arguments: ['foo', 'bar'], mode: [:read]) do |io|
        m.lock
        ran = true
        io.read # discard
        ary << 2
        m.unlock
      end
      ary << 1
      m.unlock
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
    end
    assert_equal [1,2], ary
  end

  test 'can execute external command at just once, which finishes immediately' do
    m = Mutex.new
    t1 = Time.now
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t1, 'echo', arguments: ['foo', 'bar'], mode: [:read]) do |io|
        m.lock
        ran = true
        ary << io.read
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
    end
    assert{ Time.now - t1 < 4.0 }
  end

  test 'can execute external command at just once, which can handle both of read and write' do
    m = Mutex.new
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      cmd = "ruby -e 'while !STDIN.eof? && line = STDIN.readline; puts line.chomp; STDOUT.flush rescue nil; end'"
      @d.child_process_execute(:t2, cmd, mode: [:write, :read]) do |writeio, readio|
        m.lock
        ran = true

        [[1,2],[3,4],[5,6]].each do |i,j|
          writeio.write "my data#{i}\n"
          writeio.write "my data#{j}\n"
          writeio.flush
        end
        writeio.close

        while line = readio.readline
          ary << line
        end
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
    end

    assert_equal [], @d.log.out.logs
    expected = (1..6).map{|i| "my data#{i}\n" }
    assert_equal expected, ary
  end

  test 'can execute external command at just once, which can handle all of read, write and stderr' do
    m = Mutex.new
    ary1 = []
    ary2 = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      cmd = "ruby -e 'while !STDIN.eof? && line = STDIN.readline; puts line.chomp; STDOUT.flush rescue nil; STDERR.puts line.chomp; STDERR.flush rescue nil; end'"
      @d.child_process_execute(:t2a, cmd, mode: [:write, :read, :stderr]) do |writeio, readio, stderrio|
        m.lock
        ran = true

        [[1,2],[3,4],[5,6]].each do |i,j|
          writeio.write "my data#{i}\n"
          writeio.write "my data#{j}\n"
          writeio.flush
        end
        writeio.close

        while (line1 = readio.readline) && (line2 = stderrio.readline)
          ary1 << line1
          ary2 << line2
        end

        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
    end

    assert_equal [], @d.log.out.logs
    expected = (1..6).map{|i| "my data#{i}\n" }
    assert_equal expected, ary1
    assert_equal expected, ary2
  end

  test 'can execute external command at just once, which can handle both of write and read (with stderr)' do
    m = Mutex.new
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      cmd = "ruby"
      args = ['-e', 'while !STDIN.eof? && line = STDIN.readline; puts "[s]" + line.chomp; STDOUT.flush rescue nil; STDERR.puts "[e]" + line.chomp; STDERR.flush rescue nil; end']
      @d.child_process_execute(:t2b, cmd, arguments: args, mode: [:write, :read_with_stderr]) do |writeio, readio|
        m.lock
        ran = true

        [[1,2],[3,4],[5,6]].each do |i,j|
          writeio.write "my data#{i}\n"
          writeio.write "my data#{j}\n"
          writeio.flush
        end
        writeio.close

        while line = readio.readline
          ary << line
        end

        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
    end

    assert_equal [], @d.log.out.logs
    expected = (1..6).map{|i| ["[s]my data#{i}\n", "[e]my data#{i}\n"] }.flatten
    assert_equal expected, ary
  end

  test 'can execute external command at just once, which runs forever' do
    m = Mutex.new
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      args = ["-e", "while sleep #{TEST_WAIT_INTERVAL_FOR_LOOP}; puts 1; STDOUT.flush; end"]
      @d.child_process_execute(:t3, "ruby", arguments: args, mode: [:read]) do |io|
        m.lock
        ran = true
        begin
          while @d.child_process_running? && line = io.readline
            ary << line
          end
        rescue
          # ignore
        ensure
          m.unlock
        end
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      sleep TEST_WAIT_INTERVAL_FOR_LOOP * 10
      @d.stop # nothing occurs
      @d.shutdown

      assert{ ary.size > 5 }

      @d.close

      @d.terminate
      assert @d._child_process_processes.empty?
    end
  end

  # In windows environment, child_process try KILL at first (because there's no SIGTERM)
  test 'can execute external command just once, and can terminate it forcedly when shutdown/terminate even if it ignore SIGTERM' do
    omit "SIGTERM is unavailable on Windows" if Fluent.windows?

    m = Mutex.new
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t4, "ruby -e 'Signal.trap(:TERM, nil); while sleep #{TEST_WAIT_INTERVAL_FOR_LOOP}; puts 1; STDOUT.flush rescue nil; end'", mode: [:read]) do |io|
        m.lock
        ran = true
        begin
          while line = io.readline
            ary << line
          end
        rescue
          # ignore
        ensure
          m.unlock
        end
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran

      assert_equal [], @d.log.out.logs

      @d.stop # nothing occurs
      sleep TEST_WAIT_INTERVAL_FOR_LOOP * 5
      lines1 = ary.size
      assert{ lines1 > 1 }

      pid = @d._child_process_processes.keys.first

      @d.shutdown
      sleep TEST_WAIT_INTERVAL_FOR_LOOP * 5
      lines2 = ary.size
      assert{ lines2 > lines1 }

      @d.close

      assert_nil((Process.waitpid(pid, Process::WNOHANG) rescue nil))

      @d.terminate
      assert @d._child_process_processes.empty?
      begin
        Process.waitpid(pid)
      rescue Errno::ECHILD
      end
      # Process successfully KILLed if test reaches here
      assert true
    end
  end

  test 'can execute external command many times, which finishes immediately' do
    ary = []
    arguments = ["-e", "3.times{ puts 'okay'; STDOUT.flush rescue nil; sleep #{TEST_WAIT_INTERVAL_FOR_LOOP} }"] # 0.5 * 3
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t5, "ruby", arguments: arguments, interval: 5, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 13 # 5sec * 2 + 3sec
      assert_equal [], @d.log.out.logs
      @d.stop
      assert_equal [], @d.log.out.logs
      @d.shutdown; @d.close; @d.terminate
      assert_equal 2, ary.size
    end
  end

  test 'can execute external command many times, with leading once executed immediately' do
    ary = []
    arguments = ["-e", "3.times{ puts 'okay'; STDOUT.flush rescue nil; sleep #{TEST_WAIT_INTERVAL_FOR_LOOP} }"]
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t6, "ruby", arguments: arguments, interval: 5, immediate: true, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 8 # 5sec * 1 + 3sec
              # but expected lines are same with test above
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert_equal 2, ary.size
      assert_equal [], @d.log.out.logs
    end
  end

  test 'does not execute long running external command in parallel in default' do
    ary = []
    arguments = ["-e", "10.times{ puts 'okay'; STDOUT.flush rescue nil; sleep #{TEST_WAIT_INTERVAL_FOR_LOOP} }"] # 0.5 * 10
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      assert_equal [], @d.log.out.logs
      @d.log.out.singleton_class.module_eval do
        define_method(:write){|message|
          raise "boo" if message.include?('test: {"test":"test"}') || message.include?('test: {"test"=>"test"}')
          @logs.push message
        }
      end

      @d.child_process_execute(:t7, "ruby", arguments: arguments, interval: 2, immediate: true, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 4
      assert_equal 1, @d._child_process_processes.size
      @d.stop
      warn_msg = '[warn]: previous child process is still running. skipped. title=:t7 command="ruby" arguments=["-e", "10.times{ puts \'okay\'; STDOUT.flush rescue nil; sleep 0.5 }"] interval=2 parallel=false' + "\n"
      logs = @d.log.out.logs
      assert{ logs.first.end_with?(warn_msg) }
      assert{ logs.all?{|line| line.end_with?(warn_msg) } }
      @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'can execute long running external command in parallel if specified' do
    ary = []
    arguments = ["-e", "10.times{ puts 'okay'; STDOUT.flush rescue nil; sleep #{TEST_WAIT_INTERVAL_FOR_LOOP} }"] # 0.5 * 10 sec
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t8, "ruby", arguments: arguments, interval: 1, immediate: true, parallel: true, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 4
      processes = @d._child_process_processes.size
      assert{ processes >= 3 && processes <= 5 }
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'execute external processes only for writing' do
    m = Mutex.new
    unreadable = false
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t9, "ruby", arguments: ['-e', 'a=""; while b=STDIN.readline; a+=b; end'], mode: [:write]) do |io|
        m.lock
        ran = true
        begin
          io.read
        rescue IOError
          unreadable = true
        end
        50.times do
          io.write "hahaha\n"
        end
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
      assert unreadable
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'execute external processes only for reading' do
    m = Mutex.new
    unwritable = false
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t10, "ruby", arguments: ["-e", "while sleep #{TEST_WAIT_INTERVAL_FOR_LOOP}; puts 1; STDOUT.flush rescue nil; end"], mode: [:read]) do |io|
        m.lock
        ran = true
        begin
          io.write "foobar"
        rescue IOError
          unwritable = true
        end
        _data = io.readline
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      m.unlock
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert unwritable
      assert_equal [], @d.log.out.logs
    end
  end

  test 'can control external encodings' do
    m = Mutex.new
    encodings = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t11, "ruby -e 'sleep 10'", external_encoding: 'ascii-8bit') do |r, w|
        m.lock
        ran = true
        encodings << r.external_encoding
        encodings << w.external_encoding
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      assert_equal Encoding::ASCII_8BIT, encodings[0]
      assert_equal Encoding::ASCII_8BIT, encodings[1]
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can control internal encodings' do
    m = Mutex.new
    encodings = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t12, "ruby -e 'sleep 10'", external_encoding: 'utf-8', internal_encoding: 'ascii-8bit') do |r, w|
        m.lock
        ran = true
        encodings << r.internal_encoding
        encodings << w.internal_encoding
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      assert_equal Encoding::ASCII_8BIT, encodings[0]
      assert_equal Encoding::ASCII_8BIT, encodings[1]
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can convert encodings from ascii-8bit to utf-8' do
    m = Mutex.new
    str = nil
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      args = ['-e', 'STDOUT.set_encoding("ascii-8bit"); STDOUT.write "\xA4\xB5\xA4\xC8\xA4\xB7"']
      @d.child_process_execute(:t13, "ruby", arguments: args, external_encoding: 'euc-jp', internal_encoding: 'windows-31j', mode: [:read]) do |io|
        m.lock
        ran = true
        str = io.read
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      assert_equal Encoding.find('windows-31j'), str.encoding
      expected = "さとし".encode('windows-31j')
      assert_equal expected, str
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can scrub characters without exceptions' do
    m = Mutex.new
    str = nil
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      args = ['-e', 'STDOUT.set_encoding("ascii-8bit"); STDOUT.write "\xFF\xFF\x00\xF0\xF0"']
      @d.child_process_execute(:t13a, "ruby", arguments: args, mode: [:read]) do |io|
        m.lock
        ran = true
        str = io.read
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      assert_equal Encoding.find('utf-8'), str.encoding
      expected = "\xEF\xBF\xBD\xEF\xBF\xBD\x00\xEF\xBF\xBD\xEF\xBF\xBD".force_encoding("utf-8")
      assert_equal expected, str
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can scrub characters without exceptions and replace specified chars' do
    m = Mutex.new
    str = nil
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      args = ['-e', 'STDOUT.set_encoding("ascii-8bit"); STDOUT.write "\xFF\xFF\x00\xF0\xF0"']
      @d.child_process_execute(:t13b, "ruby", arguments: args, mode: [:read], scrub: true, replace_string: '?') do |io|
        m.lock
        ran = true
        str = io.read
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      assert_equal Encoding.find('utf-8'), str.encoding
      expected = "??\x00??".force_encoding("utf-8")
      assert_equal expected, str
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  unless Fluent.windows?
    test 'can specify subprocess name' do
      io = IO.popen([["cat", "caaaaaaaaaaat"], '-'])
      process_naming_enabled = (open("|ps opid,cmd"){|_io| _io.readlines }.select{|line| line.include?("caaaaaaaaaaat") }.size > 0)
      Process.kill(:TERM, io.pid) rescue nil
      io.close rescue nil

      # Does TravisCI prohibit process renaming?
      # This test will be passed in such environment
      pend unless process_naming_enabled

      m = Mutex.new
      pids = []
      proc_lines = []
      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        ran = false
        @d.child_process_execute(:t14, "ruby", arguments:['-e', 'sleep 10; puts "hello"'], subprocess_name: "sleeeeeeeeeper", mode: [:read]) do |readio|
          m.lock
          ran = true
          pids << @d.child_process_id
          proc_lines += open("|ps opid,cmd"){|_io| _io.readlines }
          m.unlock
          readio.read
        end
        sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
        m.lock
        pid = pids.first
        # 16357 sleeeeeeeeeper -e sleep 10; puts "hello"
        assert{ proc_lines.select{|line| line =~ /^\s*#{pid}\s/ }.first.strip.split(/\s+/)[1] == "sleeeeeeeeeper" }
        @d.stop; @d.shutdown; @d.close; @d.terminate
      end
    end
  end

  test 'can set ENV variables' do
    m = Mutex.new
    str = nil
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      args = ['-e', 'puts ENV["testing_child_process"]']
      @d.child_process_execute(:t15a, "ruby", arguments: args, mode: [:read], env: {'testing_child_process' => 'Yes! True!'}) do |io|
        m.lock
        ran = true
        str = io.read
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      expected = "Yes! True!\n"
      assert_equal expected, str
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can unset ENV variables of Fluentd process' do
    m = Mutex.new
    str = nil
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      current_env_path = ENV['PATH']
      ran = false
      args = ['-e', 'puts ENV["testing_child_process1"].to_s + ENV["testing_child_process2"].to_s']
      ENV['testing_child_process1'] = "No! False!"
      @d.child_process_execute(:t15b, "ruby", arguments: args, mode: [:read], unsetenv: true, env: {'testing_child_process2' => 'Yes! True!', 'PATH' => current_env_path}) do |io|
        m.lock
        ran = true
        str = io.read
        m.unlock
      end
      sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
      m.lock
      expected = "Yes! True!\n"
      assert_equal expected, str
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  unless Fluent.windows?
    test 'can change working directory' do
      # check my real /tmp directory (for mac)
      cmd = %[|ruby -e 'Dir.chdir("/tmp"); puts Dir.pwd']
      mytmpdir = open(cmd){|io| io.read.chomp }

      m = Mutex.new
      str = nil
      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        ran = false
        args = ['-e', 'puts Dir.pwd']
        @d.child_process_execute(:t16, "ruby", arguments: args, mode: [:read], chdir: "/tmp") do |io|
          m.lock
          ran = true
          str = io.read.chomp
          m.unlock
        end
        sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until m.locked? || ran
        m.lock
        assert_equal mytmpdir, str
        @d.stop; @d.shutdown; @d.close; @d.terminate
      end
    end
  end

  sub_test_case 'on_exit_callback is specified' do
    setup do
      @temp = Tempfile.create("child_process_wait_with_on_exit_callback")
      @temp_path = @temp.path
      @temp.close
    end

    teardown do
      File.unlink @temp_path if File.exist?(@temp_path)
    end

    test 'can return exit status for child process successfully exits using on_exit_callback' do
      assert File.exist?(@temp_path)

      block_exits = false
      callback_called = false
      exit_status = nil
      args = ['-e', 'sleep ARGV[0].to_i; puts "yay"; File.unlink ARGV[1]', '1', @temp_path]
      cb = ->(status){ exit_status = status; callback_called = true }

      str = nil

      pid = nil
      @d.child_process_execute(:st1, "ruby", arguments: args, mode: [:read], on_exit_callback: cb) do |readio|
        pid = @d.instance_eval{ child_process_id }
        str = readio.read.chomp
        block_exits = true
      end
      waiting(TEST_DEADLOCK_TIMEOUT){ sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING while @d.child_process_exist?(pid) } # to get exit status
      waiting(TEST_DEADLOCK_TIMEOUT){ sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until block_exits }
      waiting(TEST_DEADLOCK_TIMEOUT){ sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until callback_called }

      assert callback_called
      assert exit_status
      assert_equal 0, exit_status.exitstatus
      assert !File.exist?(@temp_path)

      assert_equal "yay", str
    end

    test 'can return exit status with signal code for child process killed by signal using on_exit_callback' do
      omit "SIGQUIT is unsupported on Windows" if Fluent.windows?

      assert File.exist?(@temp_path)

      block_exits = false
      callback_called = false
      exit_status = nil
      args = ['-e', 'sleep ARGV[0].to_i; puts "yay"; File.unlink ARGV[1]', '100', @temp_path]
      cb = ->(status){ exit_status = status; callback_called = true }

      str = nil

      pid = nil
      @d.child_process_execute(:st1, "ruby", arguments: args, mode: [:read], on_exit_callback: cb) do |readio|
        pid = @d.instance_eval{ child_process_id }
        sleep 10 # to run child process correctly
        Process.kill(:QUIT, pid)
        sleep 1
        Process.kill(:QUIT, pid) rescue nil # once more to send kill
        sleep 1
        Process.kill(:QUIT, pid) rescue nil # just like sync
        str = readio.read.chomp rescue nil # empty string before EOF
        block_exits = true
      end
      waiting(TEST_DEADLOCK_TIMEOUT){ sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING while @d.child_process_exist?(pid) } # to get exit status
      waiting(TEST_DEADLOCK_TIMEOUT){ sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until block_exits }
      waiting(TEST_DEADLOCK_TIMEOUT){ sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until callback_called }

      assert callback_called
      assert exit_status

      # This test sometimes fails on TravisCI
      #    with [nil, 11] # SIGSEGV
      # or with [1, nil]  # ???
      assert_equal [nil, 3, true, ""], [exit_status.exitstatus, exit_status.termsig, File.exist?(@temp_path), str] # SIGQUIT
      # SIGSEGV looks a kind of BUG of ruby...
    end

    test 'calls on_exit_callback for each process exits for interval call using on_exit_callback' do
      read_data_list = []
      exit_status_list = []

      args = ['-e', 'puts "yay"', '1']
      cb = ->(status){ exit_status_list << status }

      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        @d.child_process_execute(:st1, "ruby", arguments: args, immediate: true, interval: 2, mode: [:read], on_exit_callback: cb) do |readio|
          read_data_list << readio.read.chomp
        end
        sleep 10
      end

      assert{ read_data_list.size >= 3 }
      assert{ exit_status_list.size >= 3 }
    end

    test 'waits lasting child process until wait_timeout if block is not specified' do
      assert File.exist?(@temp_path)

      callback_called = false
      exit_status = nil
      args = ['-e', 'sleep ARGV[0].to_i; File.unlink ARGV[1]', '1', @temp_path]
      cb = ->(status){ exit_status = status; callback_called = true }

      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        @d.child_process_execute(:t17, "ruby", arguments: args, on_exit_callback: cb, wait_timeout: 5)
        sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until callback_called
      end

      assert callback_called
      assert exit_status
      assert_equal 0, exit_status.exitstatus
      assert !File.exist?(@temp_path)
    end

    test 'waits lasting child process until wait_timeout after block rans if block is specified' do
      assert File.exist?(@temp_path)

      callback_called = false
      exit_status = nil
      args = ['-e', 'sleep ARGV[0].to_i; File.unlink ARGV[1]', '3', @temp_path]
      cb = ->(status){ exit_status = status; callback_called = true }

      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        @d.child_process_execute(:t17, "ruby", arguments: args, mode: nil, on_exit_callback: cb, wait_timeout: 10) do
          sleep 1
        end
        sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until callback_called
      end

      assert callback_called
      assert exit_status
      assert_equal 0, exit_status.exitstatus
      assert !File.exist?(@temp_path)
    end

    test 'kills lasting child process after wait_timeout if block is not specified' do
      assert File.exist?(@temp_path)

      callback_called = false
      exit_status = nil
      args = ['-e', 'sleep ARGV[0].to_i; File.unlink ARGV[1]', '20', @temp_path]
      cb = ->(status){ exit_status = status; callback_called = true }

      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        @d.child_process_execute(:t17, "ruby", arguments: args, on_exit_callback: cb, wait_timeout: 3)
        sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until callback_called
      end

      assert callback_called
      assert exit_status
      unless Fluent.windows? # On Windows, exitstatus is always 0 and termsig is nil
        assert_nil exit_status.exitstatus
        assert_equal 9, exit_status.termsig # SIGKILL
      end
      assert File.exist?(@temp_path)
    end

    test 'kills lasting child process after block ran and wait_timeout expires if block is specified' do
      assert File.exist?(@temp_path)

      callback_called = false
      exit_status = nil
      args = ['-e', 'sleep ARGV[0].to_i; File.unlink ARGV[1]', '20', @temp_path]
      cb = ->(status){ exit_status = status; callback_called = true }

      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        @d.child_process_execute(:t17, "ruby", arguments: args, mode: nil, on_exit_callback: cb, wait_timeout: 3) do
          sleep 3
        end
        sleep TEST_WAIT_INTERVAL_FOR_BLOCK_RUNNING until callback_called
      end

      assert callback_called
      assert exit_status
      unless Fluent.windows? # On Windows, exitstatus is always 0 and termsig is nil
        assert_nil exit_status.exitstatus
        assert_equal 9, exit_status.termsig # SIGKILL
      end
      assert File.exist?(@temp_path)
    end
  end
end
