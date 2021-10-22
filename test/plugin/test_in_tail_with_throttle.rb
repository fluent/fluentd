require_relative '../helper'

require 'fluent/test'
require 'fluent/test/helpers'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_tail_with_throttle'

class ThrottleInputTest < Test::Unit::TestCase

  def setup
    Fluent::Test.setup
    cleanup_directory(TMP_DIR)
  end

  def teardown
    super
    cleanup_directory(TMP_DIR)
    Fluent::Engine.stop
  end

  def cleanup_directory(path)
    unless Dir.exist?(path)
      FileUtils.mkdir_p(path)
      return
    end

    if Fluent.windows?
      Dir.glob("*", base: path).each do |name|
        begin
          cleanup_file(File.join(path, name))
        rescue
          # expect test driver block release already owned file handle.
        end
      end
    else
      begin
        FileUtils.rm_f(path, secure:true)
      rescue ArgumentError
        FileUtils.rm_f(path) # For Ruby 2.6 or before.
      end
      if File.exist?(path)
        FileUtils.remove_entry_secure(path, true)
      end
    end
    FileUtils.mkdir_p(path)
  end

  def cleanup_file(path)
    if Fluent.windows?
      # On Windows, when the file or directory is removed and created
      # frequently, there is a case that creating file or directory will
      # fail. This situation is caused by pending file or directory
      # deletion which is mentioned on win32 API document [1]
      # As a workaround, execute rename and remove method.
      #
      # [1] https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea#files
      #
      file = File.join(Dir.tmpdir, SecureRandom.hex(10))
      begin
        FileUtils.mv(path, file)
        FileUtils.rm_rf(file, secure: true)
      rescue ArgumentError
        FileUtils.rm_rf(file) # For Ruby 2.6 or before.
      end
      if File.exist?(file)
        # ensure files are closed for Windows, on which deleted files
        # are still visible from filesystem
        GC.start(full_mark: true, immediate_mark: true, immediate_sweep: true)
        FileUtils.remove_entry_secure(file, true)
      end
    else
      begin
        FileUtils.rm_f(path, secure: true)
      rescue ArgumentError
        FileUtils.rm_f(path) # For Ruby 2.6 or before.
      end
      if File.exist?(path)
        FileUtils.remove_entry_secure(path, true)
      end
    end
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail_with_throttle#{ENV['TEST_ENV_NUMBER']}"

  def create_group_directive(pattern, rate_period, *rules)
    config_element("", "", {}, [
      config_element("group", "", {
        "pattern" => pattern,
        "rate_period" => rate_period
      }, rules)
    ])
  end

  def create_rule_directive(namespace = [], appname = [], limit)
    params = {        
      "limit" => limit,
    }
    params["namespace"] = namespace.join(', ') if namespace.size > 0
    params["appname"] = appname.join(', ') if appname.size > 0
    config_element("rule", "", params)
  end

  def create_path_element(path)
    config_element("source", "", { "path" => "#{TMP_DIR}/#{path}" })
  end

  def create_driver(conf, add_path = true)
    conf = add_path ? conf + create_path_element("tail.txt") : conf
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ThrottleInput).configure(conf)
  end

  CONFG = config_element("source", "", {
    "@type" => "tail_with_throttle",
    "tag" => "t1",
    "pos_file" => "#{TMP_DIR}/tail.pos", 
    "refresh_interval" => "1s",
    "rotate_wait" => "2s",
  }, [
    config_element("parse", "", { 
      "@type" => "/(?<message>.*)/" })
    ]
  )
  PATTERN = "/#{TMP_DIR}\/(?<appname>[a-z0-9]([-a-z0-9]*[a-z0-9])?(\/[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)_(?<namespace>[^_]+)_(?<container>.+)-(?<docker_id>[a-z0-9]{6})\.log$/"
  READ_FROM_HEAD = config_element("source", "", { "read_from_head" => "true" })

  sub_test_case "#configuration" do
    
    test "<group> required" do
      assert_raise(Fluent::ConfigError) do
        d = create_driver(CONFG)
      end
    end

    test "<rule> required" do
      conf = CONFG + create_group_directive('.', '1m')
      assert_raise(Fluent::ConfigError) do 
        d = create_driver(conf)
      end
    end

    test "valid configuration" do
      rule1 = create_rule_directive(['namespace-a'], ['appname-b','appname-c'], 100)
      rule2 = create_rule_directive(['namespace-d', 'appname-e'], ['f'], 50)
      rule3 = create_rule_directive([], ['appname-g'], -1)
      rule4 = create_rule_directive(['appname-h'], [], 0)

      conf = CONFG + create_group_directive('.', '1m', rule1, rule2, rule3, rule4)
      assert_nothing_raised do 
        d = create_driver(conf)
      end
    end

    test "limit should be greater than DEFAULT_LIMIT (-1)" do 
      rule1 = create_rule_directive(['namespace-a'], ['appname-b','appname-c'], -100)
      rule2 = create_rule_directive(['namespace-d', 'namespace-e'], ['appname-f'], 50)
      conf = CONFG + create_group_directive('.', '1m', rule1, rule2)
      assert_raise(RuntimeError) do 
        d = create_driver(conf)
      end   
    end

  end
  
  sub_test_case "group rules line limit resolution" do

    test "valid" do
      rule1 = create_rule_directive(['namespace-a'], ['appname-b', 'appname-c'], 50)
      rule2 = create_rule_directive([], ['appname-b', 'appname-c'], 400)
      rule3 = create_rule_directive(['namespace-a'], [], 100)
  
      conf = CONFG + create_group_directive('.', '1m', rule1, rule2, rule3)
      assert_nothing_raised do
        d = create_driver(conf)

        assert_equal 25, d.instance.group_watchers[/namespace\-a/][/appname\-b/].limit
        assert_equal 25, d.instance.group_watchers[/namespace\-a/][/appname\-c/].limit
        assert_equal 100, d.instance.group_watchers[/namespace\-a/][/./].limit
        assert_equal 200, d.instance.group_watchers[/./][/appname\-b/].limit
        assert_equal 200, d.instance.group_watchers[/./][/appname\-c/].limit
        assert_equal -1, d.instance.group_watchers[/./][/./].limit
      end
    end

  end

  sub_test_case "files should be placed in groups" do
    test "invalid regex pattern places files in default group" do
      rule1 = create_rule_directive([], [], 100) ## limits default groups
      conf = CONFG + create_group_directive('.', '1m', rule1) + create_path_element("test*.txt")

      d = create_driver(conf, false)
      File.open("#{TMP_DIR}/test1.txt", 'w')
      File.open("#{TMP_DIR}/test2.txt", 'w')
      File.open("#{TMP_DIR}/test3.txt", 'w')

      d.run do
        ## checking default group_watcher's paths
        assert_equal 3, d.instance.group_watchers[/./][/./].size
        assert_true d.instance.group_watchers[/./][/./].include? File.join(TMP_DIR, 'test1.txt')
        assert_true d.instance.group_watchers[/./][/./].include? File.join(TMP_DIR, 'test2.txt')
        assert_true d.instance.group_watchers[/./][/./].include? File.join(TMP_DIR, 'test3.txt')
      end
    end
    
    test "valid regex pattern places file in their respective groups" do
      rule1 = create_rule_directive(['test-namespace1'], ['test-appname1'], 100)
      rule2 = create_rule_directive(['test-namespace1'], [], 200)
      rule3 = create_rule_directive([], ['test-appname2'], 100)
      rule4 = create_rule_directive([], [], 100)

      path_element = create_path_element("test-appname*.log")

      conf = CONFG + create_group_directive(PATTERN, '1m', rule1, rule2, rule3, rule4) + path_element
      d = create_driver(conf, false)

      File.open("#{TMP_DIR}/test-appname1_test-namespace1_test-container-15fabq.log", 'w')
      File.open("#{TMP_DIR}/test-appname3_test-namespace1_test-container-15fabq.log", 'w')
      File.open("#{TMP_DIR}/test-appname2_test-namespace2_test-container-15fabq.log", 'w')
      File.open("#{TMP_DIR}/test-appname4_test-namespace3_test-container-15fabq.log", 'w')

      d.run do
        assert_true d.instance.group_watchers[/test\-namespace1/][/test\-appname1/].include? File.join(TMP_DIR, "test-appname1_test-namespace1_test-container-15fabq.log")
        assert_true d.instance.group_watchers[/test\-namespace1/][/./].include? File.join(TMP_DIR, "test-appname3_test-namespace1_test-container-15fabq.log")
        assert_true d.instance.group_watchers[/./][/test\-appname2/].include? File.join(TMP_DIR, "test-appname2_test-namespace2_test-container-15fabq.log")
        assert_true d.instance.group_watchers[/./][/./].include? File.join(TMP_DIR, "test-appname4_test-namespace3_test-container-15fabq.log")
      end
    end
  
  end

  sub_test_case "throttling logs at in_tail level" do

    data("file test1.log no limit 5120 text: msg" => ["test1.log", 5120, "msg"],
         "file test2.log no limit 1024 text: test" => ["test2.log", 1024, "test"])
    def test_lines_collected_with_no_throttling(data)
      file, num_lines, msg = data
      rule = create_rule_directive([], [], -1)
      path_element = create_path_element(file)

      conf = CONFG + create_group_directive('.', '10s', rule) + path_element + READ_FROM_HEAD
      File.open("#{TMP_DIR}/#{file}", 'wb') do |f|
        num_lines.times do 
          f.puts "#{msg}\n"
        end
      end


      d = create_driver(conf, false)
      d.run do
        start_time = Time.now

        assert_true Time.now - start_time < 10
        assert_equal num_lines, d.record_count
        assert_equal({ "message" => msg }, d.events[0][2])

        prev_count = d.record_count
        ## waiting for atleast 12 seconds to avoid any sync errors between plugin and test driver
        sleep(1) until Time.now - start_time > 12
        ## after waiting for 10 secs, limit will reset 
        ## Plugin will start reading but it will encounter EOF Error 
        ## since no logs are left to be read
        ## Hence, d.record_count = prev_count
        assert_equal 0, d.record_count - prev_count
      end
    end
    
    test "lines collected with throttling" do
      file = "appname1_namespace12_container-123456.log"
      limit = 1000
      rate_period = '10s'
      num_lines = 3000
      msg = "a"*8190 # Total size = 8190 bytes + 2 (\n) bytes

      rule = create_rule_directive(['namespace'], ['appname'], limit)
      path_element = create_path_element(file)
      conf = CONFG + create_group_directive(PATTERN, rate_period, rule) + path_element + READ_FROM_HEAD

      d = create_driver(conf, false)

      File.open("#{TMP_DIR}/#{file}", 'wb') do |f|
        num_lines.times do 
          f.puts msg
        end
      end

      d.run do
        start_time = Time.now
        prev_count = 0

        3.times do
          assert_true Time.now - start_time < 10
          ## Check record_count after 10s to check lines reads
          assert_equal limit, d.record_count - prev_count
          prev_count = d.record_count 
          ## sleep until rate_period seconds are over so that 
          ## Plugin can read lines again
          sleep(1) until Time.now - start_time > 12 
          ## waiting for atleast 12 seconds to avoid any sync errors between plugin and test driver
          start_time = Time.now
        end
        ## When all the lines are read and rate_period seconds are over
        ## limit will reset and since there are no more logs to be read,
        ## number_lines_read will be 0
        assert_equal 0, d.instance.group_watchers[/namespace/][/appname/].current_paths["#{TMP_DIR}/#{file}"].number_lines_read
      end
    end
  end
end
