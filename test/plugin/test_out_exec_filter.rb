require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_exec_filter'
require 'fileutils'

class ExecFilterOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    command cat
    num_children 3
    <inject>
      tag_key     tag
      time_key    time_in
      time_type   string
      time_format %Y-%m-%d %H:%M:%S
    </inject>
    <format>
      keys ["time_in", "tag", "k1"]
    </format>
    <parse>
      keys ["time_out", "tag", "k2"]
    </parse>
    <extract>
      tag_key     tag
      time_key    time_out
      time_type   string
      time_format %Y-%m-%d %H:%M:%S
    </extract>
  ]

  CONFIG_COMPAT = %[
    command cat
    in_keys time_in,tag,k1
    out_keys time_out,tag,k2
    tag_key tag
    in_time_key time_in
    out_time_key time_out
    time_format %Y-%m-%d %H:%M:%S
    localtime
    num_children 3
  ]

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::ExecFilterOutput).configure(conf)
  end

  SED_SUPPORT_UNBUFFERED_OPTION = ->(){
    system("echo xxx | sed --unbuffered -l -e 's/x/y/g' >#{IO::NULL} 2>&1")
    $?.success?
  }.call
  SED_UNBUFFERED_OPTION = SED_SUPPORT_UNBUFFERED_OPTION ? '--unbuffered' : ''

  data(
    'with sections' => CONFIG,
    'traditional' => CONFIG_COMPAT,
  )
  test 'configure' do |conf|
    d = create_driver(conf)

    assert_false d.instance.parser.estimate_current_event

    assert_equal ["time_in","tag","k1"], d.instance.formatter.keys
    assert_equal ["time_out","tag","k2"], d.instance.parser.keys
    assert_equal "tag", d.instance.inject_config.tag_key
    assert_equal "tag", d.instance.extract_config.tag_key
    assert_equal "time_in", d.instance.inject_config.time_key
    assert_equal "time_out", d.instance.extract_config.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.inject_config.time_format
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.extract_config.time_format
    assert_equal true, d.instance.inject_config.localtime
    assert_equal 3, d.instance.num_children

    d = create_driver %[
      command sed -l -e s/foo/bar/
      in_keys time,k1
      out_keys time,k2
      tag xxx
      time_key time
      num_children 3
    ]
    assert_equal "sed -l -e s/foo/bar/", d.instance.command

    d = create_driver(conf + %[
      remove_prefix before
      add_prefix after
    ])
    assert_equal "before", d.instance.remove_prefix
    assert_equal "after" , d.instance.add_prefix
  end

  data(
    'with sections' => CONFIG,
    'traditional' => CONFIG_COMPAT,
  )
  test 'emit events with TSV format' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'test', expect_emits: 2, timeout: 10) do
      # sleep 0.1 until d.instance.children && !d.instance.children.empty? && d.instance.children.all?{|c| c.finished == false }
      d.feed(time, {"k1"=>1})
      d.feed(time, {"k1"=>2})
    end

    assert_equal "2011-01-02 13:14:15\ttest\t1\n", d.formatted[0]
    assert_equal "2011-01-02 13:14:15\ttest\t2\n", d.formatted[1]

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["test", time, {"k2"=>"1"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["test", time, {"k2"=>"2"}], events[1]
  end

  CONFIG_WITHOUT_TIME_FORMAT = %[
    command cat
    num_children 3
    tag xxx
    <inject>
      time_key time
      time_type unixtime
    </inject>
    <format>
      keys time,k1
    </format>
    <parse>
      keys time,k2
      time_key time
      time_type unixtime
    </parse>
  ]
  CONFIG_WITHOUT_TIME_FORMAT_COMPAT = %[
    command cat
    in_keys time,k1
    out_keys time,k2
    tag xxx
    time_key time
    num_children 3
  ]

  data(
    'with sections' => CONFIG_WITHOUT_TIME_FORMAT,
    'traditional' => CONFIG_WITHOUT_TIME_FORMAT_COMPAT,
  )
  test 'emit events without time format configuration' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15 +0900")

    d.run(default_tag: 'test', expect_emits: 2, timeout: 10) do
      d.feed(time, {"k1"=>1})
      d.feed(time, {"k1"=>2})
    end

    assert_equal "1293941655\t1\n", d.formatted[0]
    assert_equal "1293941655\t2\n", d.formatted[1]

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["xxx", time, {"k2"=>"1"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["xxx", time, {"k2"=>"2"}], events[1]
  end

  CONFIG_TO_DO_GREP = %[
    command grep --line-buffered -v poo
    num_children 3
    tag xxx
    <inject>
      time_key time
      time_type unixtime
    </inject>
    <format>
      keys time, val1
    </format>
    <parse>
      keys time, val2
      time_key time
      time_type unixtime
    </parse>
  ]
  CONFIG_TO_DO_GREP_COMPAT = %[
    command grep --line-buffered -v poo
    in_keys time,val1
    out_keys time,val2
    tag xxx
    time_key time
    num_children 3
  ]

  data(
    'with sections' => CONFIG_TO_DO_GREP,
    'traditional' => CONFIG_TO_DO_GREP_COMPAT,
  )
  test 'emit events through grep command' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15 +0900")

    d.run(default_tag: 'test', expect_emits: 1, timeout: 10) do
      d.feed(time, {"val1"=>"sed-ed value poo"})
      d.feed(time, {"val1"=>"sed-ed value foo"})
    end

    assert_equal "1293941655\tsed-ed value poo\n", d.formatted[0]
    assert_equal "1293941655\tsed-ed value foo\n", d.formatted[1]

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value foo"}], events[0]
  end

  CONFIG_TO_DO_SED = %[
    command sed #{SED_UNBUFFERED_OPTION} -l -e s/foo/bar/
    num_children 3
    tag xxx
    <inject>
      time_key time
      time_type unixtime
    </inject>
    <format>
      keys time, val1
    </format>
    <parse>
      keys time, val2
      time_key time
      time_type unixtime
    </parse>
  ]
  CONFIG_TO_DO_SED_COMPAT = %[
    command sed #{SED_UNBUFFERED_OPTION} -l -e s/foo/bar/
    in_keys time,val1
    out_keys time,val2
    tag xxx
    time_key time
    num_children 3
  ]

  data(
    'with sections' => CONFIG_TO_DO_SED,
    'traditional' => CONFIG_TO_DO_SED_COMPAT,
  )
  test 'emit events through sed command' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15 +0900")

    d.run(default_tag: 'test', expect_emits: 1, timeout: 10) do
      d.feed(time, {"val1"=>"sed-ed value poo"})
      d.feed(time, {"val1"=>"sed-ed value foo"})
    end

    assert_equal "1293941655\tsed-ed value poo\n", d.formatted[0]
    assert_equal "1293941655\tsed-ed value foo\n", d.formatted[1]

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value poo"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["xxx", time, {"val2"=>"sed-ed value bar"}], events[1]
  end

  CONFIG_TO_DO_SED_WITH_TAG_MODIFY = %[
    command sed #{SED_UNBUFFERED_OPTION} -l -e s/foo/bar/
    num_children 3
    remove_prefix input
    add_prefix output
    <inject>
      tag_key tag
      time_key time
    </inject>
    <format>
      keys tag, time, val1
    </format>
    <parse>
      keys tag, time, val2
    </parse>
    <extract>
      tag_key tag
      time_key time
    </extract>
  ]
  CONFIG_TO_DO_SED_WITH_TAG_MODIFY_COMPAT = %[
    command sed #{SED_UNBUFFERED_OPTION} -l -e s/foo/bar/
    in_keys tag,time,val1
    remove_prefix input
    out_keys tag,time,val2
    add_prefix output
    tag_key tag
    time_key time
    num_children 3
  ]

  data(
    'with sections' => CONFIG_TO_DO_SED_WITH_TAG_MODIFY,
    'traditional' => CONFIG_TO_DO_SED_WITH_TAG_MODIFY_COMPAT,
  )
  test 'emit events with add/remove tag prefix' do |conf|
    d = create_driver(conf)

    time = event_time("2011-01-02 13:14:15 +0900")

    d.run(default_tag: 'input.test', expect_emits: 2, timeout: 10) do
      d.feed(time, {"val1"=>"sed-ed value foo"})
      d.feed(time, {"val1"=>"sed-ed value poo"})
    end

    assert_equal "test\t1293941655\tsed-ed value foo\n", d.formatted[0]
    assert_equal "test\t1293941655\tsed-ed value poo\n", d.formatted[1]

    events = d.events
    assert_equal 2, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["output.test", time, {"val2"=>"sed-ed value bar"}], events[0]
    assert_equal_event_time time, events[1][1]
    assert_equal ["output.test", time, {"val2"=>"sed-ed value poo"}], events[1]
  end

  CONFIG_JSON = %[
    command cat
    <format>
      @type tsv
      keys message
    </format>
    <parse>
      @type json
    </parse>
    <extract>
      tag_key tag
      time_key time
    </extract>
  ]
  CONFIG_JSON_COMPAT = %[
    command cat
    in_keys message
    out_format json
    time_key time
    tag_key tag
  ]

  data(
    'with sections' => CONFIG_JSON,
    'traditional' => CONFIG_JSON_COMPAT,
  )
  test 'using json format' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15 +0900")

    d.run(default_tag: 'input.test', expect_emits: 1, timeout: 10) do
      i = d.instance
      assert{ i.router }
      d.feed(time, {"message"=>%[{"time":#{time},"tag":"t1","k1":"v1"}]})
    end

    assert_equal '{"time":1293941655,"tag":"t1","k1":"v1"}' + "\n", d.formatted[0]

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["t1", time, {"k1"=>"v1"}], events[0]
  end

  CONFIG_JSON_WITH_FLOAT_TIME = %[
    command cat
    <format>
      @type tsv
      keys message
    </format>
    <parse>
      @type json
    </parse>
    <extract>
      tag_key tag
      time_key time
    </extract>
  ]
  CONFIG_JSON_WITH_FLOAT_TIME_COMPAT = %[
    command cat
    in_keys message
    out_format json
    time_key time
    tag_key tag
  ]

  data(
    'with sections' => CONFIG_JSON_WITH_FLOAT_TIME,
    'traditional' => CONFIG_JSON_WITH_FLOAT_TIME_COMPAT,
  )
  test 'using json format with float time' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15.123 +0900")

    d.run(default_tag: 'input.test', expect_emits: 1, timeout: 10) do
      d.feed(time + 10, {"message"=>%[{"time":#{time.sec}.#{time.nsec},"tag":"t1","k1":"v1"}]})
    end

    assert_equal '{"time":1293941655.123000000,"tag":"t1","k1":"v1"}' + "\n", d.formatted[0]

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["t1", time, {"k1"=>"v1"}], events[0]
  end

  CONFIG_JSON_WITH_TIME_FORMAT = %[
    command cat
    <format>
      @type tsv
      keys message
    </format>
    <parse>
      @type json
    </parse>
    <extract>
      tag_key tag
      time_key time
      time_type string
      time_format %d/%b/%Y %H:%M:%S.%N %z
    </extract>
  ]
  CONFIG_JSON_WITH_TIME_FORMAT_COMPAT = %[
    command cat
    in_keys message
    out_format json
    time_key time
    time_format %d/%b/%Y %H:%M:%S.%N %z
    tag_key tag
  ]

  data(
    'with sections' => CONFIG_JSON_WITH_TIME_FORMAT,
    'traditional' => CONFIG_JSON_WITH_TIME_FORMAT_COMPAT,
  )
  test 'using json format with custom time format' do |conf|
    d = create_driver(conf)
    time_str = "28/Feb/2013 12:00:00.123456789 +0900"
    time = event_time(time_str, format: "%d/%b/%Y %H:%M:%S.%N %z")

    d.run(default_tag: 'input.test', expect_emits: 1, timeout: 10) do
      d.feed(time + 10, {"message"=>%[{"time":"#{time_str}","tag":"t1","k1":"v1"}]})
    end

    assert_equal '{"time":"28/Feb/2013 12:00:00.123456789 +0900","tag":"t1","k1":"v1"}' + "\n", d.formatted[0]

    events = d.events
    assert_equal 1, events.length
    assert_equal_event_time time, events[0][1]
    assert_equal ["t1", time, {"k1"=>"v1"}], events[0]
  end

  CONFIG_ROUND_ROBIN = %[
    command ruby -e 'STDOUT.sync = true; STDIN.each_line{|line| puts line.chomp + "\t" + Process.pid.to_s }'
    num_children 3
    <inject>
      tag_key     tag
      time_key    time_in
      time_type   string
      time_format %Y-%m-%d %H:%M:%S
    </inject>
    <format>
      keys ["time_in", "tag", "k1"]
    </format>
    <parse>
      keys ["time_out", "tag", "k2", "child_pid"]
    </parse>
    <extract>
      tag_key     tag
      time_key    time_out
      time_type   string
      time_format %Y-%m-%d %H:%M:%S
    </extract>
  ]
  CONFIG_ROUND_ROBIN_COMPAT = %[
    command ruby -e 'STDOUT.sync = true; STDIN.each_line{|line| puts line.chomp + "\t" + Process.pid.to_s }'
    in_keys time_in,tag,k1
    out_keys time_out,tag,k2,child_pid
    tag_key tag
    in_time_key time_in
    out_time_key time_out
    time_format %Y-%m-%d %H:%M:%S
    localtime
    num_children 3
  ]

  data(
    'with sections' => CONFIG_ROUND_ROBIN,
    'traditional' => CONFIG_ROUND_ROBIN_COMPAT,
  )
  test 'using child processes by round robin' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15")

    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: true,  shutdown: false){ d.feed(time, {"k1"=>1}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>2}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>3}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>4}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>5}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>6}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>7}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: false){ d.feed(time, {"k1"=>8}) }
    d.run(default_tag: 'test', expect_emits: 1, timeout: 10, start: false, shutdown: true ){ d.feed(time, {"k1"=>9}) }

    assert_equal "2011-01-02 13:14:15\ttest\t1\n", d.formatted[0]
    assert_equal "2011-01-02 13:14:15\ttest\t2\n", d.formatted[1]
    assert_equal "2011-01-02 13:14:15\ttest\t3\n", d.formatted[2]
    assert_equal "2011-01-02 13:14:15\ttest\t4\n", d.formatted[3]
    assert_equal "2011-01-02 13:14:15\ttest\t5\n", d.formatted[4]
    assert_equal "2011-01-02 13:14:15\ttest\t6\n", d.formatted[5]
    assert_equal "2011-01-02 13:14:15\ttest\t7\n", d.formatted[6]
    assert_equal "2011-01-02 13:14:15\ttest\t8\n", d.formatted[7]
    assert_equal "2011-01-02 13:14:15\ttest\t9\n", d.formatted[8]

    events = d.events
    assert_equal 9, events.length

    pid_list = []
    events.each do |event|
      pid = event[2]['child_pid']
      pid_list << pid unless pid_list.include?(pid)
    end
    assert_equal 3, pid_list.size, "the number of pids should be same with number of child processes: #{pid_list.inspect}"

    assert_equal pid_list[0], events[0][2]['child_pid']
    assert_equal pid_list[1], events[1][2]['child_pid']
    assert_equal pid_list[2], events[2][2]['child_pid']
    assert_equal pid_list[0], events[3][2]['child_pid']
    assert_equal pid_list[1], events[4][2]['child_pid']
    assert_equal pid_list[2], events[5][2]['child_pid']
    assert_equal pid_list[0], events[6][2]['child_pid']
    assert_equal pid_list[1], events[7][2]['child_pid']
    assert_equal pid_list[2], events[8][2]['child_pid']
  end

  # child process exits per 3 lines
  CONFIG_RESPAWN = %[
    command ruby -e 'STDOUT.sync = true; proc = ->(){line = STDIN.readline.chomp; puts line + "\t" + Process.pid.to_s}; proc.call; proc.call; proc.call'
    num_children 4
    child_respawn -1
    <inject>
      tag_key   tag
      time_key  time_in
      time_type unixtime
    </inject>
    <format>
      keys ["time_in", "tag", "k1"]
    </format>
    <parse>
      keys ["time_out", "tag", "k2", "child_pid"]
    </parse>
    <extract>
      tag_key   tag
      time_key  time_out
      time_type unixtime
    </extract>
  ]

  CONFIG_RESPAWN_COMPAT = %[
    command ruby -e 'STDOUT.sync = true; proc = ->(){line = STDIN.readline.chomp; puts line + "\t" + Process.pid.to_s}; proc.call; proc.call; proc.call'
    num_children 4
    child_respawn -1
    in_keys time_in,tag,k1
    out_keys time_out,tag,k2,child_pid
    tag_key tag
    in_time_key time_in
    out_time_key time_out
#    time_format %Y-%m-%d %H:%M:%S
#    localtime
  ]

  data(
    'with sections' => CONFIG_RESPAWN,
    'traditional' => CONFIG_RESPAWN_COMPAT,
  )
  test 'emit events via child processes which exits sometimes' do |conf|
    d = create_driver(conf)
    time = event_time("2011-01-02 13:14:15")

    countup = 0

    d.run(start: true, shutdown: false)

    assert_equal 4, d.instance.instance_eval{ @_child_process_processes.size }

    20.times do
      d.run(default_tag: 'test', expect_emits: 1, timeout: 10, force_flush_retry: true, start: false, shutdown: false) do
        d.feed(time, {"k1"=>countup}); countup += 1
        d.feed(time, {"k1"=>countup}); countup += 1
        d.feed(time, {"k1"=>countup}); countup += 1
      end
    end

    events = d.events
    assert_equal 60, events.length

    pid_list = []
    events.each do |event|
      pid = event[2]['child_pid']
      pid_list << pid unless pid_list.include?(pid)
    end
    # the number of pids should be same with number of child processes
    assert{ pid_list.size >= 18 }

    logs = d.instance.log.out.logs
    assert{ logs.select{|l| l.include?("child process exits with error code") }.size >= 18 } # 20
    assert{ logs.select{|l| l.include?("respawning child process") }.size >= 18 } # 20

    d.run(start: false, shutdown: true)
  end
end
