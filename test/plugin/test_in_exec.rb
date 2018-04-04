require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_exec'
require 'timecop'

class ExecInputTest < Test::Unit::TestCase
  SCRIPT_PATH = File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts', 'exec_script.rb'))
  TEST_TIME = "2011-01-02 13:14:15"
  TEST_UNIX_TIME = Time.parse(TEST_TIME)

  def setup
    Fluent::Test.setup
    @test_time = event_time()
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ExecInput).configure(conf)
  end

  DEFAULT_CONFIG_ONLY_WITH_KEYS = %[
    command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 0
    run_interval 1s
    tag "my.test.data"
    <parse>
      keys ["k1", "k2", "k3"]
    </parse>
  ]

  TSV_CONFIG = %[
    command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 0
    run_interval 1s
    <parse>
      @type tsv
      keys time, tag, k1
    </parse>
    <extract>
      tag_key tag
      time_key time
      time_type string
      time_format %Y-%m-%d %H:%M:%S
    </extract>
  ]

  JSON_CONFIG = %[
    command ruby #{SCRIPT_PATH} #{TEST_UNIX_TIME.to_i} 1
    run_interval 1s
    <parse>
      @type json
    </parse>
    <extract>
      tag_key tag
      time_key time
      time_type unixtime
    </extract>
  ]

  MSGPACK_CONFIG = %[
    command ruby #{SCRIPT_PATH} #{TEST_UNIX_TIME.to_i} 2
    run_interval 1s
    <parse>
      @type msgpack
    </parse>
    <extract>
      tag_key tagger
      time_key datetime
      time_type unixtime
    </extract>
  ]

  # here document for not de-quoting backslashes
  REGEXP_CONFIG = %[
    command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 3
    run_interval 1s
    tag regex_tag
] + <<'EOC'
    <parse>
      @type regexp
      expression "(?<time>[^\\]]*) (?<message>[^ ]*)"
      time_key time
      time_type string
      time_format %Y-%m-%d %H:%M:%S
    </parse>
EOC

  sub_test_case 'with configuration with sections' do
    test 'configure with default tsv format without extract' do
      d = create_driver DEFAULT_CONFIG_ONLY_WITH_KEYS
      assert{ d.instance.parser.is_a? Fluent::Plugin::TSVParser }
      assert_equal "my.test.data", d.instance.tag
      assert_equal ["k1", "k2", "k3"], d.instance.parser.keys
    end

    test 'configure raises error if both of tag and extract.tag_key are missing' do
      assert_raise Fluent::ConfigError.new("'tag' or 'tag_key' option is required on exec input") do
        create_driver %[
          command ruby -e 'puts "yay"'
          <parse>
            keys y1
          </parse>
        ]
      end
    end

    test 'configure for tsv' do
      d = create_driver TSV_CONFIG
      assert{ d.instance.parser.is_a? Fluent::Plugin::TSVParser }
      assert_equal ["time", "tag", "k1"], d.instance.parser.keys
      assert_equal "tag", d.instance.extract_config.tag_key
      assert_equal "time", d.instance.extract_config.time_key
      assert_equal :string, d.instance.extract_config.time_type
      assert_equal "%Y-%m-%d %H:%M:%S", d.instance.extract_config.time_format
    end

    test 'configure for json' do
      d = create_driver JSON_CONFIG
      assert{ d.instance.parser.is_a? Fluent::Plugin::JSONParser }
      assert_equal "tag", d.instance.extract_config.tag_key
      assert_equal "time", d.instance.extract_config.time_key
      assert_equal :unixtime, d.instance.extract_config.time_type
    end

    test 'configure for msgpack' do
      d = create_driver MSGPACK_CONFIG
      assert{ d.instance.parser.is_a? Fluent::Plugin::MessagePackParser }
      assert_equal "tagger", d.instance.extract_config.tag_key
      assert_equal "datetime", d.instance.extract_config.time_key
      assert_equal :unixtime, d.instance.extract_config.time_type
    end

    test 'configure for regexp' do
      d = create_driver REGEXP_CONFIG
      assert{ d.instance.parser.is_a? Fluent::Plugin::RegexpParser }
      assert_equal "regex_tag", d.instance.tag
      expression = /(?<time>[^\]]*) (?<message>[^ ]*)/
      assert_equal expression, d.instance.parser.expression
      assert_nil d.instance.extract_config
    end
  end

  TSV_CONFIG_COMPAT = %[
      command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 0
      keys time,tag,k1
      time_key time
      tag_key tag
      time_format %Y-%m-%d %H:%M:%S
      run_interval 1s
  ]

  JSON_CONFIG_COMPAT = %[
      command ruby #{SCRIPT_PATH} #{TEST_UNIX_TIME.to_i} 1
      format json
      tag_key tag
      time_key time
      run_interval 1s
  ]

  MSGPACK_CONFIG_COMPAT = %[
      command ruby #{SCRIPT_PATH} #{TEST_UNIX_TIME.to_i} 2
      format msgpack
      tag_key tagger
      time_key datetime
      run_interval 1s
  ]

  REGEXP_CONFIG_COMPAT = %[
      command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 3
      format /(?<time>[^\\\]]*) (?<message>[^ ]*)/
      tag regex_tag
      run_interval 1s
  ]

  sub_test_case 'with traditional configuration' do
    test 'configure' do
      d = create_driver TSV_CONFIG_COMPAT
      assert{ d.instance.parser.is_a? Fluent::Plugin::TSVParser }
      assert_equal ["time","tag","k1"], d.instance.parser.keys
      assert_equal "tag", d.instance.extract_config.tag_key
      assert_equal "time", d.instance.extract_config.time_key
      assert_equal "%Y-%m-%d %H:%M:%S", d.instance.extract_config.time_format
    end

    test 'configure_with_json' do
      d = create_driver JSON_CONFIG_COMPAT
      assert{ d.instance.parser.is_a? Fluent::Plugin::JSONParser }
    end

    test 'configure_with_msgpack' do
      d = create_driver MSGPACK_CONFIG_COMPAT
      assert{ d.instance.parser.is_a? Fluent::Plugin::MessagePackParser }
    end

    test 'configure_with_regexp' do
      d = create_driver REGEXP_CONFIG_COMPAT
      assert{ d.instance.parser.is_a? Fluent::Plugin::RegexpParser }
      assert_equal(/(?<time>[^\]]*) (?<message>[^ ]*)/, d.instance.parser.expression)
      assert_equal('regex_tag', d.instance.tag)
    end
  end

  sub_test_case 'with default configuration' do
    setup do
      @current_event_time = event_time('2016-10-31 20:01:30.123 -0700')
      Timecop.freeze(Time.at(@current_event_time))
    end

    teardown do
      Timecop.return
    end

    test 'emits events with current timestamp if time key is not specified' do
      d = create_driver DEFAULT_CONFIG_ONLY_WITH_KEYS
      d.run(expect_records: 2, timeout: 10)

      assert{ d.events.length > 0 }
      d.events.each do |event|
        assert_equal ["my.test.data", @current_event_time, {"k1"=>"2011-01-02 13:14:15", "k2"=>"tag1", "k3"=>"ok"}], event
      end
    end
  end

  data(
    'default' => [TSV_CONFIG,     "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'json'    => [JSON_CONFIG,    "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'msgpack' => [MSGPACK_CONFIG, "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'regexp'  => [REGEXP_CONFIG,  "regex_tag", event_time("2011-01-02 13:14:15"), {"message"=>"hello"}],
    'default_c' => [TSV_CONFIG_COMPAT,     "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'json_c'    => [JSON_CONFIG_COMPAT,    "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'msgpack_c' => [MSGPACK_CONFIG_COMPAT, "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'regexp_c'  => [REGEXP_CONFIG_COMPAT,  "regex_tag", event_time("2011-01-02 13:14:15"), {"message"=>"hello"}],
  )
  test 'emit with formats' do |data|
    config, tag, time, record = data
    d = create_driver(config)

    d.run(expect_emits: 2, timeout: 10)

    assert{ d.events.length > 0 }
    d.events.each_with_index {|event, i|
      assert_equal_event_time(time, event[1])
      assert_equal [tag, time, record], event
    }
  end
end
