require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_exec'
require 'net/http'

class ExecInputTest < Test::Unit::TestCase
  SCRIPT_PATH = File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts', 'exec_script.rb'))
  TEST_TIME = "2011-01-02 13:14:15"
  TEST_UNIX_TIME = Time.parse(TEST_TIME)

  def setup
    Fluent::Test.setup
    @test_time = event_time()
  end

  def create_driver(conf = TSV_CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ExecInput).configure(conf)
  end

  TSV_CONFIG = %[
      command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 0
      keys time,tag,k1
      time_key time
      tag_key tag
      time_format %Y-%m-%d %H:%M:%S
      run_interval 1s
  ]

  JSON_CONFIG = %[
      command ruby #{SCRIPT_PATH} #{TEST_UNIX_TIME.to_i} 1
      format json
      tag_key tag
      time_key time
      run_interval 1s
  ]

  MSGPACK_CONFIG = %[
      command ruby #{SCRIPT_PATH} #{TEST_UNIX_TIME.to_i} 2
      format msgpack
      tag_key tagger
      time_key datetime
      run_interval 1s
  ]

  REGEXP_CONFIG = %[
      command ruby #{SCRIPT_PATH} "#{TEST_TIME}" 3
      format /(?<time>[^\\\]]*) (?<message>[^ ]*)/
      tag regex_tag
      run_interval 1s
  ]
  #      time_format %Y-%m-%d %H:%M:%S

  def test_configure
    d = create_driver
    assert{ d.instance.parser.is_a? Fluent::Plugin::TSVParser }
    assert_equal ["time","tag","k1"], d.instance.parser.keys
    assert_equal "tag", d.instance.extract_config.tag_key
    assert_equal "time", d.instance.extract_config.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.extract_config.time_format
  end

  def test_configure_with_json
    d = create_driver JSON_CONFIG
    assert{ d.instance.parser.is_a? Fluent::Plugin::JSONParser }
  end

  def test_configure_with_msgpack
    d = create_driver MSGPACK_CONFIG
    assert{ d.instance.parser.is_a? Fluent::Plugin::MessagePackParser }
  end

  def test_configure_with_regexp
    d = create_driver REGEXP_CONFIG
    assert{ d.instance.parser.is_a? Fluent::Plugin::RegexpParser }
    assert_equal '(?<time>[^\]]*) (?<message>[^ ]*)', d.instance.parser.expression
    assert_equal 'regex_tag', d.instance.tag
  end

  data(
    'default' => [TSV_CONFIG,     "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'json'    => [JSON_CONFIG,    "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'msgpack' => [MSGPACK_CONFIG, "tag1", event_time("2011-01-02 13:14:15"), {"k1"=>"ok"}],
    'regexp'  => [REGEXP_CONFIG,  "regex_tag", event_time("2011-01-02 13:14:15"), {"message"=>"hello"}],
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
