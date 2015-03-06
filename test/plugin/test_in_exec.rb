require_relative '../helper'
require 'fluent/test'
require 'net/http'
require 'fluent/plugin/in_exec'

class ExecInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf=TSV_CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ExecInput).configure(conf)
  end

  TEST_TIME = Time.parse("2011-01-02 13:14:15").to_i
  SCRIPT = File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts', 'exec_script.rb'))

  TSV_CONFIG = %[
      command ruby #{SCRIPT} "2011-01-02 13:14:15" 0
      keys time,tag,k1
      time_key time
      tag_key tag
      time_format %Y-%m-%d %H:%M:%S
      run_interval 1s
  ]

  JSON_CONFIG = %[
      command ruby #{SCRIPT} #{TEST_TIME} 1
      format json
      tag_key tag
      time_key time
      run_interval 1s
  ]

  MSGPACK_CONFIG = %[
      command ruby #{SCRIPT} #{TEST_TIME} 2
      format msgpack
      tag_key tagger
      time_key datetime
      run_interval 1s
  ]

  test "configure with basic configuration" do
    d = create_driver
    assert_equal :tsv, d.instance.format
    assert_equal ["time","tag","k1"], d.instance.keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
  end

  test "configure with json configuration" do
    d = create_driver JSON_CONFIG
    assert_equal :json, d.instance.format
    assert_equal [], d.instance.keys
  end

  test "configure with msgpack configuration" do
    d = create_driver MSGPACK_CONFIG
    assert_equal :msgpack, d.instance.format
    assert_equal [], d.instance.keys
  end

  data(
    'tsv' => TSV_CONFIG,
    'json' => JSON_CONFIG,
    'msgpack' => MSGPACK_CONFIG
  )
  test "this plugin emits record from executed command output" do |data|
    d = create_driver data
    d.run_timeout = 2
    d.expected_emits_length = 1
    d.run

    emits = d.emits
    assert_equal true, emits.length > 0
    assert_equal ["tag1", TEST_TIME, {"k1"=>"ok"}], emits[0]
  end
end
