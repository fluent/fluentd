require_relative '../helper'
require 'fluent/test'
require 'net/http'
require 'fluent/plugin/in_exec'

class ExecInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @test_time = Time.parse("2011-01-02 13:14:15").to_i
    @script = File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts', 'exec_script.rb'))
  end

  def create_driver(conf=tsv_config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ExecInput).configure(conf)
  end

  def tsv_config
    %[
      command ruby #{@script} "2011-01-02 13:14:15" 0
      keys time,tag,k1
      time_key time
      tag_key tag
      time_format %Y-%m-%d %H:%M:%S
      run_interval 1s
    ]
  end

  def json_config
    %[
      command ruby #{@script} #{@test_time} 1
      format json
      tag_key tag
      time_key time
      run_interval 1s
    ]
  end

  def msgpack_config
    %[
      command ruby #{@script} #{@test_time} 2
      format msgpack
      tag_key tagger
      time_key datetime
      run_interval 1s
    ]
  end

  def test_configure
    d = create_driver
    assert_equal :tsv, d.instance.format
    assert_equal ["time","tag","k1"], d.instance.keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
  end

  def test_configure_with_json
    d = create_driver json_config
    assert_equal :json, d.instance.format
    assert_equal [], d.instance.keys
  end

  def test_configure_with_msgpack
    d = create_driver msgpack_config
    assert_equal :msgpack, d.instance.format
    assert_equal [], d.instance.keys
  end

  # TODO: Merge following tests into one case with parameters

  def test_emit
    d = create_driver
    d.run_timeout = 2
    d.expected_emits_length = 1
    d.run

    emits = d.emits
    assert_equal true, emits.length > 0
    assert_equal ["tag1", @test_time, {"k1"=>"ok"}], emits[0]
  end

  def test_emit_json
    d = create_driver json_config
    d.run_timeout = 2
    d.expected_emits_length = 1
    d.run

    emits = d.emits
    assert_equal true, emits.length > 0
    assert_equal ["tag1", @test_time, {"k1"=>"ok"}], emits[0]
  end

  def test_emit_msgpack
    d = create_driver msgpack_config
    d.run_timeout = 2
    d.expected_emits_length = 1
    d.run

    emits = d.emits
    assert_equal true, emits.length > 0
    assert_equal ["tag1", @test_time, {"k1"=>"ok"}], emits[0]
  end
end
