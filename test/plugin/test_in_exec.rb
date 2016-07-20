require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_exec'
require 'net/http'

class ExecInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @test_time = event_time("2011-01-02 13:14:15")
    @script = File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts', 'exec_script.rb'))
  end

  def create_driver(conf = tsv_config)
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

  def regexp_config
    %[
      command ruby #{@script} "2011-01-02 13:14:15" 3
      format /(?<time>[^\\\]]*) (?<message>[^ ]*)/
      tag regex_tag
      run_interval 1s
    ]
  end

  def test_configure
    d = create_driver
    assert_equal 'tsv', d.instance.format
    assert_equal ["time","tag","k1"], d.instance.keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
  end

  def test_configure_with_json
    d = create_driver json_config
    assert_equal 'json', d.instance.format
    assert_equal [], d.instance.keys
  end

  def test_configure_with_msgpack
    d = create_driver msgpack_config
    assert_equal 'msgpack', d.instance.format
    assert_equal [], d.instance.keys
  end

  def test_configure_with_regexp
    d = create_driver regexp_config
    assert_equal '/(?<time>[^\]]*) (?<message>[^ ]*)/', d.instance.format
    assert_equal 'regex_tag', d.instance.tag
  end

  # TODO: Merge following tests into one case with parameters

  def test_emit
    d = create_driver

    d.run(expect_emits: 2)

    assert_equal true, d.events.length > 0
    d.events.each_with_index {|event, i|
      assert_equal ["tag1", @test_time, {"k1"=>"ok"}], event
      assert_equal_event_time(@test_time, event[1])
    }
  end

  def test_emit_json
    d = create_driver json_config

    d.run(expect_emits: 2)

    assert_equal true, d.events.length > 0
    d.events.each_with_index {|event, i|
      assert_equal ["tag1", @test_time, {"k1"=>"ok"}], event
      assert_equal_event_time(@test_time, event[1])
    }
  end

  def test_emit_msgpack
    d = create_driver msgpack_config

    d.run(expect_emits: 2)

    assert_equal true, d.events.length > 0
    d.events.each_with_index {|event, i|
      assert_equal ["tag1", @test_time, {"k1"=>"ok"}], event
      assert_equal_event_time(@test_time, event[1])
    }
  end

  def test_emit_regexp
    d = create_driver regexp_config

    d.run(expect_emits: 2)

    assert_equal true, d.events.length > 0
    d.events.each_with_index {|event, i|
      assert_equal ["regex_tag", @test_time, {"message"=>"hello"}], event
      assert_equal_event_time(@test_time, event[1])
    }
  end
end
