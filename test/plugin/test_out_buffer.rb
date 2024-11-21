require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_buffer'

class BufferOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BufferOutput).configure(conf)
  end

  test "default setting" do
    d = create_driver(
      config_element(
        "ROOT", "", {},
        [config_element("buffer", "", {"path" => "test"})]
      )
    )

    assert_equal(
      [
        "file",
        ["tag"],
        :interval,
        10,
      ],
      [
        d.instance.buffer_config["@type"],
        d.instance.buffer_config.chunk_keys,
        d.instance.buffer_config.flush_mode,
        d.instance.buffer_config.flush_interval,
      ]
    )
  end

  test "#write" do
    d = create_driver(
      config_element(
        "ROOT", "", {},
        [config_element("buffer", "", {"@type" => "memory", "flush_mode" => "immediate"})]
      )
    )

    time = event_time
    record = {"message" => "test"}
    d.run(default_tag: 'test') do
      d.feed(time, record)
    end

    assert_equal [["test", time, record]], d.events
  end
end
