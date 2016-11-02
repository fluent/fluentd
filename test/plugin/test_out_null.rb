require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_null'

class NullOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = "")
    Fluent::Test::Driver::Output.new(Fluent::Plugin::NullOutput).configure(conf)
  end

  sub_test_case 'non-buffered' do
    test 'configure' do
      assert_nothing_raised do
        create_driver
      end
    end

    test 'process' do
      d = create_driver
      assert_nothing_raised do
        d.run do
          d.feed("test", Fluent::EventTime.now, {"test" => "null"})
        end
      end
    assert_equal([], d.events(tag: "test"))
    end
  end

  sub_test_case 'buffered' do
    test 'default chunk limit size is 100' do
      d = create_driver(config_element("ROOT", "", {}, [config_element("buffer")]))
      assert_equal 10 * 1024, d.instance.buffer_config.chunk_limit_size
      assert d.instance.buffer_config.flush_at_shutdown
      assert_equal ['tag'], d.instance.buffer_config.chunk_keys
      assert d.instance.chunk_key_tag
      assert !d.instance.chunk_key_time
      assert_equal [], d.instance.chunk_keys
    end

    test 'writes standard formattted chunks' do
      d = create_driver(config_element("ROOT", "", {}, [config_element("buffer")]))
      t = event_time("2016-05-23 00:22:13 -0800")
      d.run(default_tag: 'test', flush: true) do
        d.feed(t, {"message" => "null null null"})
        d.feed(t, {"message" => "null null"})
        d.feed(t, {"message" => "null"})
      end

      assert_equal 3, d.instance.emit_count
      assert_equal 3, d.instance.emit_records
    end

    test 'check for chunk passed to #write' do
      d = create_driver(config_element("ROOT", "", {}, [config_element("buffer")]))
      data = []
      d.instance.feed_proc = ->(chunk){ data << [chunk.unique_id, chunk.metadata.tag, chunk.read] }

      t = event_time("2016-05-23 00:22:13 -0800")
      d.run(default_tag: 'test', flush: true) do
        d.feed(t, {"message" => "null null null"})
        d.feed(t, {"message" => "null null"})
        d.feed(t, {"message" => "null"})
      end

      assert_equal 1, data.size
      _, tag, binary = data.first
      events = []
      Fluent::MessagePackFactory.unpacker.feed_each(binary){|obj| events << obj }
      assert_equal 'test', tag
      assert_equal [ [t, {"message" => "null null null"}], [t, {"message" => "null null"}], [t, {"message" => "null"}] ], events
    end

    test 'check for chunk passed to #try_write' do
      d = create_driver(config_element("ROOT", "", {}, [config_element("buffer")]))
      data = []
      d.instance.feed_proc = ->(chunk){ data << [chunk.unique_id, chunk.metadata.tag, chunk.read] }
      d.instance.delayed = true

      t = event_time("2016-05-23 00:22:13 -0800")
      d.run(default_tag: 'test', flush: true, wait_flush_completion: false, shutdown: false) do
        d.feed(t, {"message" => "null null null"})
        d.feed(t, {"message" => "null null"})
        d.feed(t, {"message" => "null"})
      end

      assert_equal 1, data.size
      chunk_id, tag, binary = data.first
      events = []
      Fluent::MessagePackFactory.unpacker.feed_each(binary){|obj| events << obj }
      assert_equal 'test', tag
      assert_equal [ [t, {"message" => "null null null"}], [t, {"message" => "null null"}], [t, {"message" => "null"}] ], events

      assert_equal [chunk_id], d.instance.buffer.dequeued.keys

      d.instance.commit_write(chunk_id)

      assert_equal [], d.instance.buffer.dequeued.keys

      d.instance_shutdown
    end
  end
end
