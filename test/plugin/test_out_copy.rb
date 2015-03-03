require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/out_copy'

class CopyOutputTest < Test::Unit::TestCase
  class << self
    def startup
      $LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '..', 'scripts'))
      require 'fluent/plugin/out_test'
    end

    def shutdown
      $LOAD_PATH.shift
    end
  end

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    <store>
      type test
      name c0
    </store>
    <store>
      type test
      name c1
    </store>
    <store>
      type test
      name c2
    </store>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput).configure(conf)
  end

  def test_configure
    d = create_driver

    outputs = d.instance.outputs
    assert_equal 3, outputs.size
    assert_equal Fluent::TestOutput, outputs[0].class
    assert_equal Fluent::TestOutput, outputs[1].class
    assert_equal Fluent::TestOutput, outputs[2].class
    assert_equal "c0", outputs[0].name
    assert_equal "c1", outputs[1].name
    assert_equal "c2", outputs[2].name
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)

    d.instance.outputs.each {|o|
      assert_equal [
          [time, {"a"=>1}],
          [time, {"a"=>2}],
        ], o.events
    }

    d.instance.outputs.each {|o|
      assert_not_nil o.router
    }
  end

  def test_msgpack_es_emit_bug
    d = Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput)

    outputs = %w(p1 p2).map do |pname|
      p = Fluent::Plugin.new_output('test')
      p.configure('name' => pname)
      p.define_singleton_method(:emit) do |tag, es, chain|
        es.each do |time, record|
          super(tag, [[time, record]], chain)
        end
      end
      p
    end

    d.instance.instance_eval { @outputs = outputs }

    es = if defined?(MessagePack::Packer)
           time = Time.parse("2013-05-26 06:37:22 UTC").to_i
           packer = MessagePack::Packer.new
           packer.pack([time, {"a" => 1}])
           packer.pack([time, {"a" => 2}])
           Fluent::MessagePackEventStream.new(packer.to_s)
         else
           events = "#{[time, {"a" => 1}].to_msgpack}#{[time, {"a" => 2}].to_msgpack}"
           Fluent::MessagePackEventStream.new(events)
         end

    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    d.instance.outputs.each { |o|
      assert_equal [
        [time, {"a"=>1}],
        [time, {"a"=>2}],
      ], o.events
    }
  end

  def create_event_test_driver(is_deep_copy = false)
    deep_copy_config = %[
deep_copy true
]

    output1 = Fluent::Plugin.new_output('test')
    output1.configure('name' => 'output1')
    output1.define_singleton_method(:emit) do |tag, es, chain|
      es.each do |time, record|
        record['foo'] = 'bar'
        super(tag, [[time, record]], chain)
      end
    end

    output2 = Fluent::Plugin.new_output('test')
    output2.configure('name' => 'output2')
    output2.define_singleton_method(:emit) do |tag, es, chain|
      es.each do |time, record|
        super(tag, [[time, record]], chain)
      end
    end

    outputs = [output1, output2]

    d = Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput)
    d = d.configure(deep_copy_config) if is_deep_copy
    d.instance.instance_eval { @outputs = outputs }
    d
  end

  def test_one_event
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(false)
    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}]],
      [[time, {"a"=>1, "foo"=>"bar"}]]
    ], d.instance.outputs.map{ |o| o.events }

    d = create_event_test_driver(true)
    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}]],
      [[time, {"a"=>1}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  def test_multi_event
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(false)
    es = Fluent::MultiEventStream.new
    es.add(time, {"a" => 1})
    es.add(time, {"b" => 2})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]],
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]]
    ], d.instance.outputs.map{ |o| o.events }

    d = create_event_test_driver(true)
    es = Fluent::MultiEventStream.new
    es.add(time, {"a" => 1})
    es.add(time, {"b" => 2})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]],
      [[time, {"a"=>1}], [time, {"b"=>2}]]
    ], d.instance.outputs.map{ |o| o.events }
  end
end

