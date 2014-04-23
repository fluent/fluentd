# encoding: utf-8
require 'fluent/test'

class CopyOutputTest < Test::Unit::TestCase
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

  def record_changing_output
    output = Fluent::Plugin.new_output('test')
    output.configure('name' => 'record_changing_output')
    output.define_singleton_method(:emit) do |tag, es, chain|
      es.each do |time, record|
        record['foo'] = 'bar'
        super(tag, [[time, record]], chain)
      end
    end
    output
  end

  def simple_output
    output = Fluent::Plugin.new_output('test')
    output.configure('name' => 'simple_output')
    output.define_singleton_method(:emit) do |tag, es, chain|
      es.each do |time, record|
        super(tag, [[time, record]], chain)
      end
    end
    output
  end

  def failing_output
    output = Fluent::Plugin.new_output('test')
    output.configure('name' => 'failing_output')
    output.define_singleton_method(:emit) do |tag, es, chain|
      raise StandardError, "ごめんなさい"
      es.each do |time, record|
        super(tag, [[time, record]], chain)
      end
    end
    output
  end

  def create_event_test_driver(options={})
    config = ""
    config << "deep_copy true\n" if options[:deep_copy]
    config << "ignore_individual_errors true\n" if options[:ignore_individual_errors]


    d = Fluent::Test::OutputTestDriver.new(Fluent::CopyOutput)
    d = d.configure(config)
    d.instance.instance_eval { @outputs = options[:outputs] }
    d
  end

  def test_one_event
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(:outputs => [record_changing_output, simple_output])
    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}]],
      [[time, {"a"=>1, "foo"=>"bar"}]]
    ], d.instance.outputs.map{ |o| o.events }

    d = create_event_test_driver(:deep_copy => true, :outputs => [record_changing_output, simple_output])
    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}]],
      [[time, {"a"=>1}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  def test_multi_event
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(:outputs => [record_changing_output, simple_output])
    es = Fluent::MultiEventStream.new
    es.add(time, {"a" => 1})
    es.add(time, {"b" => 2})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]],
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]]
    ], d.instance.outputs.map{ |o| o.events }

    d = create_event_test_driver(:deep_copy => true, :outputs => [record_changing_output, simple_output])
    es = Fluent::MultiEventStream.new
    es.add(time, {"a" => 1})
    es.add(time, {"b" => 2})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [[time, {"a"=>1, "foo"=>"bar"}], [time, {"b"=>2, "foo"=>"bar"}]],
      [[time, {"a"=>1}], [time, {"b"=>2}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  def test_single_failure_breaks_chain_unless_configured
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(:outputs => [simple_output, failing_output])

    es = Fluent::OneEventStream.new(time, {"a" => 1})

    assert_raises Fluent::OutputChainError do
      d.instance.emit('test', es, Fluent::NullOutputChain.instance)
    end
  end

  def test_single_failure_doesnt_break_chain
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(:ignore_individual_errors => true,
                                 :outputs => [failing_output,
                                              record_changing_output,
                                              simple_output])

    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [],
      [[time, {"a"=>1, "foo" => "bar"}]],
      [[time, {"a"=>1, "foo" => "bar"}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  def test_single_failure_doesnt_break_chain_with_deep_copy
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(:ignore_individual_errors => true,
                                 :deep_copy => true,
                                 :outputs => [failing_output,
                                              record_changing_output,
                                              simple_output])

    es = Fluent::OneEventStream.new(time, {"a" => 1})
    d.instance.emit('test', es, Fluent::NullOutputChain.instance)

    assert_equal [
      [],
      [[time, {"a"=>1, "foo" => "bar"}]],
      [[time, {"a"=>1}]]
    ], d.instance.outputs.map{ |o| o.events }
  end

  def test_all_failures_raises_exception
    time = Time.parse("2013-05-26 06:37:22 UTC").to_i

    d = create_event_test_driver(:ignore_individual_errors => true,
                                 :outputs => [failing_output, failing_output])

    es = Fluent::OneEventStream.new(time, {"a" => 1})

    assert_raises Fluent::OutputChainError do
      d.instance.emit('test', es, Fluent::NullOutputChain.instance)
    end
  end

end

