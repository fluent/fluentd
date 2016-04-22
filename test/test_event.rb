require_relative 'helper'
require 'fluent/test'
require 'fluent/event'

module EventTest
  class OneEventStreamTest < ::Test::Unit::TestCase
    include Fluent

    def setup
      @time = event_time()
      @record = {'k' => 'v', 'n' => 1}
      @es = OneEventStream.new(@time, @record)
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'dup' do
      dupped = @es.dup
      assert_kind_of OneEventStream, dupped
      assert_not_equal @es.object_id, dupped.object_id
    end

    test 'each' do
      @es.each { |time, record|
        assert_equal @time, time
        assert_equal @record, record
      }
    end

    test 'to_msgpack_stream' do
      stream = @es.to_msgpack_stream
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @time, time
        assert_equal @record, record
      }
    end

    test 'to_msgpack_stream with time_int argument' do
      stream = @es.to_msgpack_stream(time_int: true)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @time.to_i, time
        assert_equal @record, record
      }
    end
  end

  class ArrayEventStreamTest < ::Test::Unit::TestCase
    include Fluent

    def setup
      time = Engine.now
      @times = [Fluent::EventTime.new(time.sec), Fluent::EventTime.new(time.sec + 1)]
      @records = [{'k' => 'v1', 'n' => 1}, {'k' => 'v2', 'n' => 2}]
      @es = ArrayEventStream.new(@times.zip(@records))
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'dup' do
      dupped = @es.dup
      assert_kind_of ArrayEventStream, dupped
      assert_not_equal @es.object_id, dupped.object_id
    end

    test 'empty?' do
      assert_not_empty @es
      assert_true ArrayEventStream.new([]).empty?
    end

    test 'each' do
      i = 0
      @es.each { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end

    test 'to_msgpack_stream' do
      i = 0
      stream = @es.to_msgpack_stream
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end
  end

  class MultiEventStreamTest < ::Test::Unit::TestCase
    include Fluent

    def setup
      time = Engine.now
      @times = [Fluent::EventTime.new(time.sec), Fluent::EventTime.new(time.sec + 1)]
      @records = [{'k' => 'v1', 'n' => 1}, {'k' => 'v2', 'n' => 2}]
      @es = MultiEventStream.new
      @times.zip(@records).each { |_time, record|
        @es.add(_time, record)
      }
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'dup' do
      dupped = @es.dup
      assert_kind_of MultiEventStream, dupped
      assert_not_equal @es.object_id, dupped.object_id
    end

    test 'empty?' do
      assert_not_empty @es
      assert_true MultiEventStream.new.empty?
    end

    test 'each' do
      i = 0
      @es.each { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end

    test 'to_msgpack_stream' do
      i = 0
      stream = @es.to_msgpack_stream
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end
  end

  class MessagePackEventStreamTest < ::Test::Unit::TestCase
    include Fluent

    def setup
      pk = Fluent::Engine.msgpack_factory.packer
      time = Engine.now
      @times = [Fluent::EventTime.new(time.sec), Fluent::EventTime.new(time.sec + 1)]
      @records = [{'k' => 'v1', 'n' => 1}, {'k' => 'v2', 'n' => 2}]
      @times.zip(@records).each { |_time, record|
        pk.write([_time, record])
      }
      @es = MessagePackEventStream.new(pk.to_s)
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'each' do
      i = 0
      @es.each { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end

    test 'to_msgpack_stream' do
      i = 0
      stream = @es.to_msgpack_stream
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end
  end
end
