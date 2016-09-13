require_relative 'helper'
require 'fluent/test'
require 'fluent/event'
require 'fluent/plugin/compressable'

module EventTest
  module DeepCopyAssertion
    def assert_duplicated_records(es1, es2)
      ary1 = []
      es1.each do |_, record|
        ary1 << record
      end
      ary2 = []
      es2.each do |_, record|
        ary2 << record
      end
      assert_equal ary1.size, ary2.size
      ary1.each_with_index do |r, i|
        assert_not_equal r.object_id, ary2[i].object_id
      end
    end
  end

  class OneEventStreamTest < ::Test::Unit::TestCase
    include Fluent
    include DeepCopyAssertion
    include Fluent::Plugin::Compressable

    def setup
      @time = event_time()
      @record = {'k' => 'v', 'n' => 1}
      @es = OneEventStream.new(@time, @record)
    end

    test 'empty?' do
      assert_false @es.empty?
    end

    test 'size' do
      assert_equal 1, @es.size
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'dup' do
      dupped = @es.dup
      assert_kind_of OneEventStream, dupped
      assert_not_equal @es.object_id, dupped.object_id
      assert_duplicated_records @es, dupped
    end

    test 'slice' do
      assert_equal 0, @es.slice(1, 1).size
      assert_equal 0, @es.slice(0, 0).size

      sliced = @es.slice(0, 1)
      assert_kind_of EventStream, sliced
      assert_equal 1, sliced.size

      sliced.each do |time, record|
        assert_equal @time, time
        assert_equal @record, record
      end
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

    test 'to_compressed_msgpack_stream' do
      stream = @es.to_compressed_msgpack_stream
      Fluent::Engine.msgpack_factory.unpacker.feed_each(decompress(stream)) { |time, record|
        assert_equal @time, time
        assert_equal @record, record
      }
    end

    test 'to_compressed_msgpack_stream with time_int argument' do
      stream = @es.to_compressed_msgpack_stream(time_int: true)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(decompress(stream)) { |time, record|
        assert_equal @time.to_i, time
        assert_equal @record, record
      }
    end
  end

  class ArrayEventStreamTest < ::Test::Unit::TestCase
    include Fluent
    include DeepCopyAssertion
    include Fluent::Plugin::Compressable

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
      assert_duplicated_records @es, dupped
    end

    test 'empty?' do
      assert_not_empty @es
      assert_true ArrayEventStream.new([]).empty?
    end

    test 'size' do
      assert_equal 2, @es.size
      assert_equal 0, ArrayEventStream.new([]).size
    end

    test 'slice' do
      sliced = @es.slice(1,1)
      assert_kind_of EventStream, sliced
      assert_equal 1, sliced.size

      sliced.each do |time, record|
        assert_equal @times[1], time
        assert_equal 'v2', record['k']
        assert_equal 2, record['n']
      end

      sliced = @es.slice(0,2)
      assert_kind_of EventStream, sliced
      assert_equal 2, sliced.size

      counter = 0
      sliced.each do |time, record|
        assert_equal @times[counter], time
        assert_equal @records[counter]['k'], record['k']
        assert_equal @records[counter]['n'], record['n']
        counter += 1
      end
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

    test 'to_compressed_msgpack_stream' do
      i = 0
      compressed_stream = @es.to_compressed_msgpack_stream
      stream = decompress(compressed_stream)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end

    test 'to_compressed_msgpack_stream with time_int argument' do
      i = 0
      compressed_stream = @es.to_compressed_msgpack_stream(time_int: true)
      stream = decompress(compressed_stream)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i].to_i, time
        assert_equal @records[i], record
        i += 1
      }
    end
  end

  class MultiEventStreamTest < ::Test::Unit::TestCase
    include Fluent
    include DeepCopyAssertion
    include Fluent::Plugin::Compressable

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
      assert_duplicated_records @es, dupped
    end

    test 'empty?' do
      assert_not_empty @es
      assert_true MultiEventStream.new.empty?
    end

    test 'size' do
      assert_equal 2, @es.size
      assert_equal 0, MultiEventStream.new.size
    end

    test 'slice' do
      sliced = @es.slice(1,1)
      assert_kind_of EventStream, sliced
      assert_equal 1, sliced.size

      sliced.each do |time, record|
        assert_equal @times[1], time
        assert_equal 'v2', record['k']
        assert_equal 2, record['n']
      end

      sliced = @es.slice(0,2)
      assert_kind_of EventStream, sliced
      assert_equal 2, sliced.size

      counter = 0
      sliced.each do |time, record|
        assert_equal @times[counter], time
        assert_equal @records[counter]['k'], record['k']
        assert_equal @records[counter]['n'], record['n']
        counter += 1
      end
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

    test 'to_compressed_msgpack_stream' do
      i = 0
      compressed_stream = @es.to_compressed_msgpack_stream
      stream = decompress(compressed_stream)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end

    test 'to_compressed_msgpack_stream with time_int argument' do
      i = 0
      compressed_stream = @es.to_compressed_msgpack_stream(time_int: true)
      stream = decompress(compressed_stream)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i].to_i, time
        assert_equal @records[i], record
        i += 1
      }
    end
  end

  class MessagePackEventStreamTest < ::Test::Unit::TestCase
    include Fluent
    include DeepCopyAssertion
    include Fluent::Plugin::Compressable

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

    test 'dup' do
      dupped = @es.dup
      assert_kind_of MessagePackEventStream, dupped
      assert_not_equal @es.object_id, dupped.object_id
      assert_duplicated_records @es, dupped

      # After iteration of events (done in assert_duplicated_records),
      # duplicated event stream still has unpacked objects and correct size
      dupped = @es.dup
      assert_equal 2, dupped.instance_eval{ @size }
    end

    test 'empty?' do
      assert_false @es.empty?
      assert_true MessagePackEventStream.new('', 0).empty?
    end

    test 'size' do
      assert_equal 2, @es.size
      assert_equal 0, MessagePackEventStream.new('').size
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'slice' do
      sliced = @es.slice(1,1)
      assert_kind_of EventStream, sliced
      assert_equal 1, sliced.size

      sliced.each do |time, record|
        assert_equal @times[1], time
        assert_equal 'v2', record['k']
        assert_equal 2, record['n']
      end

      sliced = @es.slice(0,2)
      assert_kind_of EventStream, sliced
      assert_equal 2, sliced.size

      counter = 0
      sliced.each do |time, record|
        assert_equal @times[counter], time
        assert_equal @records[counter]['k'], record['k']
        assert_equal @records[counter]['n'], record['n']
        counter += 1
      end
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

    test 'to_compressed_msgpack_stream' do
      i = 0
      compressed_stream = @es.to_compressed_msgpack_stream
      stream = decompress(compressed_stream)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end
  end

  class CompressedMessagePackEventStreamTest < ::Test::Unit::TestCase
    include Fluent
    include DeepCopyAssertion
    include Fluent::Plugin::Compressable

    def setup
      time = Engine.now
      @times = [Fluent::EventTime.new(time.sec), Fluent::EventTime.new(time.sec + 1)]
      @records = [{ 'k' => 'v1', 'n' => 1 }, { 'k' => 'v2', 'n' => 2 }]
      @packed_record = ''
      @entries = ''
      @times.zip(@records).each do |_time, record|
        v = [_time, record].to_msgpack
        @packed_record += v
        @entries += compress(v)
      end
      @es = CompressedMessagePackEventStream.new(@entries)
    end

    def ensure_data_is_decompressed
      assert_equal @entries, @es.instance_variable_get(:@data)
      yield
      assert_equal @packed_record, @es.instance_variable_get(:@data)
    end

    test 'dup' do
      dupped = @es.dup
      assert_kind_of CompressedMessagePackEventStream, dupped
      assert_not_equal @es.object_id, dupped.object_id
      assert_duplicated_records @es, dupped

      # After iteration of events (done in assert_duplicated_records),
      # duplicated event stream still has unpacked objects and correct size
      dupped = @es.dup
      assert_equal 2, dupped.instance_eval{ @size }
    end

    test 'repeatable?' do
      assert_true @es.repeatable?
    end

    test 'size' do
      assert_equal 0, CompressedMessagePackEventStream.new('').size
      ensure_data_is_decompressed { assert_equal 2, @es.size  }
    end

    test 'each' do
      i = 0
      ensure_data_is_decompressed do
        @es.each do |time, record|
          assert_equal @times[i], time
          assert_equal @records[i], record
          i += 1
        end
      end
    end

    test 'slice' do
      sliced = nil
      ensure_data_is_decompressed { sliced = @es.slice(1,1) }
      assert_kind_of EventStream, sliced
      assert_equal 1, sliced.size

      sliced.each do |time, record|
        assert_equal @times[1], time
        assert_equal 'v2', record['k']
        assert_equal 2, record['n']
      end

      sliced = @es.slice(0,2)
      assert_kind_of EventStream, sliced
      assert_equal 2, sliced.size

      counter = 0
      sliced.each do |time, record|
        assert_equal @times[counter], time
        assert_equal @records[counter]['k'], record['k']
        assert_equal @records[counter]['n'], record['n']
        counter += 1
      end
    end

    test 'to_msgpack_stream' do
      i = 0
      stream = nil
      ensure_data_is_decompressed { stream = @es.to_msgpack_stream }

      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end

    test 'to_compressed_msgpack_stream' do
      i = 0
      # Do not call ensure_decompressed!
      assert_equal @entries, @es.instance_variable_get(:@data)
      compressed_stream = @es.to_compressed_msgpack_stream
      assert_equal @entries, @es.instance_variable_get(:@data)

      stream = decompress(compressed_stream)
      Fluent::Engine.msgpack_factory.unpacker.feed_each(stream) { |time, record|
        assert_equal @times[i], time
        assert_equal @records[i], record
        i += 1
      }
    end
  end
end
