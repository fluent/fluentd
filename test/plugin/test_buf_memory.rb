# -*- coding: utf-8 -*-
require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/buf_memory'

require 'stringio'
require 'msgpack'

module FluentMemoryBufferTest
  class MemoryBufferChunkTest < Test::Unit::TestCase
    def test_init
      chunk = Fluent::MemoryBufferChunk.new('key')
      assert_equal 'key', chunk.key
      assert_equal '', chunk.instance_eval{ @data }
      assert_equal 'ASCII-8BIT', chunk.instance_eval{ @data }.encoding.to_s
      assert chunk.unique_id # non nil

      chunk2 = Fluent::MemoryBufferChunk.new('initdata', 'data')
      assert_equal 'initdata', chunk2.key
      assert_equal 'data', chunk2.instance_eval{ @data }
    end

    def test_buffer_chunk_interface
      chunk = Fluent::BufferChunk.new('key')

      assert chunk.respond_to?(:empty?)
      assert chunk.respond_to?(:<<)
      assert chunk.respond_to?(:size)
      assert chunk.respond_to?(:close)
      assert chunk.respond_to?(:purge)
      assert chunk.respond_to?(:read)
      assert chunk.respond_to?(:open)
      assert chunk.respond_to?(:write_to)
      assert chunk.respond_to?(:msgpack_each)
    end

    def test_empty?
      chunk = Fluent::MemoryBufferChunk.new('key')
      assert chunk.empty?

      chunk.instance_eval{ @data = "non empty" }
      assert !(chunk.empty?)
    end

    def test_append_data_and_size
      chunk = Fluent::MemoryBufferChunk.new('key')
      assert_equal '', chunk.instance_eval{ @data }

      chunk << "foo bar baz\n".force_encoding('UTF-8')
      assert_equal "foo bar baz\n", chunk.instance_eval{ @data }
      assert_equal 'ASCII-8BIT', chunk.instance_eval{ @data }.encoding.to_s

      assert_equal 12, chunk.size # bytesize

      chunk << "日本語Japanese\n".force_encoding('UTF-8')
      assert_equal "foo bar baz\n日本語Japanese\n".force_encoding('ASCII-8BIT'), chunk.instance_eval{ @data }
      assert_equal 'ASCII-8BIT', chunk.instance_eval{ @data }.encoding.to_s

      assert_equal 30, chunk.size # bytesize
    end

    def test_close_and_purge_does_nothing
      chunk = Fluent::MemoryBufferChunk.new('key', 'data')
      chunk.close
      chunk.close
      chunk.close
      chunk.close
      chunk.purge
      chunk.purge
      chunk.purge
      chunk.purge
      assert_equal 'data', chunk.instance_eval{ @data }
    end

    def test_read_just_returns_data
      data = "data1\ndata2\n"
      chunk = Fluent::MemoryBufferChunk.new('key', data)
      assert_equal data, chunk.read
      assert_equal data.object_id, chunk.read.object_id
    end

    def test_open
      # StringIO.open(@data, &block)
      chunk = Fluent::MemoryBufferChunk.new('key', 'foo bar baz')
      chunk.open do |io|
        assert 'foo bar baz', io.read
      end
    end

    def test_write_to
      chunk = Fluent::MemoryBufferChunk.new('key', 'foo bar baz')
      dummy_dst = StringIO.new
      chunk.write_to(dummy_dst)
      assert_equal 'foo bar baz', dummy_dst.string
    end

    def test_msgpack_each
      d0 = MessagePack.pack([[1, "foo"], [2, "bar"], [3, "baz"]])
      d1 = MessagePack.pack({"key1" => "value1", "key2" => "value2"})
      d2 = MessagePack.pack("string1")
      d3 = MessagePack.pack(1)
      d4 = MessagePack.pack(nil)
      chunk = Fluent::MemoryBufferChunk.new('key', d0 + d1 + d2 + d3 + d4)

      store = []
      chunk.msgpack_each do |data|
        store << data
      end

      assert_equal 5, store.size
      assert_equal [[1, "foo"], [2, "bar"], [3, "baz"]], store[0]
      assert_equal({"key1" => "value1", "key2" => "value2"}, store[1])
      assert_equal "string1", store[2]
      assert_equal 1, store[3]
      assert_equal nil, store[4]
    end
  end

  class MemoryBufferTest < Test::Unit::TestCase
    def test_init_configure
      buf = Fluent::MemoryBuffer.new

      buf.configure({})
      assert buf.flush_at_shutdown
      assert_equal 64, buf.buffer_queue_limit
    end

    class DummyOutput
      attr_accessor :written

      def write(chunk)
        @written ||= []
        @written.push chunk
        "return value"
      end
    end

    def test_before_shutdown
      buf = Fluent::MemoryBuffer.new
      buf.start

      # before_shutdown flushes all chunks in @map and @queue

      c1 = [ buf.new_chunk('k0'), buf.new_chunk('k1'), buf.new_chunk('k2'), buf.new_chunk('k3') ]
      c2 = [ buf.new_chunk('q0'), buf.new_chunk('q1') ]

      buf.instance_eval do
        @map = {
          'k0' => c1[0], 'k1' => c1[1], 'k2' => c1[2], 'k3' => c1[3],
          'q0' => c2[0], 'q1' => c2[1]
        }
      end
      c1[0] << "data1\ndata2\n"
      c1[1] << "data1\ndata2\n"
      c1[2] << "data1\ndata2\n"
      # k3 chunk is empty!

      c2[0] << "data1\ndata2\n"
      c2[1] << "data1\ndata2\n"
      buf.push('q0')
      buf.push('q1')

      buf.instance_eval do
        @enqueue_hook_times = 0
        def enqueue(chunk)
          @enqueue_hook_times += 1
        end
      end
      assert_equal 0, buf.instance_eval{ @enqueue_hook_times }

      out = DummyOutput.new
      assert_equal nil, out.written

      buf.before_shutdown(out)

      assert_equal 3, buf.instance_eval{ @enqueue_hook_times } # k0, k1, k2
      assert_equal 5, out.written.size
      assert_equal [c2[0], c2[1], c1[0], c1[1], c1[2]], out.written
    end

    def test_new_chunk
      buf = Fluent::MemoryBuffer.new
      chunk = buf.new_chunk('key')
      assert_equal Fluent::MemoryBufferChunk, chunk.class
      assert_equal 'key', chunk.key
      assert chunk.empty?
    end

    def test_resume
      buf = Fluent::MemoryBuffer.new
      resumed_queue, resumed_store = buf.resume
      assert resumed_queue.empty?
      assert resumed_store.empty?
    end

    def test_enqueue_does_nothing # enqueue hook
      buf = Fluent::MemoryBuffer.new
      chunk = Fluent::MemoryBufferChunk.new('k', "data1\ndata2\n")
      assert_equal "data1\ndata2\n", chunk.read
      buf.enqueue(chunk)
      assert_equal "data1\ndata2\n", chunk.read
    end
  end
end
