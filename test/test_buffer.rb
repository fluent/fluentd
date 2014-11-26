require_relative 'helper'
require 'fluent/test'
require 'fluent/buffer'

require 'stringio'
require 'msgpack'

module FluentBufferTest
  class BufferTest < Test::Unit::TestCase
    def test_buffer_interface
      buf = Fluent::Buffer.new

      assert buf.respond_to?(:configure)
      assert buf.respond_to?(:start)
      assert buf.respond_to?(:shutdown)
      assert buf.respond_to?(:before_shutdown)

      # virtual methods
      assert buf.respond_to?(:emit)
      assert_raise(NotImplementedError){ buf.emit('key', 'data', 'chain') }
      assert buf.respond_to?(:keys)
      assert_raise(NotImplementedError){ buf.keys }
      assert buf.respond_to?(:push)
      assert_raise(NotImplementedError){ buf.push('key') }
      assert buf.respond_to?(:pop)
      assert_raise(NotImplementedError){ buf.pop('out') }
      assert buf.respond_to?(:clear!)
      assert_raise(NotImplementedError){ buf.clear! }
    end

    def test_buffer_does_nothing
      buf = Fluent::Buffer.new

      buf.start
      buf.before_shutdown(nil) # out == nil
      buf.shutdown
    end
  end

  class DummyChunk < Fluent::BufferChunk
    attr_accessor :size, :data, :purged, :closed
    def initialize(key, size=0)
      super(key)
      @size = size
    end

    def <<(data)
      @size += data.bytesize
    end

    def open(&block)
      StringIO.open(@data, &block)
    end

    def purge
      @purged = true
    end

    def close
      @closed = true
    end
  end

  class BufferChunkTest < Test::Unit::TestCase
    def test_has_key
      chunk = Fluent::BufferChunk.new('key')
      assert_equal 'key', chunk.key
    end

    def test_buffer_chunk_interface
      chunk = Fluent::BufferChunk.new('key')

      assert chunk.respond_to?(:empty?)
      assert chunk.respond_to?(:write_to)
      assert chunk.respond_to?(:msgpack_each)

      # virtual methods
      assert chunk.respond_to?(:<<)
      assert_raise(NotImplementedError){ chunk << 'data' }
      assert chunk.respond_to?(:size)
      assert_raise(NotImplementedError){ chunk.size }
      assert chunk.respond_to?(:close)
      assert_raise(NotImplementedError){ chunk.close }
      assert chunk.respond_to?(:purge)
      assert_raise(NotImplementedError){ chunk.purge }
      assert chunk.respond_to?(:read)
      assert_raise(NotImplementedError){ chunk.read }
      assert chunk.respond_to?(:open)
      assert_raise(NotImplementedError){ chunk.open }
    end

    def test_empty?
      dchunk = DummyChunk.new('key', 1)

      assert !(dchunk.empty?)

      dchunk.size = 0
      assert dchunk.empty?
    end

    def test_write_to
      dummy_chunk = DummyChunk.new('key')
      dummy_chunk.data = 'foo bar baz'

      dummy_dst = StringIO.new
      dummy_chunk.write_to(dummy_dst)
      assert_equal 'foo bar baz', dummy_dst.string
    end

    def test_msgpack_each
      dummy_chunk = DummyChunk.new('key')
      d0 = MessagePack.pack([[1, "foo"], [2, "bar"], [3, "baz"]])
      d1 = MessagePack.pack({"key1" => "value1", "key2" => "value2"})
      d2 = MessagePack.pack("string1")
      d3 = MessagePack.pack(1)
      d4 = MessagePack.pack(nil)

      dummy_chunk.data = d0 + d1 + d2 + d3 + d4

      store = []
      dummy_chunk.msgpack_each do |data|
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

  class DummyBuffer < Fluent::BasicBuffer
    attr_accessor :queue, :map, :enqueue_hook_times

    def initialize
      super
      @queue = nil
      @map = nil
      @enqueue_hook_times = 0
    end

    def resume
      return [], {}
    end

    def new_chunk(key)
      DummyChunk.new(key)
    end

    def enqueue(chunk)
      @enqueue_hook_times += 1
    end
  end

  class DummyChain
    def next
      true
    end
  end

  class BasicBufferTest < Test::Unit::TestCase
    def test_parallel_pop_default
      bb = Fluent::BasicBuffer.new

      assert bb.instance_eval{ @parallel_pop }
      bb.enable_parallel(false)
      assert !(bb.instance_eval{ @parallel_pop })
      bb.enable_parallel()
      assert bb.instance_eval{ @parallel_pop }
    end

    def test_configure
      bb1 = Fluent::BasicBuffer.new
      bb1.configure({})
      assert_equal 8 * 1024 * 1024, bb1.buffer_chunk_limit
      assert_equal 256, bb1.buffer_queue_limit

      bb2 = Fluent::BasicBuffer.new
      bb2.configure({
          "buffer_chunk_limit" => 256 * 1024 * 1024,
          "buffer_queue_limit" => 16
        })
      assert_equal 256 * 1024 * 1024, bb2.buffer_chunk_limit
      assert_equal 16, bb2.buffer_queue_limit
    end

    def test_virtual_methods
      bb = Fluent::BasicBuffer.new

      assert_raise(NotImplementedError){ bb.new_chunk('key') }
      assert_raise(NotImplementedError){ bb.resume }
      assert_raise(NotImplementedError){ bb.enqueue('chunk') }
    end

    def test_start
      db = DummyBuffer.new
      db.start
      assert_equal([], db.queue)
      assert_equal({}, db.map)
    end

    def test_shutdown
      db1 = DummyBuffer.new
      db1.start
      db1.shutdown

      db2 = DummyBuffer.new
      db2.start

      chunks = [ DummyChunk.new('k1'), DummyChunk.new('k2'), DummyChunk.new('k3'), DummyChunk.new('k4') ]

      db2.queue << chunks[0]
      db2.queue << chunks[1]
      db2.map = { 'k3' => chunks[2], 'k4' => chunks[3] }

      db2.shutdown

      assert chunks[0].closed
      assert chunks[1].closed
      assert chunks[2].closed
      assert chunks[3].closed

      assert_equal 0, db2.queue.size
    end

    def test_storable?
      db = DummyBuffer.new
      db.configure({})
      assert_equal 8 * 1024 * 1024, db.buffer_chunk_limit
      assert_equal 256, db.buffer_queue_limit

      # db.storable?(chunk, data)
      chunk0 = DummyChunk.new('k', 0)
      chunk1 = DummyChunk.new('k', 7 * 1024 * 1024)

      assert db.storable?(chunk0, 'b' * 1024 * 1024)
      assert db.storable?(chunk0, 'b' * 8 * 1024 * 1024)
      assert !(db.storable?(chunk0, 'b' * 9 * 1024 * 1024))

      assert db.storable?(chunk1, 'b' * 1024 * 1024)
      assert !(db.storable?(chunk1, 'b' * ( 1024 * 1024 + 1 ) ))
    end

    def test_emit
      db = DummyBuffer.new
      db.configure({})
      db.start

      chain = DummyChain.new

      assert_equal 8 * 1024 * 1024, db.buffer_chunk_limit
      assert_equal 256, db.buffer_queue_limit

      assert_equal 0, db.enqueue_hook_times

      s1m = "a" * 1024 * 1024

      d1 = s1m * 4
      d2 = s1m * 4 #=> 8
      d3 = s1m * 1 #=> 9, 1
      d4 = s1m * 6 #=> 7
      d5 = s1m * 2 #=> 9, 2
      d6 = s1m * 9 #=> 11, 9
      d7 = s1m * 9 #=> 18, 9
      d8 = s1m * 1 #=> 10, 1
      d9 = s1m * 2 #=> 3

      assert !(db.emit('key', d1, chain)) # stored in new chunk, and queue is empty
      assert !(db.map['key'].empty?)
      assert_equal 0, db.queue.size
      assert_equal 0, db.enqueue_hook_times

      assert !(db.emit('key', d2, chain)) # just storable, not queued yet.
      assert_equal 0, db.queue.size
      assert_equal 0, db.enqueue_hook_times

      assert db.emit('key', d3, chain) # not storable, so old chunk is enqueued & new chunk size is 1m and to be flushed
      assert_equal 1, db.queue.size
      assert_equal 1, db.enqueue_hook_times

      assert !(db.emit('key', d4, chain)) # stored in chunk
      assert_equal 1, db.queue.size
      assert_equal 1, db.enqueue_hook_times

      assert !(db.emit('key', d5, chain)) # not storable, old chunk is enqueued & new chunk size is 2m
                                          # not to be flushed (queue is not empty)
      assert_equal 2, db.queue.size
      assert_equal 2, db.enqueue_hook_times

      db.queue.reject!{|v| true } # flush

      assert db.emit('key', d6, chain) # not storable, old chunk is enqueued
                                       # new chunk is larger than buffer_chunk_limit
                                       # to be flushed
      assert_equal 1, db.queue.size
      assert_equal 3, db.enqueue_hook_times

      assert !(db.emit('key', d7, chain)) # chunk before emit is already larger than buffer_chunk_limit, so enqueued
                                          # not to be flushed
      assert_equal 2, db.queue.size
      assert_equal 4, db.enqueue_hook_times

      db.queue.reject!{|v| true } # flush

      assert db.emit('key', d8, chain) # chunk before emit is already larger than buffer_chunk_limit, so enqueued
                                       # to be flushed because just after flushing
      assert_equal 1, db.queue_size
      assert_equal 5, db.enqueue_hook_times

      db.queue.reject!{|v| true } # flush

      assert !(db.emit('key', d9, chain)) # stored in chunk
      assert_equal 0, db.queue_size
      assert_equal 5, db.enqueue_hook_times
    end

    def test_keys
      db = DummyBuffer.new
      db.start

      chunks = [ DummyChunk.new('k1'), DummyChunk.new('k2'), DummyChunk.new('k3'), DummyChunk.new('k4') ]

      db.queue << chunks[0]
      db.queue << chunks[1]
      db.map = { 'k3' => chunks[2], 'k4' => chunks[3] }

      assert_equal ['k3', 'k4'], db.keys
    end

    def test_queue_size
      db = DummyBuffer.new
      db.start

      chunks = [ DummyChunk.new('k1'), DummyChunk.new('k2'), DummyChunk.new('k3'), DummyChunk.new('k4') ]

      db.queue << chunks[0]
      db.queue << chunks[1]
      db.map = { 'k3' => chunks[2], 'k4' => chunks[3] }

      assert_equal 2, db.queue_size
    end

    def test_total_queued_chunk_size
      db = DummyBuffer.new
      db.start

      chunks = [ DummyChunk.new('k1', 1000), DummyChunk.new('k2', 2000), DummyChunk.new('k3', 3000), DummyChunk.new('k4', 4000) ]

      db.queue << chunks[0]
      db.queue << chunks[1]
      db.map = { 'k3' => chunks[2], 'k4' => chunks[3] }

      assert_equal (1000 + 2000 + 3000 + 4000), db.total_queued_chunk_size
    end

    def test_push
      db = DummyBuffer.new
      db.start

      chunks = [ DummyChunk.new('k1', 1000), DummyChunk.new('k2', 2000), DummyChunk.new('k3', 3000), DummyChunk.new('k4', 4000) ]

      db.map = { 'k1' => chunks[0], 'k2' => chunks[1], 'k3' => chunks[2], 'k4' => chunks[3] }

      assert_equal 0, db.queue.size
      assert_equal 4, db.map.size

      # if key does not exits, this method doesn't anything, and returns false
      assert_nil db.map['k5']
      assert !(db.push('k5'))
      assert_equal 0, db.queue.size

      # if empty chunk exists for specified key, this method doesn't anything and returns false
      empty_chunk = DummyChunk.new('key')
      db.map['k5'] = empty_chunk
      assert !(db.push('k5'))
      assert_equal empty_chunk, db.map['k5']

      # if non-empty chunk exists for specified key, that chunk is enqueued, and true returned
      assert db.push('k3')
      assert_equal 1, db.queue.size
      assert_equal 3000, db.queue.first.size
      assert_nil db.map['k3']
      assert_equal 1, db.instance_eval{ @enqueue_hook_times }
    end

    class DummyOutput
      attr_accessor :written

      def write(chunk)
        @written = chunk
        "return value"
      end
    end

    def test_pop
      ### pop(out)
      # 1. find a chunk that not owned (by checking monitor)
      # 2. return false if @queue is empty or all chunks are already owned
      # 3. call `write_chunk(chunk, out)` if it isn't empty
      # 4. remove that chunk from @queue
      # 5. call `chunk.purge`
      # 6. return @queue is not empty, or not

      db = DummyBuffer.new
      db.start
      out = DummyOutput.new

      assert !(db.pop(out)) # queue is empty
      assert_nil out.written

      c1 = DummyChunk.new('k1', 1)
      db.map = { 'k1' => c1 }
      db.push('k1')
      assert_equal 1, db.queue.size

      pop_return_value = nil
      c1.synchronize do
        pop_return_value = Thread.new {
          db.pop(out)
        }.value
      end
      assert !(pop_return_value) # a chunk is in queue, and it's owned by another thread
      assert_equal 1, db.queue.size
      assert_nil out.written
      assert_nil c1.purged

      c2 = DummyChunk.new('k2', 1)
      db.map['k2'] = c2
      db.push('k2')
      assert_equal 2, db.queue.size

      pop_return_value = nil
      c1.synchronize do
        pop_return_value = Thread.new {
          c2.synchronize do
            Thread.new {
              db.pop(out)
            }.value
          end
        }.value
      end
      assert !(pop_return_value) # two chunks are in queue, and these are owned by another thread
      assert_equal 2, db.queue.size
      assert_nil out.written
      assert_nil c1.purged
      assert_nil c2.purged

      c3 = DummyChunk.new('k3', 1)
      db.map['k3'] = c3
      db.push('k3')
      c4 = DummyChunk.new('k4', 1)
      db.map['k4'] = c4
      db.push('k4')
      assert_equal 4, db.queue.size

      # all of c[1234] are not empty
      queue_to_be_flushed_more = db.pop(out)
      assert queue_to_be_flushed_more # queue has more chunks
      assert c1.purged       # the first chunk is shifted, and purged
      assert_equal c1, out.written # empty chunk is not passed to output plugin
      assert_equal 3, db.queue.size

      c3.synchronize do
        queue_to_be_flushed_more = Thread.new {
          db.pop(out)
        }.value
      end
      assert queue_to_be_flushed_more # c3, c4 exists in queue
      assert c2.purged
      assert_equal c2, out.written
      assert_equal 2, db.queue.size

      c3.synchronize do
        queue_to_be_flushed_more = Thread.new {
          db.pop(out)
        }.value
      end
      assert queue_to_be_flushed_more # c3 exists in queue
      assert c4.purged
      assert_equal c4, out.written
      assert_equal 1, db.queue.size

      queue_to_be_flushed_more = db.pop(out)
      assert c3.purged
      assert_equal c3, out.written
      assert_equal 0, db.queue.size
    end

    def test_write_chunk
      db = DummyBuffer.new
      db.start

      chunk = DummyChunk.new('k1', 1)
      out = DummyOutput.new

      assert_equal "return value", db.write_chunk(chunk, out)
      assert_equal chunk, out.written
    end

    def test_clear!
      db = DummyBuffer.new
      db.start

      keys = (1..5).map{ |i| "c_#{i}" }
      chunks = keys.map{ |k| DummyChunk.new(k, 1) }
      db.map = Hash[ [keys,chunks].transpose ]

      assert_equal 5, db.map.size
      assert_equal 0, db.queue.size

      db.clear!
      assert_equal 5, db.map.size
      assert_equal 0, db.queue.size

      keys.each do |k|
        db.push(k)
      end
      assert_equal 0, db.map.size
      assert_equal 5, db.queue.size

      db.clear!
      assert_equal 0, db.map.size
      assert_equal 0, db.queue.size

      assert chunks.reduce(true){|a,b| a && b.purged }
    end
  end
end
