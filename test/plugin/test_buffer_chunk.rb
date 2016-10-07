require_relative '../helper'
require 'fluent/plugin/buffer/chunk'

class BufferChunkTest < Test::Unit::TestCase
  sub_test_case 'blank buffer chunk' do
    test 'has generated unique id, given metadata, created_at and modified_at' do
      meta = Object.new
      chunk = Fluent::Plugin::Buffer::Chunk.new(meta)
      assert{ chunk.unique_id.bytesize == 16 }
      assert{ chunk.metadata.object_id == meta.object_id }
      assert{ chunk.created_at.is_a? Time }
      assert{ chunk.modified_at.is_a? Time }
      assert chunk.unstaged?
      assert !chunk.staged?
      assert !chunk.queued?
      assert !chunk.closed?
    end

    test 'has many methods for chunks, but not implemented' do
      meta = Object.new
      chunk = Fluent::Plugin::Buffer::Chunk.new(meta)

      assert chunk.respond_to?(:append)
      assert chunk.respond_to?(:concat)
      assert chunk.respond_to?(:commit)
      assert chunk.respond_to?(:rollback)
      assert chunk.respond_to?(:bytesize)
      assert chunk.respond_to?(:size)
      assert chunk.respond_to?(:length)
      assert chunk.respond_to?(:empty?)
      assert chunk.respond_to?(:read)
      assert chunk.respond_to?(:open)
      assert chunk.respond_to?(:write_to)
      assert_raise(NotImplementedError){ chunk.append([]) }
      assert_raise(NotImplementedError){ chunk.concat(nil, 0) }
      assert_raise(NotImplementedError){ chunk.commit }
      assert_raise(NotImplementedError){ chunk.rollback }
      assert_raise(NotImplementedError){ chunk.bytesize }
      assert_raise(NotImplementedError){ chunk.size }
      assert_raise(NotImplementedError){ chunk.length }
      assert_raise(NotImplementedError){ chunk.empty? }
      assert_raise(NotImplementedError){ chunk.read }
      assert_raise(NotImplementedError){ chunk.open(){} }
      assert_raise(NotImplementedError){ chunk.write_to(nil) }
      assert !chunk.respond_to?(:msgpack_each)
    end

    test 'has method #each and #msgpack_each only when extended by ChunkMessagePackEventStreamer' do
      meta = Object.new
      chunk = Fluent::Plugin::Buffer::Chunk.new(meta)

      assert !chunk.respond_to?(:each)
      assert !chunk.respond_to?(:msgpack_each)

      chunk.extend Fluent::ChunkMessagePackEventStreamer
      assert chunk.respond_to?(:each)
      assert chunk.respond_to?(:msgpack_each)
    end

    test 'some methods raise ArgumentError with an option of `compressed: :gzip` and without extending Compressble`' do
      meta = Object.new
      chunk = Fluent::Plugin::Buffer::Chunk.new(meta)

      assert_raise(ArgumentError){ chunk.read(compressed: :gzip) }
      assert_raise(ArgumentError){ chunk.open(compressed: :gzip){} }
      assert_raise(ArgumentError){ chunk.write_to(nil, compressed: :gzip) }
      assert_raise(ArgumentError){ chunk.append(nil, compress: :gzip) }
    end
  end

  class TestChunk < Fluent::Plugin::Buffer::Chunk
    attr_accessor :data
    def initialize(meta)
      super
      @data = ''
    end
    def size
      @data.size
    end
    def open(**kwargs)
      require 'stringio'
      io = StringIO.new(@data)
      yield io
    end
  end

  sub_test_case 'minimum chunk implements #size and #open' do
    test 'chunk lifecycle' do
      c = TestChunk.new(Object.new)
      assert c.unstaged?
      assert !c.staged?
      assert !c.queued?
      assert !c.closed?
      assert c.writable?

      c.staged!

      assert !c.unstaged?
      assert c.staged?
      assert !c.queued?
      assert !c.closed?
      assert c.writable?

      c.enqueued!

      assert !c.unstaged?
      assert !c.staged?
      assert c.queued?
      assert !c.closed?
      assert !c.writable?

      c.close

      assert !c.unstaged?
      assert !c.staged?
      assert !c.queued?
      assert c.closed?
      assert !c.writable?
    end

    test 'chunk can be unstaged' do
      c = TestChunk.new(Object.new)
      assert c.unstaged?
      assert !c.staged?
      assert !c.queued?
      assert !c.closed?
      assert c.writable?

      c.staged!

      assert !c.unstaged?
      assert c.staged?
      assert !c.queued?
      assert !c.closed?
      assert c.writable?

      c.unstaged!

      assert c.unstaged?
      assert !c.staged?
      assert !c.queued?
      assert !c.closed?
      assert c.writable?

      c.enqueued!

      assert !c.unstaged?
      assert !c.staged?
      assert c.queued?
      assert !c.closed?
      assert !c.writable?

      c.close

      assert !c.unstaged?
      assert !c.staged?
      assert !c.queued?
      assert c.closed?
      assert !c.writable?
    end

    test 'can respond to #empty? correctly' do
      c = TestChunk.new(Object.new)
      assert_equal 0, c.size
      assert c.empty?
    end

    test 'can write its contents to io object' do
      c = TestChunk.new(Object.new)
      c.data << "my data\nyour data\n"
      io = StringIO.new
      c.write_to(io)
      assert "my data\nyour data\n", io.to_s
    end

    test 'can feed objects into blocks with unpacking msgpack if ChunkMessagePackEventStreamer is included' do
      require 'msgpack'
      c = TestChunk.new(Object.new)
      c.extend Fluent::ChunkMessagePackEventStreamer
      c.data << MessagePack.pack(['my data', 1])
      c.data << MessagePack.pack(['your data', 2])
      ary = []
      c.msgpack_each do |obj|
        ary << obj
      end
      assert_equal ['my data', 1], ary[0]
      assert_equal ['your data', 2], ary[1]
    end
  end

  sub_test_case 'when compress is gzip' do
    test 'create decompressable chunk' do
      meta = Object.new
      chunk = Fluent::Plugin::Buffer::Chunk.new(meta, compress: :gzip)
      assert chunk.singleton_class.ancestors.include?(Fluent::Plugin::Buffer::Chunk::Decompressable)
    end
  end
end
