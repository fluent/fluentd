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
      assert chunk.staged?
      assert !chunk.queued?
      assert !chunk.closed?
    end

    test 'has many methods for chunks, but not implemented' do
      meta = Object.new
      chunk = Fluent::Plugin::Buffer::Chunk.new(meta)

      assert chunk.respond_to?(:append)
      assert chunk.respond_to?(:commit)
      assert chunk.respond_to?(:rollback)
      assert chunk.respond_to?(:bytesize)
      assert chunk.respond_to?(:size)
      assert chunk.respond_to?(:length)
      assert chunk.respond_to?(:empty?)
      assert chunk.respond_to?(:read)
      assert chunk.respond_to?(:open)
      assert chunk.respond_to?(:write_to)
      assert chunk.respond_to?(:msgpack_each)
      assert_raise(NotImplementedError){ chunk.append(nil) }
      assert_raise(NotImplementedError){ chunk.commit }
      assert_raise(NotImplementedError){ chunk.rollback }
      assert_raise(NotImplementedError){ chunk.bytesize }
      assert_raise(NotImplementedError){ chunk.size }
      assert_raise(NotImplementedError){ chunk.length }
      assert_raise(NotImplementedError){ chunk.empty? }
      assert_raise(NotImplementedError){ chunk.read }
      assert_raise(NotImplementedError){ chunk.open(){} }
      assert_raise(NotImplementedError){ chunk.write_to(nil) }
      assert_raise(NotImplementedError){ chunk.msgpack_each(){|v| v} }
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
    def open
      require 'stringio'
      io = StringIO.new(@data)
      yield io
    end
  end

  sub_test_case 'minimum chunk implements #size and #open' do
    test 'chunk lifecycle' do
      c = TestChunk.new(Object.new)
      assert c.staged?
      assert !c.queued?
      assert !c.closed?

      c.enqueued!

      assert !c.staged?
      assert c.queued?
      assert !c.closed?

      c.close

      assert !c.staged?
      assert !c.queued?
      assert c.closed?
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

    test 'can feed objects into blocks with unpacking msgpack' do
      require 'msgpack'
      c = TestChunk.new(Object.new)
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
end
