require 'helper'
require 'fluent/test'
require 'fluent/buffer'
require 'fluent/output'
require 'stringio'

module FluentBufferTest
  include Fluent

  class SimpleBufferedOutput < BasicBuffer
    def resume
      # queue, map
      return [], {}
    end

    def enqueue(top);end

    def new_chunk(key)
      StringIO.new
    end
  end

  class BasicBufferTest < ::Test::Unit::TestCase
    def setup
      @out = Fluent::Test::TestDriver.new(SimpleBufferedOutput)
    end

    def test_raises_exception_if_chunk_limit_exceeded
      @out.configure %[
        buffer_chunk_limit 3
        buffer_queue_limit 0
      ]

      assert_raises Fluent::BufferQueueLimitError do
        @out.run { @out.instance.emit('sample.key', "data", Fluent::NullOutputChain.instance) }
      end
    end

    def test_doesnt_raise_exception_if_chunk_limit_exceeded_and_ignored
      @out.configure %[
        buffer_chunk_limit 3
        buffer_queue_limit 0
        buffer_ignore_exceeded_chunk true
      ]

      @out.run { @out.instance.emit('sample.key', "data", Fluent::NullOutputChain.instance) }
    end
  end
end


