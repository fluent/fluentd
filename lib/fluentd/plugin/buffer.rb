#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluentd
  module Plugin

    require 'fileutils'
    require 'fluentd/configurable'

    class BufferError < StandardError
    end

    class BufferRecordLimitError < BufferError
    end

    class BufferChunkLimitError < BufferError
    end

    class BufferQueueLimitError < BufferError
    end

    #
    # Buffer buffers binary data, queues buffered data chunks, and provides
    # interface to take the buffered data chunks.
    #
    # key => BufferChunkBuilder ---+
    #                              |    queue:
    # key => BufferChunkBuilder ---+--> [BufferChunk, BufferChunk, ...]
    #                              |
    # key => BufferChunkBuilder ---+
    #
    # Buffer has a BufferChunkBuilder for each key. Buffer#open method provides
    # a writer object to append data into the BufferChunkBuilders.
    #
    # When a BufferChunkBuilder buffers `buffer_chunk_limit` bytes of data, it
    # flushes the built BufferChunk to a queue.
    #
    # Buffer#acquire is the thread-safe interface to take the first BufferChunk
    # from the queue. If the queue is empty, #acquire method calls
    # `early_flush_strategy.call(key, status)` for each BufferChunkBuilder. If
    # the callback method returns true, it flushes the BufferChunkBuilder to the
    # queue. By default, early_flush_strategy always returns true.
    #
    class Buffer
      include Configurable

      config_param :buffer_chunk_limit, :size, :default => 8*1024*1024
      config_param :buffer_queue_limit, :integer, :default => 512

      def initialize
        super
        @early_flush_strategy = proc {|key,status| true }
      end

      attr_writer :early_flush_strategy

      def persistent?
        raise NoMethodError, "#{self.class}#persistent? is not implemented"
      end

      def open(&block)
        raise NoMethodError, "#{self.class}#open(&block) is not implemented"
      end

      def acquire(&block)
        raise NoMethodError, "#{self.class}#acquire(&block) is not implemented"
      end

      def clear
        raise NoMethodError, "#{self.class}#clear is not implemented"
      end

      def start
      end

      def shutdown
      end

      def close
      end
    end

    class BasicBuffer < Buffer
      def initialize
        super
        @mutex = Mutex.new
      end

      def open(&block)
        @mutex.synchronize do
          yield self
        end
      end

      def append(key, data)
        if data.bytesize > @buffer_chunk_limit
          raise BufferRecordLimitError, "buffer record size exceeds limit"
        end
        get_builder(key).append(data)
      end

      def get_builder(key)
        raise NoMethodError, "#{self.class}#get_builder(key) is not implemented"
      end

      def clear
        raise NoMethodError, "#{self.class}#clear is not implemented"
      end

      def acquire(&block)
        lock = @mutex.lock
        begin
          acquired_chunk = acquire_next_chunk

          unless acquire_next_chunk
            early_flushed = true
            early_flush_chunk(@early_flush_strategy)
            acquired_chunk = acquire_next_chunk
            unless acquired_chunk
              return nil
            end
          end

          lock.unlock
          lock = nil

          # blocking (long-running) operation:
          block.call(acquired_chunk)

          chunk = acquired_chunk
          acquired_chunk = nil
          remove_acquired_chunk(chunk)

          return early_flushed ? false : true

        ensure
          begin
            if acquired_chunk
              lock = @mutex.lock unless lock
              release_acquired_chunk(acquired_chunk)
            end
          ensure
            lock.unlock if lock
          end
        end
      end

      def early_flush_chunk(strategy)
        raise NoMethodError, "#{self.class}#early_flush_chunk(strategy) is not implemented"
      end

      def acquire_next_chunk
        raise NoMethodError, "#{self.class}#acquire_next_chunk is not implemented"
      end

      def remove_acquired_chunk(chunk)
        raise NoMethodError, "#{self.class}#remove_acquired_chunk(chunk) is not implemented"
      end

      def release_acquired_chunk(chunk)
        raise NoMethodError, "#{self.class}#release_acquired_chunk(chunk) is not implemented"
      end
    end

    class BufferChunkBuilder
      def initialize(key)
        @key = key
      end

      attr_reader :key
    end

    class BufferChunk
      def initialize(key)
        @key = key
      end

      attr_reader :key

      def size
        raise NoMethodError, "#{self.class}#size is not implemented"
      end

      def empty?
        size == 0
      end

      def read
        raise NoMethodError, "#{self.class}#read is not implemented"
      end

      def open(&block)
        StringIO.open(read, &block)
      end

      def write_to(io)
        open {|src_io|
          FileUtils.copy_stream(src_io, io)
        }
      end

      def msgpack_each(&block)
        open {|io|
          u = MessagePack::Unpacker.new(io)
          u.each(&block)
        }
      end
    end

  end
end
