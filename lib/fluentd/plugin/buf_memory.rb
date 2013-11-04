#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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

    require 'stringio'
    require 'fluentd/plugin/buffer'

    class MemoryBuffer < BasicBuffer
      Plugin.register_buffer('memory', self)

      def initialize
        super
        @map = {}
        @queue = []
      end

      def persistent?
        false
      end

      def get_builder(key)
        @map[key] ||= MemoryBufferChunkBuilder.new(key, @queue, @buffer_chunk_limit)
      end

      def early_flush_chunk(strategy)
        key, _ = @map.find {|key,builder| strategy.call(key, builder) }

        if key && builder = @map.delete(key)
          builder.flush
          return true
        else
          return false
        end
      end

      def acquire_next_chunk
        # Hash is ordered
        @queue.shift
      end

      def remove_acquired_chunk(chunk)
        # do nothing
      end

      def release_acquired_chunk(chunk)
        @queue.unshift << chunk
      end

      class MemoryBufferChunkBuilder < BufferChunkBuilder
        def initialize(key, queue, buffer_chunk_limit)
          super(key)
          @queue = queue
          @buffer_chunk_limit = buffer_chunk_limit
          @data = ''.force_encoding('ASCII-8BIT')
        end

        def append(data)
          if @data.bytesize + data.bytesize + @buffer_chunk_limit
            flush
          end
          @data << data
        end

        def flush
          @queue << MemoryBufferChunk.new(@key, @data.freeze)
          @data = ''.force_encoding('ASCII-8BIT')
        end
      end

      class MemoryBufferChunk < BufferChunk
        def initialize(key, data)
          @key = key
          @data = data
        end

        # override
        def size
          @data.bytesize
        end

        # override
        def read
          @data
        end

        # override
        def write_to(io)
          io.write(@data)
        end

        # override
        def msgpack_each(&block)
          u = MessagePack::Unpacker.new
          u.feed_each(@data, &block)
        end
      end
    end

  end
end
